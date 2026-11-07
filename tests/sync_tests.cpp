#include "sync_tests.h"
#include "test_support.h"

#include <suitepp/suite.hpp>
#include <threadpp/condition_variable.hpp>
#include <threadpp/future.hpp>
#include <threadpp/mutex.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <system_error>
#include <thread>
#include <vector>

//-----------------------------------------------------------------------------
/// tpp::mutex, tpp::condition_variable, the thread mailboxes and sleeps, on
/// registered threads and on threads that are not.
//-----------------------------------------------------------------------------
namespace sync_tests
{
using namespace std::chrono_literals;
using test_support::run_case;

/// A thread registered with tpp for as long as `body` runs.
auto start_registered(std::function<void()> body) -> std::thread
{
    return std::thread(
        [body = std::move(body)]()
        {
            tpp::this_thread::register_this_thread();
            body();
            tpp::this_thread::unregister_this_thread();
        });
}

void test_mutex_contention()
{
    constexpr static int thread_count = 4;
    constexpr static int lock_count = 2000;
    tpp::mutex mutex;
    int counter = 0;
    std::vector<std::thread> threads;
    for(int i = 0; i < thread_count; ++i)
    {
        threads.push_back(start_registered(
            [&mutex, &counter]()
            {
                for(int j = 0; j < lock_count; ++j)
                {
                    std::lock_guard<tpp::mutex> lock(mutex);
                    ++counter;
                }
            }));
    }
    for(auto& thread : threads)
    {
        thread.join();
    }
    EXPECT(counter == thread_count * lock_count);
}

void test_mutex_ownership()
{
    tpp::mutex mutex;
    mutex.lock();
    EXPECT_THROWS_AS(mutex.lock(), std::system_error);
    std::atomic<bool> has_locked{true};
    std::atomic<bool> has_thrown{false};
    auto other = start_registered(
        [&]()
        {
            has_locked = mutex.try_lock();
            try
            {
                mutex.unlock();
            }
            catch(const std::system_error&)
            {
                has_thrown = true;
            }
        });
    other.join();
    EXPECT(!has_locked.load());
    EXPECT(has_thrown.load());
    mutex.unlock();
    EXPECT(mutex.try_lock());
    mutex.unlock();
}

void test_producer_consumer()
{
    constexpr static int item_count = 2000;
    std::mutex mutex;
    tpp::condition_variable items_event;
    std::vector<int> items;
    auto producer = start_registered(
        [&]()
        {
            for(int i = 0; i < item_count; ++i)
            {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    items.push_back(i);
                }
                items_event.notify_one();
            }
        });
    long long sum = 0;
    int received = 0;
    while(received < item_count)
    {
        std::unique_lock<std::mutex> lock(mutex);
        while(items.empty())
        {
            items_event.wait(lock);
        }
        for(const auto item : items)
        {
            sum += item;
        }
        received += static_cast<int>(items.size());
        items.clear();
    }
    producer.join();
    EXPECT(sum == static_cast<long long>(item_count) * (item_count - 1) / 2);
}

void test_notify_all_wakes_every_waiter(bool is_registered)
{
    constexpr static int waiter_count = 4;
    std::mutex mutex;
    tpp::condition_variable ready_event;
    bool is_ready = false;
    std::atomic<int> waiting{0};
    std::atomic<int> woken{0};
    std::vector<std::thread> threads;
    for(int i = 0; i < waiter_count; ++i)
    {
        auto body = [&]()
        {
            std::unique_lock<std::mutex> lock(mutex);
            ++waiting;
            while(!is_ready)
            {
                ready_event.wait(lock);
            }
            ++woken;
        };
        threads.push_back(is_registered ? start_registered(body) : std::thread(body));
    }
    EXPECT(test_support::poll_until(
        [&waiting]()
        {
            return waiting.load() == waiter_count;
        }));
    {
        std::lock_guard<std::mutex> lock(mutex);
        is_ready = true;
    }
    ready_event.notify_all();
    for(auto& thread : threads)
    {
        thread.join();
    }
    EXPECT(woken.load() == waiter_count);
}

void test_wait_for_timeout()
{
    std::mutex mutex;
    tpp::condition_variable never_event;
    std::unique_lock<std::mutex> lock(mutex);
    const auto start = std::chrono::steady_clock::now();
    EXPECT(never_event.wait_for(lock, 10ms) == std::cv_status::timeout);
    EXPECT(std::chrono::steady_clock::now() - start >= 10ms);
    EXPECT(lock.owns_lock());
}

void test_wait_runs_own_tasks()
{
    // The task that ends the wait is queued to the waiting thread itself: only a wait that runs it returns.
    const auto main_id = tpp::this_thread::get_id();
    std::mutex mutex;
    tpp::condition_variable ready_event;
    bool is_ready = false;
    auto sender = start_registered(
        [&]()
        {
            tpp::invoke(main_id,
                        [&]()
                        {
                            {
                                std::lock_guard<std::mutex> lock(mutex);
                                is_ready = true;
                            }
                            ready_event.notify_one();
                        });
        });
    {
        std::unique_lock<std::mutex> lock(mutex);
        while(!is_ready)
        {
            ready_event.wait(lock);
        }
    }
    sender.join();
    EXPECT(is_ready);
}

void test_nested_waits_on_one_condition_variable()
{
    // A task run inside a wait waits on the same condition variable: a notify must reach the inner wait,
    // which has to end before the outer one can.
    const auto main_id = tpp::this_thread::get_id();
    std::mutex mutex;
    tpp::condition_variable change_event;
    bool is_outer_ready = false;
    bool is_inner_ready = false;
    std::atomic<bool> is_inner_waiting{false};
    std::atomic<bool> is_inner_done{false};
    tpp::invoke(main_id,
                [&]()
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    is_inner_waiting = true;
                    while(!is_inner_ready)
                    {
                        change_event.wait(lock);
                    }
                    is_inner_done = true;
                });
    auto helper = start_registered(
        [&]()
        {
            test_support::poll_until(
                [&]()
                {
                    return is_inner_waiting.load();
                });
            {
                std::lock_guard<std::mutex> lock(mutex);
                is_inner_ready = true;
            }
            change_event.notify_one();
            test_support::poll_until(
                [&]()
                {
                    return is_inner_done.load();
                });
            {
                std::lock_guard<std::mutex> lock(mutex);
                is_outer_ready = true;
            }
            change_event.notify_one();
        });
    {
        std::unique_lock<std::mutex> lock(mutex);
        while(!is_outer_ready)
        {
            change_event.wait(lock);
        }
    }
    helper.join();
    EXPECT(is_inner_done.load());
}

void test_mailbox_wakes()
{
    std::atomic<tpp::thread::id> waiter_id{tpp::invalid_id()};
    std::atomic<bool> is_done{false};
    std::atomic<long long> waited_ms{0};
    auto waiter = start_registered(
        [&]()
        {
            waiter_id = tpp::this_thread::get_id();
            const auto start = std::chrono::steady_clock::now();
            while(!is_done)
            {
                tpp::this_thread::wait_for(5s);
            }
            waited_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
                    .count();
        });
    EXPECT(test_support::poll_until(
        [&]()
        {
            return waiter_id.load() != tpp::invalid_id();
        }));
    tpp::invoke(waiter_id.load(),
                [&is_done]()
                {
                    is_done = true;
                });
    waiter.join();
    EXPECT(waited_ms.load() < 4000);

    // A notify sent before the thread waits still ends its next wait.
    std::atomic<bool> may_wait{false};
    std::atomic<bool> has_returned{false};
    waiter_id = tpp::invalid_id();
    auto late_waiter = start_registered(
        [&]()
        {
            waiter_id = tpp::this_thread::get_id();
            test_support::poll_until(
                [&may_wait]()
                {
                    return may_wait.load();
                });
            tpp::this_thread::wait();
            has_returned = true;
        });
    EXPECT(test_support::poll_until(
        [&]()
        {
            return waiter_id.load() != tpp::invalid_id();
        }));
    tpp::notify(waiter_id.load());
    may_wait = true;
    late_waiter.join();
    EXPECT(has_returned.load());
}

void test_sleep_runs_own_tasks()
{
    const auto main_id = tpp::this_thread::get_id();
    const auto main_thread = std::this_thread::get_id();
    std::atomic<bool> has_run_on_main{false};
    auto sender = start_registered(
        [&]()
        {
            tpp::invoke(main_id,
                        [&has_run_on_main, main_thread]()
                        {
                            has_run_on_main = std::this_thread::get_id() == main_thread;
                        });
        });
    sender.join();
    const auto start = std::chrono::steady_clock::now();
    tpp::this_thread::sleep_for(20ms);
    EXPECT(has_run_on_main.load());
    EXPECT(std::chrono::steady_clock::now() - start >= 20ms);

    std::atomic<long long> slept_us{0};
    std::thread unregistered(
        [&slept_us]()
        {
            const auto unregistered_start = std::chrono::steady_clock::now();
            tpp::this_thread::sleep_for(10ms);
            slept_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                             unregistered_start)
                           .count();
        });
    unregistered.join();
    EXPECT(slept_us.load() >= 10000);
}

void test_mailbox_order_and_dispatch()
{
    auto worker = tpp::make_thread("mailbox order");
    test_support::order_log order;
    constexpr static int task_count = 100;
    for(int i = 0; i < task_count; ++i)
    {
        tpp::invoke(worker.get_id(),
                    [&order, i]()
                    {
                        order.add(i);
                    });
    }
    tpp::async(worker.get_id(), []() {}).wait();
    const auto values = order.get();
    bool is_in_order = values.size() == static_cast<size_t>(task_count);
    for(int i = 0; is_in_order && i < task_count; ++i)
    {
        is_in_order = values[static_cast<size_t>(i)] == i;
    }
    EXPECT(is_in_order);
    bool has_run = false;
    tpp::dispatch(tpp::this_thread::get_id(),
                  [&has_run]()
                  {
                      has_run = true;
                  });
    EXPECT(has_run);
}

void test_unregistered_promise_waits()
{
    tpp::promise<int> promise;
    auto future = promise.get_future().share();
    std::atomic<int> sum{0};
    std::vector<std::thread> waiters;
    for(int i = 0; i < 4; ++i)
    {
        waiters.emplace_back(
            [future, &sum]()
            {
                sum += future.get();
            });
    }
    std::this_thread::sleep_for(5ms);
    promise.set_value(3);
    for(auto& waiter : waiters)
    {
        waiter.join();
    }
    EXPECT(sum.load() == 12);
}

void run_tests()
{
    const auto& options = test_support::get_options();
    for(int i = 0; i < options.iterations; ++i)
    {
        run_case("sync : a contended mutex counts every lock", test_mutex_contention);
        run_case("sync : a mutex knows its owner", test_mutex_ownership);
        run_case("sync : a condition variable hands over every item", test_producer_consumer);
        run_case("sync : notify_all wakes every registered waiter",
                 []()
                 {
                     test_notify_all_wakes_every_waiter(true);
                 });
        run_case("sync : notify_all wakes every waiter that is not registered",
                 []()
                 {
                     test_notify_all_wakes_every_waiter(false);
                 });
        run_case("sync : wait_for times out and keeps the lock", test_wait_for_timeout);
        run_case("sync : a condition variable wait runs the thread's tasks", test_wait_runs_own_tasks);
        run_case("sync : nested waits on one condition variable end", test_nested_waits_on_one_condition_variable);
        run_case("sync : mailbox tasks and notify wake a waiting thread", test_mailbox_wakes);
        run_case("sync : a sleep runs the thread's tasks and still sleeps", test_sleep_runs_own_tasks);
        run_case("sync : mailbox tasks run first queued first; dispatch runs inline", test_mailbox_order_and_dispatch);
        run_case("sync : threads that are not registered wait on a promise", test_unregistered_promise_waits);
    }
}
} // namespace sync_tests
