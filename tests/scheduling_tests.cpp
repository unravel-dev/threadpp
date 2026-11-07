#include "scheduling_tests.h"
#include "test_support.h"

#include <suitepp/suite.hpp>
#include <threadpp/condition_variable.hpp>
#include <threadpp/thread_pool.h>
#include <threadpp/when_all_any.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

//-----------------------------------------------------------------------------
/// The scheduling contract, at every pool size (test_support::get_worker_counts):
/// - liveness: a wait returns once queued work can produce its result;
/// - placement: a pool job runs on a worker, or on a thread that waits for it
///   when no worker is free; never in a timed wait of a thread outside the pool;
/// - order: category, then priority, then first scheduled first;
/// - re-entrancy: waits run the thread's own tasks, never its on-process tasks;
/// - lifetime: futures outlive their pool, a moved pool keeps its jobs.
//-----------------------------------------------------------------------------
namespace scheduling_tests
{
using namespace std::chrono_literals;
using test_support::name_for;
using test_support::run_case;

auto make_workers(size_t worker_count) -> std::map<tpp::priority::category, size_t>
{
    return {{tpp::priority::category::low, worker_count}};
}

/// Keeps every worker of a pool busy until released, so only waiters run its queued jobs.
class busy_workers
{
public:
    busy_workers(tpp::thread_pool& pool, size_t worker_count) : worker_count_(worker_count)
    {
        for(size_t i = 0; i < worker_count; ++i)
        {
            jobs_.push_back(pool.schedule(
                [this]()
                {
                    gate_.wait();
                }));
        }
    }

    ~busy_workers()
    {
        release();
    }

    busy_workers(const busy_workers&) = delete;
    auto operator=(const busy_workers&) -> busy_workers& = delete;

    /// Whether every worker runs one of the holding jobs.
    auto is_holding() const -> bool
    {
        return test_support::poll_until(
            [this]()
            {
                return gate_.get_waiting_count() == static_cast<int>(worker_count_);
            });
    }

    void release()
    {
        gate_.open();
        for(auto& job : jobs_)
        {
            job.wait();
        }
        jobs_.clear();
    }

private:
    size_t worker_count_{};
    test_support::gate gate_;
    std::vector<tpp::job_future<void>> jobs_;
};

/// Lets parked workers settle after the pool starts or a burst ends.
void let_workers_settle()
{
    std::this_thread::sleep_for(test_support::scaled(20ms));
}

//-----------------------------------------------------------------------------
/// Liveness
//-----------------------------------------------------------------------------
void test_job_future(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    auto job = pool.schedule(
        []()
        {
            return 42;
        });
    EXPECT(job.get() == 42);
    // A finished job leaves the count just after its future is ready.
    EXPECT(test_support::poll_until(
        [&pool]()
        {
            return pool.get_jobs_count() == 0u;
        }));
}

void test_continuation_on_waiting_thread(size_t worker_count)
{
    // The engine's script compile pattern: a job, a continuation on the main thread, a wait on the continuation.
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    auto job = pool.schedule(
        []()
        {
            return 20;
        });
    auto continuation = job.then(main_id,
                                 [](tpp::future<int> parent)
                                 {
                                     return parent.get() + 1;
                                 });
    auto chained = continuation.then(main_id,
                                     [](tpp::future<int> parent)
                                     {
                                         return parent.get() * 2;
                                     });
    chained.wait();
    EXPECT(chained.get() == 42);
}

void test_continuation_of_shared_job(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    auto job = pool.schedule(
                       []()
                       {
                           return 5;
                       })
                   .share();
    auto first = job.then(main_id,
                          [](tpp::shared_future<int> parent)
                          {
                              return parent.get() + 1;
                          });
    auto second = job.then(main_id,
                           [](tpp::shared_future<int> parent)
                           {
                               return parent.get() + 2;
                           });
    EXPECT(first.get() + second.get() == 13);
}

void test_when_all_of_jobs(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    auto first = pool.schedule(
        []()
        {
            return 1;
        });
    auto second = pool.schedule(
        []()
        {
            return 2;
        });
    auto all = tpp::when_all(std::move(first), std::move(second));
    auto results = all.get();
    EXPECT(std::get<0>(results).get() + std::get<1>(results).get() == 3);

    std::vector<tpp::future<int>> futures;
    constexpr int job_count = 16;
    for(int i = 0; i < job_count; ++i)
    {
        futures.emplace_back(pool.schedule(
            [i]()
            {
                return i;
            }));
    }
    auto all_of_range = tpp::when_all(futures.begin(), futures.end());
    int sum = 0;
    for(auto& future : all_of_range.get())
    {
        sum += future.get();
    }
    EXPECT(sum == job_count * (job_count - 1) / 2);
}

void test_when_any_of_jobs(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    auto first = pool.schedule(
        []()
        {
            return 1;
        });
    auto second = pool.schedule(
        []()
        {
            return 2;
        });
    auto any = tpp::when_any(std::move(first), std::move(second));
    auto result = any.get();
    EXPECT(result.index < 2u);

    std::vector<tpp::future<int>> futures;
    for(int i = 0; i < 4; ++i)
    {
        futures.emplace_back(pool.schedule(
            [i]()
            {
                return i;
            }));
    }
    auto any_of_range = tpp::when_any(futures.begin(), futures.end());
    auto range_result = any_of_range.get();
    EXPECT(range_result.index < 4u);
    EXPECT(range_result.futures[range_result.index].is_ready());
}

void test_promise_set_by_job(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    auto promise = std::make_shared<tpp::promise<int>>();
    auto future = promise->get_future();
    pool.schedule(
        [promise]()
        {
            promise->set_value(7);
        });
    EXPECT(future.get() == 7);
}

void test_polling_with_sleep(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    auto job = pool.schedule(
        []()
        {
            return 3;
        });
    while(!job.is_ready())
    {
        tpp::this_thread::sleep_for(1ms);
    }
    EXPECT(job.get() == 3);
    auto continuation = pool.schedule(
                                []()
                                {
                                    return 4;
                                })
                            .then(main_id,
                                  [](tpp::future<int> parent)
                                  {
                                      return parent.get() + 1;
                                  });
    while(!continuation.is_ready())
    {
        tpp::this_thread::sleep_for(1ms);
    }
    EXPECT(continuation.get() == 5);
}

void test_every_worker_waits_on_a_child(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    const size_t outer_count = std::max<size_t>(worker_count, 1) * 3;
    std::vector<tpp::job_future<int>> outers;
    for(size_t i = 0; i < outer_count; ++i)
    {
        outers.push_back(pool.schedule(
            [&pool]()
            {
                auto child = pool.schedule(
                    []()
                    {
                        std::this_thread::sleep_for(100us);
                        return 1;
                    });
                return child.get() + 1;
            }));
    }
    int sum = 0;
    for(auto& outer : outers)
    {
        sum += outer.get();
    }
    EXPECT(sum == static_cast<int>(outer_count) * 2);
}

auto run_nested(tpp::thread_pool& pool, int depth) -> int
{
    if(depth == 0)
    {
        return 0;
    }
    return pool
               .schedule(
                   [&pool, depth]()
                   {
                       return run_nested(pool, depth - 1);
                   })
               .get() +
           1;
}

auto run_fork_join(tpp::thread_pool& pool, int depth, int fanout) -> int
{
    if(depth == 0)
    {
        return 1;
    }
    std::vector<tpp::job_future<int>> children;
    for(int i = 0; i < fanout; ++i)
    {
        children.push_back(pool.schedule(
            [&pool, depth, fanout]()
            {
                return run_fork_join(pool, depth - 1, fanout);
            }));
    }
    int leaves = 0;
    for(auto& child : children)
    {
        leaves += child.get();
    }
    return leaves;
}

void test_nested_waits(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    constexpr int depth = 24;
    EXPECT(run_nested(pool, depth) == depth);
    constexpr int tree_depth = 4;
    constexpr int fanout = 4;
    EXPECT(run_fork_join(pool, tree_depth, fanout) == 256);
}

void test_unregistered_waiter(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    auto job = pool.schedule(
        []()
        {
            return 9;
        });
    std::atomic<int> job_result{0};
    std::thread job_waiter(
        [&job, &job_result]()
        {
            job_result = job.get();
        });
    job_waiter.join();
    EXPECT(job_result.load() == 9);

    tpp::promise<int> promise;
    auto future = promise.get_future();
    std::atomic<int> promise_result{0};
    std::atomic<bool> is_waiting{false};
    std::thread promise_waiter(
        [&]()
        {
            is_waiting = true;
            promise_result = future.get();
        });
    test_support::poll_until(
        [&is_waiting]()
        {
            return is_waiting.load();
        });
    std::this_thread::sleep_for(5ms);
    promise.set_value(4);
    promise_waiter.join();
    EXPECT(promise_result.load() == 4);
}

void test_promise_inside_job(size_t worker_count)
{
    // The producer is scheduled by the waiting job itself: a wait inside the job finds it in its region.
    tpp::thread_pool pool(make_workers(worker_count));
    auto outer = pool.schedule(
        [&pool]()
        {
            auto promise = std::make_shared<tpp::promise<int>>();
            auto future = promise->get_future();
            pool.schedule(
                [promise]()
                {
                    promise->set_value(5);
                });
            return future.get() * 2;
        });
    EXPECT(outer.get() == 10);
}

void test_condition_variable_inside_job(size_t worker_count)
{
    struct shared_state
    {
        std::mutex mutex;
        tpp::condition_variable ready_event;
        bool is_ready{false};
    };
    tpp::thread_pool pool(make_workers(worker_count));
    auto state = std::make_shared<shared_state>();
    auto outer = pool.schedule(
        [&pool, state]()
        {
            pool.schedule(
                [state]()
                {
                    {
                        std::lock_guard<std::mutex> lock(state->mutex);
                        state->is_ready = true;
                    }
                    state->ready_event.notify_all();
                });
            std::unique_lock<std::mutex> lock(state->mutex);
            while(!state->is_ready)
            {
                state->ready_event.wait(lock);
            }
            return 1;
        });
    EXPECT(outer.get() == 1);
}

//-----------------------------------------------------------------------------
/// Placement and responsiveness
//-----------------------------------------------------------------------------
void test_idle_worker_runs_waited_job(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_thread = std::this_thread::get_id();
    constexpr int rounds = 10;
    int on_main = 0;
    for(int i = 0; i < rounds; ++i)
    {
        let_workers_settle();
        std::atomic<std::thread::id> ran_on{};
        auto job = pool.schedule(
            [&ran_on]()
            {
                ran_on = std::this_thread::get_id();
            });
        job.get();
        on_main += ran_on.load() == main_thread ? 1 : 0;
    }
    EXPECT(on_main == 0);
}

void test_busy_workers_leave_waited_job_to_waiter(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    busy_workers busy(pool, worker_count);
    EXPECT(busy.is_holding());
    const auto main_thread = std::this_thread::get_id();
    std::atomic<std::thread::id> ran_on{};
    auto job = pool.schedule(
        [&ran_on]()
        {
            ran_on = std::this_thread::get_id();
            return 1;
        });
    EXPECT(job.get() == 1);
    const bool is_on_main = ran_on.load() == main_thread;
    EXPECT(is_on_main);
}

void test_timed_wait_runs_no_job_with_workers(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    busy_workers busy(pool, worker_count);
    EXPECT(busy.is_holding());
    std::atomic<bool> has_run{false};
    auto job = pool.schedule(
        [&has_run]()
        {
            has_run = true;
        });
    EXPECT(job.wait_for(20ms) == std::future_status::timeout);
    tpp::this_thread::sleep_for(5ms);
    EXPECT(!has_run.load());
    busy.release();
    job.get();
    EXPECT(has_run.load());
}

void test_timed_wait_runs_job_without_workers()
{
    tpp::thread_pool pool(make_workers(0));
    auto job = pool.schedule(
        []()
        {
            return 6;
        });
    EXPECT(job.wait_for(0ms) == std::future_status::ready);
    auto other = pool.schedule(
        []()
        {
            return 7;
        });
    EXPECT(other.wait_until(std::chrono::steady_clock::now()) == std::future_status::ready);
    EXPECT(job.get() + other.get() == 13);
}

void test_wait_all_polling(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    constexpr int job_count = 12;
    std::atomic<int> runs{0};
    for(int i = 0; i < job_count; ++i)
    {
        pool.schedule(tpp::priority::normal(),
                      [&runs]()
                      {
                          std::this_thread::sleep_for(1ms);
                          ++runs;
                      });
    }
    bool has_wrong_count = false;
    pool.wait_all_polling(tpp::priority::category::low,
                          [&has_wrong_count](const tpp::thread_pool::progress_info& info)
                          {
                              has_wrong_count = has_wrong_count || info.current_job > info.total_jobs ||
                                                info.total_jobs > static_cast<size_t>(job_count);
                          });
    EXPECT(runs.load() == job_count);
    EXPECT(!has_wrong_count);
    // A finished job leaves the count just after its future is ready.
    EXPECT(test_support::poll_until(
        [&pool]()
        {
            return pool.get_jobs_count() == 0u;
        }));
}

void test_sleep_runs_jobs_only_on_host(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    busy_workers busy(pool, worker_count);
    EXPECT(busy.is_holding());
    std::atomic<bool> has_run{false};
    auto job = pool.schedule(
        [&has_run]()
        {
            has_run = true;
        });
    tpp::this_thread::sleep_for(10ms);
    // The host of a pool without workers runs its jobs in a sleep; any other thread only sleeps.
    EXPECT(has_run.load() == (worker_count == 0));
    busy.release();
    job.get();
}

void test_levels_without_eligible_worker(size_t worker_count)
{
    // Workers only for high jobs: a waited low job runs on the waiter.
    tpp::thread_pool pool({{tpp::priority::category::high, worker_count}});
    auto job = pool.schedule(tpp::priority::low(),
                             []()
                             {
                                 return 2;
                             });
    EXPECT(job.get() == 2);
    auto high_job = pool.schedule(tpp::priority::high(),
                                  []()
                                  {
                                      return 3;
                                  });
    EXPECT(high_job.get() == 3);
}

//-----------------------------------------------------------------------------
/// Order
//-----------------------------------------------------------------------------
/// Schedules the mixed set every order case uses; returns the order it must run in.
auto schedule_ordered_set(tpp::thread_pool& pool, test_support::order_log& order) -> std::vector<int>
{
    const auto add = [&pool, &order](tpp::priority::group group, int value)
    {
        pool.schedule(group,
                      [&order, value]()
                      {
                          order.add(value);
                      });
    };
    add(tpp::priority::low(), 300);
    for(int i = 0; i < 5; ++i)
    {
        add(tpp::priority::normal(), i);
    }
    add(tpp::priority::normal(5), 100);
    add(tpp::priority::high(), 200);
    add(tpp::priority::critical(), 400);
    return {400, 200, 100, 0, 1, 2, 3, 4, 300};
}

void test_order_without_workers()
{
    tpp::thread_pool pool(make_workers(0));
    test_support::order_log order;
    const auto expected = schedule_ordered_set(pool, order);
    EXPECT(pool.run_pending(10s) == expected.size());
    EXPECT(order.get() == expected);
}

void test_order_with_one_worker()
{
    tpp::thread_pool pool(make_workers(1));
    test_support::order_log order;
    std::vector<int> expected;
    {
        busy_workers busy(pool, 1);
        EXPECT(busy.is_holding());
        expected = schedule_ordered_set(pool, order);
    }
    EXPECT(test_support::poll_until(
        [&order, &expected]()
        {
            return order.get().size() == expected.size();
        }));
    EXPECT(order.get() == expected);
}

void test_change_priority_runs_once()
{
    tpp::thread_pool pool(make_workers(0));
    test_support::order_log order;
    std::vector<tpp::job_future<void>> jobs;
    for(int i = 0; i < 3; ++i)
    {
        jobs.push_back(pool.schedule(
            [&order, i]()
            {
                order.add(i);
            }));
    }
    jobs[2].change_priority(tpp::priority::high());
    jobs[2].change_priority(tpp::priority::critical());
    EXPECT(pool.run_pending(10s) == 3u);
    EXPECT((order.get() == std::vector<int>{2, 0, 1}));
    EXPECT(pool.run_pending(10s) == 0u);
}

void test_waited_job_starts_first()
{
    tpp::thread_pool pool(make_workers(0));
    test_support::order_log order;
    std::vector<tpp::job_future<void>> jobs;
    for(int i = 0; i < 3; ++i)
    {
        jobs.push_back(pool.schedule(
            [&order, i]()
            {
                order.add(i);
            }));
    }
    jobs[2].get();
    EXPECT((order.get() == std::vector<int>{2}));
    EXPECT(pool.run_pending(10s) == 2u);
    EXPECT((order.get() == std::vector<int>{2, 0, 1}));
}

//-----------------------------------------------------------------------------
/// Re-entrancy
//-----------------------------------------------------------------------------
void test_job_calls_waiting_thread(size_t worker_count)
{
    // The asset reader pattern: a job hands work to the main thread and waits, while main waits on the job.
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    auto job = pool.schedule(
        [main_id]()
        {
            return tpp::async(main_id,
                              []()
                              {
                                  return 2;
                              })
                       .get() *
                   3;
        });
    EXPECT(job.get() == 6);
}

void test_on_process_task_waits_for_process(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    std::atomic<bool> has_run{false};
    tpp::invoke_on_process(main_id,
                           [&has_run]()
                           {
                               has_run = true;
                           });
    auto job = pool.schedule(
        []()
        {
            return 1;
        });
    EXPECT(job.get() == 1);
    tpp::this_thread::sleep_for(2ms);
    EXPECT(!has_run.load());
    tpp::this_thread::process();
    EXPECT(has_run.load());
}

//-----------------------------------------------------------------------------
/// Cancelling, deferring, failing
//-----------------------------------------------------------------------------
void test_stop_breaks_promise_without_lock(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    busy_workers busy(pool, worker_count);
    EXPECT(busy.is_holding());
    const auto main_id = tpp::this_thread::get_id();
    auto job = pool.schedule(
        []()
        {
            return 1;
        });
    bool has_rescheduled = false;
    // Runs inline as the stop breaks the promise: it uses the pool, which must not be locked then.
    auto continuation = job.then(main_id,
                                 [&pool, &has_rescheduled](tpp::future<int> parent)
                                 {
                                     bool is_broken = false;
                                     try
                                     {
                                         parent.get();
                                     }
                                     catch(const std::future_error&)
                                     {
                                         is_broken = true;
                                     }
                                     pool.schedule([]() {});
                                     has_rescheduled = true;
                                     return is_broken;
                                 });
    job.stop();
    EXPECT(continuation.get());
    EXPECT(has_rescheduled);
    auto other = pool.schedule(
        []()
        {
            return 2;
        });
    auto other_continuation = other.then(main_id,
                                         [&pool](tpp::future<int> parent)
                                         {
                                             pool.schedule([]() {});
                                             return parent.has_error();
                                         });
    pool.stop_all();
    EXPECT(other_continuation.get());
}

void test_deferred_job(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    std::atomic<int> runs{0};
    auto job = pool.create_job(
        [&runs]()
        {
            return ++runs;
        });
    auto copy = job.share();
    EXPECT(!copy.is_submitted());
    EXPECT(!copy.is_ready());
    EXPECT(pool.run_pending(1s) == 0u);
    EXPECT(copy.wait_for(1ms) == std::future_status::timeout);
    EXPECT(runs.load() == 0);
    EXPECT(copy.get() == 1);
    auto second_copy = copy;
    EXPECT(second_copy.get() == 1);
    EXPECT(runs.load() == 1);
}

void test_exceptions(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    auto failing = pool.schedule(
        []() -> int
        {
            throw std::runtime_error("job failed");
        });
    EXPECT_THROWS_AS(failing.get(), std::runtime_error);
    auto continuation = pool.schedule(
                                []() -> int
                                {
                                    throw std::runtime_error("job failed");
                                })
                            .then(main_id,
                                  [](tpp::future<int> parent)
                                  {
                                      return parent.has_error();
                                  });
    EXPECT(continuation.get());
    auto good = pool.schedule(
        []()
        {
            return 1;
        });
    auto bad = pool.schedule(
        []() -> int
        {
            throw std::runtime_error("job failed");
        });
    auto all = tpp::when_all(std::move(good), std::move(bad)).get();
    EXPECT(std::get<0>(all).get() == 1);
    EXPECT(std::get<1>(all).has_error());
}

//-----------------------------------------------------------------------------
/// Lifetime
//-----------------------------------------------------------------------------
void test_future_outlives_pool(size_t worker_count)
{
    tpp::job_future<int> job;
    tpp::job_future<int> deferred;
    {
        tpp::thread_pool pool(make_workers(worker_count));
        job = pool.schedule(
            []()
            {
                return 5;
            });
        deferred = pool.create_job(
            []()
            {
                return 6;
            });
    }
    job.change_priority(tpp::priority::high());
    job.stop();
    deferred.submit();
    bool is_valid_outcome = false;
    try
    {
        is_valid_outcome = job.get() == 5;
    }
    catch(const std::future_error&)
    {
        is_valid_outcome = true;
    }
    EXPECT(is_valid_outcome);
    EXPECT_THROWS_AS(deferred.get(), std::future_error);
}

void test_moved_pool_keeps_jobs(size_t worker_count)
{
    tpp::thread_pool first(make_workers(worker_count));
    auto deferred = first.create_job(
        []()
        {
            return 8;
        });
    tpp::thread_pool second(std::move(first));
    EXPECT(deferred.get() == 8);
    auto job = second.schedule(
        []()
        {
            return 9;
        });
    tpp::thread_pool third(make_workers(1));
    third = std::move(second);
    EXPECT(job.get() == 9);
    EXPECT(third
               .schedule(
                   []()
                   {
                       return 10;
                   })
               .get() == 10);
}

void test_own_stack_wait_is_reported(size_t worker_count)
{
    test_support::clear_log_lines();
    tpp::thread_pool pool(make_workers(worker_count));
    auto self = std::make_shared<tpp::job_shared_future<int>>();
    auto job = pool.create_job(
                       "self waiter",
                       [self]()
                       {
                           return self->wait_for(20ms) == std::future_status::timeout ? 1 : 0;
                       })
                   .share();
    *self = job;
    job.submit();
    EXPECT(job.get() == 1);
    EXPECT(test_support::has_log_line_containing("further up the same thread"));
    self.reset();
}

void test_host_widens_with_a_log_line()
{
    test_support::clear_log_lines();
    tpp::thread_pool pool(make_workers(0));
    auto promise = std::make_shared<tpp::promise<int>>();
    auto future = promise->get_future().share();
    pool.schedule("producer",
                  [promise]()
                  {
                      promise->set_value(1);
                  });
    auto outer = pool.schedule("consumer",
                               [future]()
                               {
                                   return future.get() + 1;
                               });
    EXPECT(outer.get() == 2);
    EXPECT(test_support::has_log_line_containing("none of its own jobs queued"));
}

//-----------------------------------------------------------------------------
/// async and then without a thread
//-----------------------------------------------------------------------------
void test_detached_executor(size_t worker_count)
{
    const auto main_thread = std::this_thread::get_id();
    {
        auto on_thread = tpp::async(
            []()
            {
                return std::this_thread::get_id();
            });
        const bool is_elsewhere = on_thread.get() != main_thread;
        EXPECT(is_elsewhere);
    }
    tpp::thread_pool pool(make_workers(worker_count));
    std::atomic<int> executed{0};
    tpp::set_detached_executor(
        [&pool, &executed](tpp::task work)
        {
            ++executed;
            pool.schedule("detached", std::move(work));
        });
    auto result = tpp::async(
        []()
        {
            return 5;
        });
    EXPECT(result.get() == 5);
    auto continuation = pool.schedule(
                                []()
                                {
                                    return 1;
                                })
                            .then(
                                [](tpp::future<int> parent)
                                {
                                    return parent.get() + 1;
                                });
    EXPECT(continuation.get() == 2);
    tpp::set_detached_executor(nullptr);
    EXPECT(executed.load() == 2);
}

void run_pool_cases(size_t worker_count)
{
    const auto run = [worker_count](const std::string& text, void (*test)(size_t))
    {
        run_case(name_for(worker_count, text),
                 [worker_count, test]()
                 {
                     test(worker_count);
                 });
    };
    run("a wait on a job returns its value", test_job_future);
    run("a wait on a continuation on the waiting thread returns", test_continuation_on_waiting_thread);
    run("continuations of a shared job return", test_continuation_of_shared_job);
    run("when_all of jobs returns", test_when_all_of_jobs);
    run("when_any of jobs returns", test_when_any_of_jobs);
    run("a wait on a promise a job sets returns", test_promise_set_by_job);
    run("a polling loop with sleep_for ends", test_polling_with_sleep);
    run("jobs that all wait on a child job end", test_every_worker_waits_on_a_child);
    run("nested waits and fork-join trees end", test_nested_waits);
    run("a thread that is not registered can wait", test_unregistered_waiter);
    run("a job waiting on a promise of its own child ends", test_promise_inside_job);
    run("a job waiting on a condition variable of its own child ends", test_condition_variable_inside_job);
    run("a sleep runs pool jobs only on a host", test_sleep_runs_jobs_only_on_host);
    run("a waited low job runs with workers only for high jobs", test_levels_without_eligible_worker);
    run("wait_all_polling runs every job", test_wait_all_polling);
    run("a job hands work to the waiting thread", test_job_calls_waiting_thread);
    run("on-process tasks run only in process", test_on_process_task_waits_for_process);
    run("stop breaks the promise outside the pool lock", test_stop_breaks_promise_without_lock);
    run("a deferred job runs once, when got", test_deferred_job);
    run("exceptions reach waits and continuations", test_exceptions);
    run("futures outlive their pool", test_future_outlives_pool);
    run("a moved pool keeps its jobs", test_moved_pool_keeps_jobs);
    run("a wait on a job further up the stack is reported", test_own_stack_wait_is_reported);
    run("async and then without a thread use the detached executor", test_detached_executor);
    if(worker_count > 0)
    {
        run("an idle worker runs a waited job", test_idle_worker_runs_waited_job);
        run("a waiter runs its job when every worker is busy", test_busy_workers_leave_waited_job_to_waiter);
        run("a timed wait runs no job while the pool has workers", test_timed_wait_runs_no_job_with_workers);
    }
}

void run_tests()
{
    const auto& options = test_support::get_options();
    for(int i = 0; i < options.iterations; ++i)
    {
        for(const auto worker_count : test_support::get_worker_counts())
        {
            run_pool_cases(worker_count);
        }
        run_case("pool[0] : a timed wait runs the job", test_timed_wait_runs_job_without_workers);
        run_case("pool[0] : jobs start by category, priority, then first scheduled", test_order_without_workers);
        run_case("pool[1] : jobs start by category, priority, then first scheduled", test_order_with_one_worker);
        run_case("pool[0] : a raised job runs once and first", test_change_priority_runs_once);
        run_case("pool[0] : a waited job starts ahead of the queue", test_waited_job_starts_first);
        run_case("pool[0] : a host widens to other jobs with a log line", test_host_widens_with_a_log_line);
    }
}
} // namespace scheduling_tests
