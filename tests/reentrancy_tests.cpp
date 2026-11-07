#include "reentrancy_tests.h"
#include "test_support.h"

#include <suitepp/suite.hpp>
#include <threadpp/thread_pool.h>

#include <atomic>
#include <chrono>
#include <map>
#include <string>
#include <thread>
#include <vector>

//-----------------------------------------------------------------------------
/// A thread blocked in a wait runs the tasks other threads hand it, so work that
/// the awaited work itself needs from the waiting thread gets done: the waiting
/// thread asyncs A elsewhere, A hands B back to the waiting thread and waits for
/// it, the waiting thread runs B inside its wait on A, A finishes, the wait ends.
/// On a thread's mailbox and on the job pool at every size.
//-----------------------------------------------------------------------------
namespace reentrancy_tests
{
using namespace std::chrono_literals;
using test_support::name_for;
using test_support::run_case;

auto make_workers(size_t worker_count) -> std::map<tpp::priority::category, size_t>
{
    return {{tpp::priority::category::low, worker_count}};
}

//-----------------------------------------------------------------------------
/// On threads
//-----------------------------------------------------------------------------
void test_thread_job_hands_work_back()
{
    auto worker = tpp::make_thread("reentrancy worker");
    const auto main_id = tpp::this_thread::get_id();
    const auto main_thread = std::this_thread::get_id();
    std::atomic<bool> is_b_on_main{false};
    auto a = tpp::async(worker.get_id(),
                        [&]()
                        {
                            auto b = tpp::async(main_id,
                                                [&]()
                                                {
                                                    is_b_on_main = std::this_thread::get_id() == main_thread;
                                                    return 2;
                                                });
                            return b.get() * 3;
                        });
    EXPECT(a.get() == 6);
    EXPECT(is_b_on_main.load());
}

void test_thread_job_waits_on_continuation_on_waiter()
{
    auto worker = tpp::make_thread("reentrancy worker");
    auto other = tpp::make_thread("reentrancy other");
    const auto main_id = tpp::this_thread::get_id();
    const auto main_thread = std::this_thread::get_id();
    const auto other_id = other.get_id();
    std::atomic<bool> is_b_on_main{false};
    auto a = tpp::async(worker.get_id(),
                        [&]()
                        {
                            return tpp::async(other_id,
                                              []()
                                              {
                                                  return 4;
                                              })
                                .then(main_id,
                                      [&](tpp::future<int> x)
                                      {
                                          is_b_on_main = std::this_thread::get_id() == main_thread;
                                          return x.get() + 1;
                                      })
                                .get();
                        });
    EXPECT(a.get() == 5);
    EXPECT(is_b_on_main.load());
}

/// `here` hands the next level to `there` and waits: both threads end up nested in waits on each other.
auto bounce(int depth, tpp::thread::id here, tpp::thread::id there) -> int
{
    if(depth == 0)
    {
        return 0;
    }
    return tpp::async(there,
                      [depth, here, there]()
                      {
                          return bounce(depth - 1, there, here);
                      })
               .get() +
           1;
}

void test_threads_bounce_work_back_and_forth()
{
    auto worker = tpp::make_thread("reentrancy worker");
    constexpr int depth = 16;
    EXPECT(bounce(depth, tpp::this_thread::get_id(), worker.get_id()) == depth);
}

void test_wait_runs_earlier_tasks_first()
{
    // The waiting thread's tasks run in the order they were queued: those queued before B run first.
    auto worker = tpp::make_thread("reentrancy worker");
    const auto main_id = tpp::this_thread::get_id();
    test_support::order_log order;
    for(int i = 0; i < 3; ++i)
    {
        tpp::invoke(main_id,
                    [&order, i]()
                    {
                        order.add(i);
                    });
    }
    auto a = tpp::async(worker.get_id(),
                        [&order, main_id]()
                        {
                            return tpp::async(main_id,
                                              [&order]()
                                              {
                                                  order.add(100);
                                                  return 1;
                                              })
                                .get();
                        });
    EXPECT(a.get() == 1);
    EXPECT((order.get() == std::vector<int>{0, 1, 2, 100}));
}

void test_timed_wait_runs_handed_back_work()
{
    auto worker = tpp::make_thread("reentrancy worker");
    const auto main_id = tpp::this_thread::get_id();
    auto a = tpp::async(worker.get_id(),
                        [main_id]()
                        {
                            return tpp::async(main_id,
                                              []()
                                              {
                                                  return 9;
                                              })
                                .get();
                        });
    while(a.wait_for(1ms) != std::future_status::ready)
    {
    }
    EXPECT(a.get() == 9);
}

void test_on_process_work_waits_for_process()
{
    // An on-process task never runs inside a wait: A cannot finish until the waiting thread calls process().
    auto worker = tpp::make_thread("reentrancy worker");
    const auto main_id = tpp::this_thread::get_id();
    auto a = tpp::async(worker.get_id(),
                        [main_id]()
                        {
                            return tpp::async_on_process(main_id,
                                                         []()
                                                         {
                                                             return 7;
                                                         })
                                .get();
                        });
    EXPECT(a.wait_for(30ms) == std::future_status::timeout);
    tpp::this_thread::process();
    EXPECT(a.get() == 7);
}

//-----------------------------------------------------------------------------
/// On the job pool
//-----------------------------------------------------------------------------
void test_job_waits_on_continuation_on_waiter(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    const auto main_thread = std::this_thread::get_id();
    std::atomic<bool> is_b_on_main{false};
    auto a = pool.schedule(
        [&]()
        {
            return pool
                .schedule(
                    []()
                    {
                        return 4;
                    })
                .then(main_id,
                      [&](tpp::future<int> x)
                      {
                          is_b_on_main = std::this_thread::get_id() == main_thread;
                          return x.get() + 1;
                      })
                .get();
        });
    EXPECT(a.get() == 5);
    EXPECT(is_b_on_main.load());
}

auto bounce_on_main(tpp::thread_pool& pool, tpp::thread::id main_id, int depth) -> int;

/// A job hands the next level back to the main thread and waits for it.
auto bounce_in_job(tpp::thread_pool& pool, tpp::thread::id main_id, int depth) -> int
{
    if(depth == 0)
    {
        return 0;
    }
    return tpp::async(main_id,
                      [&pool, main_id, depth]()
                      {
                          return bounce_on_main(pool, main_id, depth - 1);
                      })
               .get() +
           1;
}

/// The main thread schedules the next level as a job and waits for it.
auto bounce_on_main(tpp::thread_pool& pool, tpp::thread::id main_id, int depth) -> int
{
    return pool
        .schedule(
            [&pool, main_id, depth]()
            {
                return bounce_in_job(pool, main_id, depth);
            })
        .get();
}

void test_main_and_jobs_bounce_work_back_and_forth(size_t worker_count)
{
    tpp::thread_pool pool(make_workers(worker_count));
    constexpr int depth = 12;
    EXPECT(bounce_on_main(pool, tpp::this_thread::get_id(), depth) == depth);
}

void test_jobs_hand_work_back_while_main_waits_on_all(size_t worker_count)
{
    // Many jobs need the main thread at once while it waits on each of them in turn.
    constexpr int job_count = 32;
    tpp::thread_pool pool(make_workers(worker_count));
    const auto main_id = tpp::this_thread::get_id();
    std::atomic<int> on_main{0};
    std::vector<tpp::job_future<int>> jobs;
    for(int i = 0; i < job_count; ++i)
    {
        jobs.push_back(pool.schedule(
            [main_id, &on_main, i]()
            {
                return tpp::async(main_id,
                                  [&on_main, i]()
                                  {
                                      ++on_main;
                                      return i;
                                  })
                    .get();
            }));
    }
    int sum = 0;
    for(auto& job : jobs)
    {
        sum += job.get();
    }
    EXPECT(sum == job_count * (job_count - 1) / 2);
    EXPECT(on_main.load() == job_count);
}

void run_tests()
{
    const auto& options = test_support::get_options();
    for(int i = 0; i < options.iterations; ++i)
    {
        run_case("re-entrancy : a job on a thread hands work back to the waiting thread",
                 test_thread_job_hands_work_back);
        run_case("re-entrancy : a job on a thread waits on a continuation on the waiting thread",
                 test_thread_job_waits_on_continuation_on_waiter);
        run_case("re-entrancy : two threads bounce work back and forth, nested", test_threads_bounce_work_back_and_forth);
        run_case("re-entrancy : the waiting thread runs earlier tasks first", test_wait_runs_earlier_tasks_first);
        run_case("re-entrancy : a timed wait runs work handed back", test_timed_wait_runs_handed_back_work);
        run_case("re-entrancy : on-process work waits for process()", test_on_process_work_waits_for_process);
        for(const auto worker_count : test_support::get_worker_counts())
        {
            const auto run = [worker_count](const std::string& text, void (*test)(size_t))
            {
                run_case(name_for(worker_count, "re-entrancy : " + text),
                         [worker_count, test]()
                         {
                             test(worker_count);
                         });
            };
            run("a job waits on a continuation on the waiting thread", test_job_waits_on_continuation_on_waiter);
            run("main and jobs bounce work back and forth, nested", test_main_and_jobs_bounce_work_back_and_forth);
            run("many jobs hand work back while main waits on each",
                test_jobs_hand_work_back_while_main_waits_on_all);
        }
    }
}
} // namespace reentrancy_tests
