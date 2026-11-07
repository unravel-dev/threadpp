#include "benchmark_tests.h"
#include "test_support.h"

#include <threadpp/thread_pool.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <limits>
#include <map>
#include <string>
#include <vector>

namespace benchmark_tests
{
namespace
{
constexpr int repeat_count = 5;

/// The best of a few runs, in milliseconds.
auto measure(const std::function<void()>& body) -> double
{
    double best = std::numeric_limits<double>::max();
    for(int i = 0; i < repeat_count; ++i)
    {
        const auto start = std::chrono::steady_clock::now();
        body();
        const auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start);
        best = std::min(best, elapsed.count());
    }
    return best;
}

void report(const std::string& name, double milliseconds, int operations)
{
    std::printf("[bench] %-52s %9.2f ms  %8.3f us/op\n",
                name.c_str(),
                milliseconds,
                milliseconds * 1000.0 / static_cast<double>(operations));
}

auto make_workers(size_t worker_count) -> std::map<tpp::priority::category, size_t>
{
    return {{tpp::priority::category::low, worker_count}};
}

void bench_burst(size_t worker_count)
{
    constexpr int job_count = 100000;
    tpp::thread_pool pool(make_workers(worker_count));
    std::atomic<int> runs{0};
    const auto milliseconds = measure(
        [&]()
        {
            for(int i = 0; i < job_count; ++i)
            {
                pool.schedule(
                    [&runs]()
                    {
                        runs.fetch_add(1, std::memory_order_relaxed);
                    });
            }
            if(worker_count == 0)
            {
                pool.run_pending(std::chrono::hours(1));
            }
            pool.wait_all();
        });
    report("burst of empty jobs + wait_all, " + std::to_string(worker_count) + " workers", milliseconds, job_count);
}

void bench_round_trip(size_t worker_count)
{
    constexpr int trip_count = 20000;
    tpp::thread_pool pool(make_workers(worker_count));
    const auto milliseconds = measure(
        [&]()
        {
            for(int i = 0; i < trip_count; ++i)
            {
                pool.schedule(
                        [i]()
                        {
                            return i;
                        })
                    .get();
            }
        });
    report("schedule + get, one at a time, " + std::to_string(worker_count) + " workers", milliseconds, trip_count);
}

void bench_fork_join(size_t worker_count)
{
    constexpr int round_count = 2000;
    constexpr int fanout = 16;
    tpp::thread_pool pool(make_workers(worker_count));
    const auto milliseconds = measure(
        [&]()
        {
            for(int round = 0; round < round_count; ++round)
            {
                std::vector<tpp::job_future<int>> jobs;
                jobs.reserve(fanout);
                for(int i = 0; i < fanout; ++i)
                {
                    jobs.push_back(pool.schedule(
                        [i]()
                        {
                            volatile int sum = 0;
                            for(int k = 0; k < 200; ++k)
                            {
                                sum = sum + k * i;
                            }
                            return static_cast<int>(sum);
                        }));
                }
                for(auto& job : jobs)
                {
                    job.get();
                }
            }
        });
    report("fork-join 16 small jobs, " + std::to_string(worker_count) + " workers",
           milliseconds,
           round_count * fanout);
}

void bench_mailbox()
{
    constexpr int task_count = 200000;
    auto worker = tpp::make_thread("bench mailbox");
    std::atomic<int> runs{0};
    const auto milliseconds = measure(
        [&]()
        {
            for(int i = 0; i < task_count; ++i)
            {
                tpp::invoke(worker.get_id(),
                            [&runs]()
                            {
                                runs.fetch_add(1, std::memory_order_relaxed);
                            });
            }
            tpp::async(worker.get_id(), []() {}).wait();
        });
    report("invoke to a thread + one async wait", milliseconds, task_count);
}
} // namespace

void run_benchmarks()
{
    bench_mailbox();
    for(const size_t worker_count : {size_t(0), size_t(1), size_t(8)})
    {
        bench_burst(worker_count);
        bench_round_trip(worker_count);
        bench_fork_join(worker_count);
    }
}
} // namespace benchmark_tests
