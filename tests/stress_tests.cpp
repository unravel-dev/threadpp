#include "stress_tests.h"
#include "test_support.h"

#include <suitepp/suite.hpp>
#include <threadpp/thread_pool.h>
#include <threadpp/when_all_any.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <thread>
#include <vector>

//-----------------------------------------------------------------------------
/// Many threads scheduling, waiting, raising, stopping and dropping at once:
/// the cases a race detector (THREADPP_SANITIZE=thread) is for.
//-----------------------------------------------------------------------------
namespace stress_tests
{
using namespace std::chrono_literals;
using test_support::name_for;
using test_support::run_case;

/// A small deterministic random sequence, the same on every platform.
class sequence
{
public:
    explicit sequence(std::uint32_t seed) : state_(seed * 2654435761u + 1u)
    {
    }

    auto next(std::uint32_t bound) -> std::uint32_t
    {
        state_ = state_ * 1664525u + 1013904223u;
        return (state_ >> 8) % bound;
    }

private:
    std::uint32_t state_;
};

auto make_group(sequence& random) -> tpp::priority::group
{
    const auto level = static_cast<tpp::priority::category>(random.next(4));
    return {level, random.next(4)};
}

void test_many_producers(size_t worker_count)
{
    constexpr static int producer_count = 8;
    constexpr static int jobs_per_producer = 300;
    constexpr static int child_every = 7;
    tpp::thread_pool pool({{tpp::priority::category::low, worker_count}});
    std::atomic<long long> total{0};
    std::vector<std::thread> producers;
    for(int p = 0; p < producer_count; ++p)
    {
        // Half the producers are registered with tpp, half are not.
        const bool is_registered = p % 2 == 0;
        producers.emplace_back(
            [&pool, &total, p, is_registered]()
            {
                if(is_registered)
                {
                    tpp::this_thread::register_this_thread();
                }
                sequence random(static_cast<std::uint32_t>(p + 1));
                std::vector<tpp::job_future<int>> jobs;
                jobs.reserve(jobs_per_producer);
                for(int i = 0; i < jobs_per_producer; ++i)
                {
                    const bool has_child = i % child_every == 0;
                    jobs.push_back(pool.schedule(make_group(random),
                                                 [&pool, i, has_child]()
                                                 {
                                                     if(!has_child)
                                                     {
                                                         return i;
                                                     }
                                                     return pool
                                                         .schedule(
                                                             [i]()
                                                             {
                                                                 return i;
                                                             })
                                                         .get();
                                                 }));
                }
                long long sum = 0;
                for(auto& job : jobs)
                {
                    sum += job.get();
                }
                total += sum;
                if(is_registered)
                {
                    tpp::this_thread::unregister_this_thread();
                }
            });
    }
    for(auto& producer : producers)
    {
        producer.join();
    }
    const long long per_producer = static_cast<long long>(jobs_per_producer) * (jobs_per_producer - 1) / 2;
    EXPECT(total.load() == per_producer * producer_count);
    // A finished job leaves the count just after its future is ready.
    EXPECT(test_support::poll_until(
        [&pool]()
        {
            return pool.get_jobs_count() == 0u;
        }));
}

void test_shared_job_many_waiters(size_t worker_count)
{
    constexpr static int waiter_count = 8;
    tpp::thread_pool pool({{tpp::priority::category::low, worker_count}});
    std::atomic<int> runs{0};
    auto job = pool.schedule(
                       [&runs]()
                       {
                           ++runs;
                           std::this_thread::sleep_for(1ms);
                           return 5;
                       })
                   .share();
    std::atomic<int> sum{0};
    std::vector<std::thread> waiters;
    for(int i = 0; i < waiter_count; ++i)
    {
        waiters.emplace_back(
            [job, &sum, i]()
            {
                const bool is_registered = i % 2 == 0;
                if(is_registered)
                {
                    tpp::this_thread::register_this_thread();
                }
                sum += job.get();
                if(is_registered)
                {
                    tpp::this_thread::unregister_this_thread();
                }
            });
    }
    sum += job.get();
    for(auto& waiter : waiters)
    {
        waiter.join();
    }
    EXPECT(sum.load() == 5 * (waiter_count + 1));
    EXPECT(runs.load() == 1);
}

void test_mixed_graph(size_t worker_count)
{
    constexpr static int job_count = 200;
    tpp::thread_pool pool({{tpp::priority::category::low, worker_count}});
    const auto main_id = tpp::this_thread::get_id();
    sequence random(static_cast<std::uint32_t>(worker_count + 11));
    std::vector<tpp::future<int>> results;
    std::vector<bool> is_stoppable;
    tpp::shared_future<int> previous = tpp::make_ready_future(0).share();
    for(int i = 0; i < job_count; ++i)
    {
        auto job = pool.schedule(make_group(random),
                                 [i]()
                                 {
                                     return i;
                                 });
        const auto choice = random.next(5);
        if(choice == 0)
        {
            job.change_priority(tpp::priority::critical());
        }
        if(choice == 1)
        {
            job.stop();
            results.push_back(std::move(job));
            is_stoppable.push_back(true);
            continue;
        }
        if(choice == 2)
        {
            results.push_back(job.then(main_id,
                                       [](tpp::future<int> parent)
                                       {
                                           return parent.get();
                                       }));
            is_stoppable.push_back(false);
            continue;
        }
        if(choice == 3)
        {
            tpp::shared_future<int> shared = job.share();
            auto both = tpp::when_all(shared, previous);
            previous = shared;
            results.push_back(both.then(main_id,
                                        [](auto parent)
                                        {
                                            return std::get<0>(parent.get()).get();
                                        }));
            is_stoppable.push_back(false);
            continue;
        }
        results.push_back(std::move(job));
        is_stoppable.push_back(false);
    }
    bool is_consistent = true;
    for(size_t i = 0; i < results.size(); ++i)
    {
        try
        {
            is_consistent = is_consistent && results[i].get() == static_cast<int>(i);
        }
        catch(const std::future_error&)
        {
            is_consistent = is_consistent && is_stoppable[i];
        }
    }
    EXPECT(is_consistent);
}

void test_pool_churn(size_t worker_count)
{
    constexpr static int pool_count = 30;
    constexpr static int jobs_per_pool = 20;
    std::atomic<int> runs{0};
    std::vector<tpp::job_future<void>> orphans;
    for(int p = 0; p < pool_count; ++p)
    {
        tpp::thread_pool pool({{tpp::priority::category::low, worker_count}});
        for(int i = 0; i < jobs_per_pool; ++i)
        {
            auto job = pool.schedule(
                [&runs]()
                {
                    ++runs;
                });
            if(i % 5 == 0)
            {
                orphans.push_back(std::move(job));
            }
        }
        orphans.push_back(pool.create_job(
            [&runs]()
            {
                ++runs;
            }));
    }
    int broken = 0;
    for(auto& orphan : orphans)
    {
        try
        {
            orphan.get();
        }
        catch(const std::future_error&)
        {
            ++broken;
        }
    }
    EXPECT(broken >= pool_count);
    EXPECT(runs.load() <= pool_count * jobs_per_pool);
}

void test_concurrent_priority_changes(size_t worker_count)
{
    constexpr static int job_count = 400;
    constexpr static int changer_count = 4;
    tpp::thread_pool pool({{tpp::priority::category::low, worker_count}});
    std::vector<tpp::job_shared_future<int>> jobs;
    for(int i = 0; i < job_count; ++i)
    {
        jobs.push_back(pool.schedule(
                               [i]()
                               {
                                   return i % 3;
                               })
                           .share());
    }
    std::vector<std::thread> changers;
    for(int c = 0; c < changer_count; ++c)
    {
        changers.emplace_back(
            [jobs, c]() mutable
            {
                sequence random(static_cast<std::uint32_t>(c + 100));
                for(auto& job : jobs)
                {
                    job.change_priority(make_group(random));
                }
            });
    }
    long long sum = 0;
    for(auto& job : jobs)
    {
        sum += job.get();
    }
    for(auto& changer : changers)
    {
        changer.join();
    }
    long long expected = 0;
    for(int i = 0; i < job_count; ++i)
    {
        expected += i % 3;
    }
    EXPECT(sum == expected);
    EXPECT(pool.run_pending(1s) == 0u);
}

void run_tests()
{
    const auto& options = test_support::get_options();
    for(int i = 0; i < options.iterations; ++i)
    {
        for(const auto worker_count : test_support::get_worker_counts())
        {
            const auto run = [worker_count](const std::string& text, void (*test)(size_t))
            {
                run_case(name_for(worker_count, text),
                         [worker_count, test]()
                         {
                             test(worker_count);
                         },
                         std::chrono::milliseconds(60000));
            };
            run("stress : many producers schedule and wait", test_many_producers);
            run("stress : many threads wait on one shared job", test_shared_job_many_waiters);
            run("stress : a graph of raised, stopped, continued and joined jobs", test_mixed_graph);
            run("stress : pools come and go with jobs left", test_pool_churn);
            run("stress : priorities change from many threads", test_concurrent_priority_changes);
        }
    }
}
} // namespace stress_tests
