#include "thread_pool_tests.h"
#include "utils.hpp"

#include <threadpp/thread_pool.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <vector>

namespace thread_pool_tests
{
using namespace std::chrono_literals;

void expect_count(const char* label, int actual, int expected)
{
	if(actual != expected)
	{
		sout() << label << " lost jobs: " << actual << " / " << expected;
		std::abort();
	}
}

void run_burst_tests()
{
	constexpr int job_count = 1000;
	tpp::thread_pool pool({{tpp::priority::category::normal, 4}});
	std::atomic<int> completed{0};
	for(int i = 0; i < job_count; ++i)
	{
		pool.schedule([&completed]() { completed.fetch_add(1, std::memory_order_relaxed); });
	}
	pool.wait_all();
	expect_count("burst schedule", completed.load(), job_count);

	for(int i = 0; i < job_count; ++i)
	{
		pool.schedule([&completed]() { completed.fetch_add(1, std::memory_order_relaxed); });
	}
	pool.wait_all();
	expect_count("burst idle wave", completed.load(), job_count * 2);

	std::atomic<int> submitted_completed{0};
	std::vector<tpp::job_future<void>> deferred;
	deferred.reserve(static_cast<size_t>(job_count));
	for(int i = 0; i < job_count; ++i)
	{
		deferred.emplace_back(pool.create_job([&submitted_completed]() {
			submitted_completed.fetch_add(1, std::memory_order_relaxed);
		}));
	}
	for(auto& job : deferred)
	{
		job.submit();
	}
	pool.wait_all();
	expect_count("burst submit", submitted_completed.load(), job_count);
}

void run_tests(int iterations)
{
	auto now = tpp::clock::now();

	tpp::thread_pool pool({{tpp::priority::category::normal, 2},
						   {tpp::priority::category::high, 1},
						   {tpp::priority::category::critical, 1}});

	for(int i = 0; i < iterations; ++i)
	{
		for(size_t j = 0; j < 5; ++j)
		{
			// clang-format off
            auto job = pool.schedule(tpp::priority::normal(j), [i, j]()
            {
                std::this_thread::sleep_for(10ms);
                sout() << "call normal priority job " << i << " variant : " << j;
            });
//            pool.change_priority(job.id, tpp::priority::critical());

//            //job is just a normal shared_future and we can use it like any other
//            job.then(tpp::this_thread::get_id(), [](auto parent)
//            {
//                sout() << "job is done\n";
//            });
//            pool.wait(job.id);
//            pool.stop(job.id);

			// clang-format on
		}

		for(size_t j = 0; j < 5; ++j)
		{
			// clang-format off
            pool.schedule(tpp::priority::high(j), [i, j]()
            {
                std::this_thread::sleep_for(10ms);
                sout() << "call high priority job " << i << " variant : " << j;
            });
			// clang-format on
		}

		for(size_t j = 0; j < 5; ++j)
		{
			// clang-format off
            pool.schedule(tpp::priority::critical(j), [i, j]()
            {
                std::this_thread::sleep_for(10ms);
                sout() << "call critical priority job " << i << " variant : " << j;
            });
			// clang-format on
		}
	}

	// pool.stop_all();
	pool.wait_all();
	run_burst_tests();

	auto end = tpp::clock::now();
	auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - now);
	sout() << dur.count() << "ms\n";
}
} // namespace thread_pool_tests
