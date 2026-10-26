#include "thread_pool_tests.h"
#include "utils.hpp"

#include <suitepp/suite.hpp>
#include <threadpp/thread_pool.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
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

// A job that holds its worker until it is released, so a test can look at
// the pool while the job is running.
struct held_job
{
	std::atomic<bool> is_started{false};
	std::atomic<bool> is_released{false};

	void run()
	{
		is_started = true;
		while(!is_released)
		{
			std::this_thread::sleep_for(1ms);
		}
	}
};

// spins between std sleeps until the predicate holds or two seconds pass
template<typename Predicate>
bool poll_until(Predicate predicate)
{
	const auto deadline = std::chrono::steady_clock::now() + 2s;
	while(!predicate())
	{
		if(std::chrono::steady_clock::now() > deadline)
		{
			return false;
		}
		std::this_thread::sleep_for(1ms);
	}
	return true;
}

void run_monitoring_tests()
{
	TEST_CASE("thread pool : a running job is reported with its name, detail and time")
	{
		tpp::thread_pool pool({{tpp::priority::category::normal, 2}});
		EXPECT(pool.get_workers_count() == 2u);
		EXPECT(pool.get_running_jobs().empty());

		held_job held;
		auto job = pool.schedule(tpp::job_desc{"Compiling Texture", "textures/rock.png"}, [&held]() { held.run(); });
		EXPECT(poll_until([&held]() { return held.is_started.load(); }));
		// Timed on the clock the pool reads, not with one sleep: a sleep can end early by part of
		// a timer tick. The job started before it said so, so it has run at least this long.
		const auto seen_running = std::chrono::steady_clock::now();
		while(std::chrono::steady_clock::now() - seen_running < 20ms)
		{
			std::this_thread::sleep_for(1ms);
		}

		const auto running = pool.get_running_jobs();
		EXPECT(running.size() == 1u);
		if(!running.empty())
		{
			const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(running.front().elapsed).count();
			EXPECT(running.front().name == "Compiling Texture");
			EXPECT(running.front().detail == "textures/rock.png");
			EXPECT(elapsed_ms >= 20);
		}

		held.is_released = true;
		job.wait();
		EXPECT(poll_until([&pool]() { return pool.get_running_jobs().empty(); }));
		EXPECT(pool.get_jobs_count() == 0u);
	};

	TEST_CASE("thread pool : the longest running job comes first")
	{
		tpp::thread_pool pool({{tpp::priority::category::normal, 2}});
		held_job first;
		held_job second;
		auto first_job = pool.schedule(tpp::job_desc{"Job", "first"}, [&first]() { first.run(); });
		EXPECT(poll_until([&first]() { return first.is_started.load(); }));
		std::this_thread::sleep_for(10ms);
		auto second_job = pool.schedule(tpp::job_desc{"Job", "second"}, [&second]() { second.run(); });
		EXPECT(poll_until([&second]() { return second.is_started.load(); }));

		const auto running = pool.get_running_jobs();
		EXPECT(running.size() == 2u);
		if(running.size() == 2u)
		{
			EXPECT(running[0].detail == "first");
			EXPECT(running[1].detail == "second");
		}

		first.is_released = true;
		second.is_released = true;
		pool.wait_all();
	};

	TEST_CASE("thread pool : the detail does not split the count by name")
	{
		tpp::thread_pool pool({{tpp::priority::category::normal, 1}});
		held_job held;
		auto blocker = pool.schedule("Blocker", [&held]() { held.run(); });
		EXPECT(poll_until([&held]() { return held.is_started.load(); }));
		auto first = pool.schedule(tpp::job_desc{"Compiling Mesh", "a.gltf"}, []() {});
		auto second = pool.schedule(tpp::job_desc{"Compiling Mesh", "b.gltf"}, tpp::priority::normal(), []() {});

		auto counts = pool.get_jobs_count_detailed();
		EXPECT(counts.size() == 2u);
		EXPECT(counts["Compiling Mesh"] == 2u);
		EXPECT(counts["Blocker"] == 1u);

		held.is_released = true;
		pool.wait_all();
	};

	TEST_CASE("thread pool : wait_all_polling counts the finished jobs")
	{
		tpp::thread_pool pool({{tpp::priority::category::normal, 2}});
		constexpr size_t job_count = 12;
		held_job held;
		// Scheduled first and released last: whichever job the wait starts on, the
		// count has to move while this one still runs.
		pool.schedule(tpp::job_desc{"Slow", "held"}, [&held]() { held.run(); });
		for(size_t i = 0; i < job_count - 1; ++i)
		{
			pool.schedule(tpp::job_desc{"Quick", std::to_string(i)}, []() { std::this_thread::sleep_for(2ms); });
		}
		std::thread releaser([&held]() {
			std::this_thread::sleep_for(150ms);
			held.is_released = true;
		});

		std::vector<size_t> reported;
		size_t reported_total = 0;
		bool has_named_job = true;
		pool.wait_all_polling(tpp::priority::category::normal,
							  [&](const tpp::thread_pool::progress_info& info)
							  {
								  reported.push_back(info.current_job);
								  reported_total = info.total_jobs;
								  has_named_job = has_named_job && !info.name.empty();
							  });
		releaser.join();

		EXPECT(reported_total == job_count);
		EXPECT(has_named_job);
		EXPECT(std::is_sorted(reported.begin(), reported.end()));
		// The quick jobs are done long before the held one is let go.
		EXPECT(!reported.empty());
		if(!reported.empty())
		{
			EXPECT(reported.back() >= job_count - 1);
		}
		EXPECT(poll_until([&pool]() { return pool.get_jobs_count() == 0; }));
	};
}

void run_shared_future_tests()
{
	TEST_CASE("thread pool : a deferred job submitted from many threads through one shared future runs once")
	{
		constexpr int thread_count = 16;
		constexpr int rounds = 200;
		tpp::thread_pool pool({{tpp::priority::category::normal, 4}});
		bool is_every_job_run_once = true;
		bool is_every_job_submitted = true;
		for(int round = 0; round < rounds; ++round)
		{
			std::atomic<int> runs{0};
			auto job = pool.create_job([&runs]() {
				runs.fetch_add(1, std::memory_order_relaxed);
				return 7;
			}).share();
			std::atomic<bool> go{false};
			std::vector<std::thread> threads;
			for(int t = 0; t < thread_count; ++t)
			{
				threads.emplace_back([&]() {
					while(!go.load(std::memory_order_acquire))
					{
					}
					job.submit();
				});
			}
			go.store(true, std::memory_order_release);
			for(auto& thread : threads)
			{
				thread.join();
			}
			is_every_job_submitted = is_every_job_submitted && job.is_submitted();
			is_every_job_run_once = is_every_job_run_once && job.get() == 7;
			pool.wait_all();
			is_every_job_run_once = is_every_job_run_once && runs.load() == 1;
		}
		EXPECT(is_every_job_submitted);
		EXPECT(is_every_job_run_once);
	};

	TEST_CASE("thread pool : many threads reading a ready shared future get its one stored value")
	{
		constexpr int thread_count = 16;
		constexpr int reads = 20000;
		tpp::thread_pool pool({{tpp::priority::category::normal, 2}});
		auto job = pool.schedule([]() { return std::string("a value longer than the small string buffer"); }).share();
		job.wait();
		const std::string* stored = &job.get();
		std::atomic<int> mismatches{0};
		std::vector<std::thread> threads;
		for(int t = 0; t < thread_count; ++t)
		{
			threads.emplace_back([&]() {
				for(int i = 0; i < reads; ++i)
				{
					const auto& value = job.get();
					if(&value != stored || value != "a value longer than the small string buffer")
					{
						mismatches.fetch_add(1, std::memory_order_relaxed);
					}
				}
			});
		}
		for(auto& thread : threads)
		{
			thread.join();
		}
		EXPECT(mismatches.load() == 0);
	};
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
	run_monitoring_tests();
	run_shared_future_tests();

	auto end = tpp::clock::now();
	auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(end - now);
	sout() << dur.count() << "ms\n";
}
} // namespace thread_pool_tests
