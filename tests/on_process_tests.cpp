#include "on_process_tests.h"

#include <suitepp/suite.hpp>
#include <threadpp/future.hpp>
#include <threadpp/thread.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <vector>

// The contract of the on-process tasks (invoke_on_process, dispatch_on_process,
// async_on_process, then_on_process): only the target thread's own process()
// called from the top of its stack runs them - never a blocking call, which
// runs the thread's other tasks wherever it blocks, and never a process()
// called from inside a task.
namespace on_process_tests
{
namespace
{
constexpr std::chrono::milliseconds wait_duration{20};
constexpr std::chrono::milliseconds poll_deadline{2000};
constexpr int repost_process_calls = 3;
constexpr int async_value = 42;
constexpr int promise_value = 7;
// a plain task arrives this late to end a nested process_and_wait
constexpr std::chrono::milliseconds waker_delay{40};
// that wait has to last at least this long: it did not return on the
// on-process task it cannot run
constexpr std::chrono::milliseconds min_nested_wait{20};

// polls the predicate between std sleeps, which run none of this thread's
// tasks, until it holds or the deadline passes
template<typename Predicate>
bool poll_until(Predicate predicate)
{
	const auto deadline = std::chrono::steady_clock::now() + poll_deadline;
	while(!predicate())
	{
		if(std::chrono::steady_clock::now() > deadline)
		{
			return false;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
	return true;
}
} // namespace

void run_tests()
{
	const auto main_id = tpp::main_thread::get_id();

	TEST_CASE("on-process : blocking calls never run them, process() does")
	{
		bool on_process_ran = false;
		bool regular_ran = false;
		tpp::invoke_on_process(main_id, [&]() { on_process_ran = true; });
		tpp::invoke(main_id, [&]() { regular_ran = true; });
		tpp::this_thread::wait_for(wait_duration);
		tpp::this_thread::sleep_for(wait_duration);
		EXPECT(regular_ran);
		EXPECT(!on_process_ran);

		auto worker = tpp::make_thread("on-process worker");
		auto slow = tpp::async(worker.get_id(),
							   []()
							   {
								   std::this_thread::sleep_for(wait_duration);
								   return true;
							   });
		slow.wait();
		EXPECT(!on_process_ran);

		tpp::this_thread::process();
		EXPECT(on_process_ran);
	};

	TEST_CASE("on-process : a task queued while a batch runs waits for the next process()")
	{
		int runs = 0;
		bool keep_reposting = true;
		std::function<void()> repost;
		repost = [&]()
		{
			++runs;
			if(keep_reposting)
			{
				tpp::invoke_on_process(main_id, repost);
			}
		};
		tpp::invoke_on_process(main_id, repost);
		for(int call = 1; call <= repost_process_calls; ++call)
		{
			tpp::this_thread::process();
			EXPECT(runs == call);
		}
		// the copy still queued runs now and queues nothing, so no task
		// outlives the locals it references
		keep_reposting = false;
		tpp::this_thread::process();
	};

	TEST_CASE("on-process : a process() called from inside a task runs none")
	{
		std::vector<std::string> order;
		tpp::invoke_on_process(main_id,
							   [&]()
							   {
								   order.emplace_back("first begin");
								   tpp::invoke_on_process(main_id, [&]() { order.emplace_back("queued in batch"); });
								   tpp::this_thread::process();
								   order.emplace_back("first end");
							   });
		tpp::invoke_on_process(main_id, [&]() { order.emplace_back("second"); });
		tpp::this_thread::process();
		const std::vector<std::string> first_call = {"first begin", "first end", "second"};
		EXPECT(order == first_call);

		tpp::this_thread::process();
		const std::vector<std::string> second_call = {"first begin", "first end", "second", "queued in batch"};
		EXPECT(order == second_call);
	};

	TEST_CASE("on-process : dispatch runs directly only from an on-process task at its own level")
	{
		bool outside_ran = false;
		tpp::dispatch_on_process(main_id, [&]() { outside_ran = true; });
		EXPECT(!outside_ran);

		bool inline_ran = false;
		bool inline_ran_at_call = false;
		bool nested_ran = false;
		bool nested_ran_at_call = true;
		tpp::invoke_on_process(main_id,
							   [&]()
							   {
								   tpp::dispatch_on_process(main_id, [&]() { inline_ran = true; });
								   inline_ran_at_call = inline_ran;
								   // a plain task that a blocking call of this task runs
								   // is not at the on-process level
								   tpp::invoke(main_id,
											   [&]()
											   {
												   tpp::dispatch_on_process(main_id, [&]() { nested_ran = true; });
												   nested_ran_at_call = nested_ran;
											   });
								   tpp::this_thread::wait_for(wait_duration);
							   });
		tpp::this_thread::process();
		EXPECT(outside_ran);
		EXPECT(inline_ran_at_call);
		EXPECT(!nested_ran_at_call);

		tpp::this_thread::process();
		EXPECT(nested_ran);
	};

	TEST_CASE("on-process : async_on_process")
	{
		auto queued = tpp::async_on_process(main_id, []() { return async_value; });
		tpp::this_thread::wait_for(wait_duration);
		EXPECT(!queued.is_ready());

		tpp::this_thread::process();
		EXPECT(queued.is_ready());
		EXPECT(queued.get() == async_value);

		bool inner_ready = false;
		tpp::invoke_on_process(main_id,
							   [&]()
							   {
								   auto inner = tpp::async_on_process(main_id, []() { return async_value; });
								   inner_ready = inner.is_ready();
							   });
		tpp::this_thread::process();
		EXPECT(inner_ready);

		std::atomic<int> helper_result{0};
		std::thread helper(
			[&]()
			{
				tpp::this_thread::register_this_thread("on-process helper");
				auto from_helper = tpp::async_on_process(main_id, []() { return async_value; });
				helper_result = from_helper.get();
				tpp::this_thread::unregister_this_thread();
			});
		const bool helper_done = poll_until(
			[&]()
			{
				tpp::this_thread::process();
				return helper_result.load() == async_value;
			});
		helper.join();
		EXPECT(helper_done);
	};

	TEST_CASE("on-process : then_on_process")
	{
		tpp::promise<int> promise;
		bool continuation_ran = false;
		auto continued = promise.get_future().then_on_process(main_id,
															  [&](tpp::future<int> value)
															  {
																  continuation_ran = true;
																  return value.get() + 1;
															  });
		std::thread setter([&]() { promise.set_value(promise_value); });
		setter.join();
		tpp::this_thread::wait_for(wait_duration);
		EXPECT(!continuation_ran);

		tpp::this_thread::process();
		EXPECT(continuation_ran);
		EXPECT(continued.is_ready());
		EXPECT(continued.get() == promise_value + 1);
	};

	TEST_CASE("on-process : a make_thread thread runs them in its idle loop")
	{
		auto worker = tpp::make_thread("on-process idle worker");
		std::atomic<bool> ran{false};
		tpp::invoke_on_process(worker.get_id(), [&]() { ran = true; });
		EXPECT(poll_until([&]() { return ran.load(); }));
	};

	TEST_CASE("on-process : an external thread loop on process_and_wait runs them")
	{
		std::atomic<tpp::thread::id> loop_id{tpp::invalid_id()};
		std::atomic<bool> queued_first_ran{false};
		std::atomic<bool> regular_ran{false};
		std::atomic<bool> on_process_ran{false};
		std::thread loop(
			[&]()
			{
				tpp::this_thread::register_this_thread("process_and_wait loop");
				// queued before the loop first blocks: it must not be missed
				tpp::invoke_on_process(tpp::this_thread::get_id(), [&]() { queued_first_ran = true; });
				loop_id = tpp::this_thread::get_id();
				while(!tpp::this_thread::notified_for_exit())
				{
					tpp::this_thread::process_and_wait();
				}
				tpp::this_thread::unregister_this_thread();
			});
		EXPECT(poll_until([&]() { return loop_id.load() != tpp::invalid_id(); }));
		tpp::invoke(loop_id.load(), [&]() { regular_ran = true; });
		tpp::invoke_on_process(loop_id.load(), [&]() { on_process_ran = true; });
		EXPECT(poll_until([&]() { return queued_first_ran.load(); }));
		EXPECT(poll_until([&]() { return regular_ran.load(); }));
		EXPECT(poll_until([&]() { return on_process_ran.load(); }));
		tpp::notify_for_exit(loop_id.load());
		loop.join();
	};

	TEST_CASE("on-process : process_and_wait from inside a task neither runs them nor spins on them")
	{
		auto waker = tpp::make_thread("on-process waker");
		bool on_process_ran = false;
		bool ran_inside = true;
		std::chrono::steady_clock::duration nested_wait{};
		tpp::invoke(main_id,
					[&]()
					{
						tpp::invoke_on_process(main_id, [&]() { on_process_ran = true; });
						tpp::invoke(waker.get_id(),
									[main_id]()
									{
										std::this_thread::sleep_for(waker_delay);
										tpp::invoke(main_id, []() {});
									});
						const auto start = std::chrono::steady_clock::now();
						tpp::this_thread::process_and_wait();
						nested_wait = std::chrono::steady_clock::now() - start;
						ran_inside = on_process_ran;
					});
		tpp::this_thread::process();
		EXPECT(!ran_inside);
		EXPECT(nested_wait >= min_nested_wait);
		EXPECT(on_process_ran);
	};

	TEST_CASE("on-process : the pending task count includes them")
	{
		// a registered thread that never processes, so nothing else changes
		// its count
		std::atomic<bool> release{false};
		std::atomic<tpp::thread::id> idle_id{tpp::invalid_id()};
		std::thread idle(
			[&]()
			{
				tpp::this_thread::register_this_thread("on-process idle thread");
				idle_id = tpp::this_thread::get_id();
				while(!release)
				{
					std::this_thread::sleep_for(std::chrono::milliseconds(1));
				}
				tpp::this_thread::unregister_this_thread();
			});
		const bool registered = poll_until([&]() { return idle_id.load() != tpp::invalid_id(); });
		EXPECT(registered);
		if(registered)
		{
			tpp::invoke_on_process(idle_id.load(), []() {});
			EXPECT(tpp::get_pending_task_count(idle_id.load()) == 1);
		}
		release = true;
		idle.join();
	};
}
} // namespace on_process_tests
