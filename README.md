# threadpp - thread communication
![windows](https://github.com/unravel-dev/threadpp/actions/workflows/windows.yml/badge.svg)
![linux](https://github.com/unravel-dev/threadpp/actions/workflows/linux.yml/badge.svg)
![macos](https://github.com/unravel-dev/threadpp/actions/workflows/macos.yml/badge.svg)
![sanitizers](https://github.com/unravel-dev/threadpp/actions/workflows/sanitizers.yml/badge.svg)

C++14 library providing easy interface for thread communication in a single process.
It has no dependencies except the standard library.

The whole idea behind it is that all the blocking calls like future::wait or this_thread::sleep_for
can actually process different tasks if someone invokes into that thread while it is waiting on something.
Tasks posted with invoke_on_process, dispatch_on_process, async_on_process or then_on_process are the
exception: only the target thread's own this_thread::process() / process_for(), called from the top of its
stack, runs them - never a blocking call, and never a process() called from inside a task. Use them for work
that must not run in the middle of whatever the thread is doing when it blocks, such as changing state that
the code around the blocking call holds references into. Tasks posted while a batch of them runs wait for the
next process() call. A thread loop built on this_thread::process_and_wait() runs them between waits, as threads
made with make_thread do; a loop on this_thread::wait() never does.
It provides abstractions like condition_variable, promise, future, shared_future, future continuations, async
which are standard conforming. 
It provides a very easy interface to attach any std thread to it and start invoking into it.

## Scheduling: the same behavior with any number of threads

A thread_pool may have any number of workers, none included (a program that cannot start threads, such as a
browser build). Every blocking call - future wait/get/wait_for, thread_pool::wait/wait_all, sleep_for,
condition_variable, mutex - runs one loop, so a program behaves the same whatever the count:

- Liveness: a wait returns once queued work can produce its result. A wait follows a future to the jobs that
  produce it - its own job, a continuation's source, a when_all/when_any's inputs - and runs one itself when no
  worker is free to.
- Placement: a mailbox task runs only on its thread. A pool job runs on a worker, or on a thread that waits for
  it (or for something made from it) when no worker is free. A timed wait of a thread outside a pool with
  workers runs none of its jobs, so it returns near its deadline.
- Order: jobs start highest category first, then highest priority, then first scheduled first. A waited job may
  start ahead of that order.
- Re-entrancy: any blocking call may run the waiting thread's own tasks and the jobs its result depends on; never
  its on-process tasks. A worker, or the thread that hosts a pool without workers (the one that created it),
  waiting for something whose producer is unknown (a promise, a condition variable, a sleep) also runs other
  queued jobs when nothing else would: those its own job scheduled first, then any (the host says so in the log
  once).
- Threads need no registration to wait; registration is what gives a thread a mailbox.

Two things only threads can give: a job that needs a job suspended further down the same stack cannot finish
on one thread (a wait for a job running further up the same thread is reported in the log), and a lock held
across a wait may be wanted by a job the wait runs. Never hold a lock across a wait.

async(f) and then(f) without a thread id start a thread each; set_detached_executor hands them to something else
instead, e.g. a pool without workers.

## Tests and sanitizers

```
threadpp_test [--iterations N] [--filter TEXT] [--timeout-scale X] [--bench]
```

The scheduling, re-entrancy, sync and stress cases run at 0, 1, 2 and 8 workers under a watchdog that fails the run with the
case's name when a wait never returns. THREADPP_TEST_ITERATIONS and THREADPP_TEST_TIMEOUT_SCALE set the defaults.
With a filter only the cases whose name contains it run. --bench prints timings to compare builds instead.

THREADPP_SANITIZE builds the library and tests with sanitizers: `thread` (ThreadSanitizer, Linux and macOS
clang/gcc) or `address;undefined`. The presets do it:

```
cmake --preset linux-clang-tsan && cmake --build --preset linux-clang-tsan && ctest --preset linux-clang-tsan
cmake --preset linux-clang-asan && cmake --build --preset linux-clang-asan && ctest --preset linux-clang-asan
```

macos-arm64-tsan does the same on macOS. On Ubuntu 24.04 run `sudo sysctl vm.mmap_rnd_bits=28` first, as the
sanitizers workflow does.

```c++
std::thread make_std_thread()
{
	std::thread th([]() {
		tpp::this_thread::register_this_thread();

		while(!tpp::this_thread::notified_for_exit())
		{
			tpp::this_thread::wait();
		}

		tpp::this_thread::unregister_this_thread();
	});
	tpp::register_thread(th.get_id());
	return th;
}
tpp::thread::id make_detached_std_thread()
{
	std::thread th([]() {
		tpp::this_thread::register_this_thread();

		while(!tpp::this_thread::notified_for_exit())
		{
			tpp::this_thread::wait();
		}

		tpp::this_thread::unregister_this_thread();
	});
	auto id = tpp::register_thread(th.get_id());
	th.detach();

	return id;
}

itc::thread::id create_detached_itc_thread()
{
	auto th = tpp::make_thread();
	th.detach();
	return th.get_id();
}

int main()
{
	tpp::init_data data;
	data.log_error = [](const std::string& msg) { sout() << msg << "\n"; };
	data.log_info = [](const std::string& msg) { sout() << msg << "\n"; };
	data.set_thread_name = [](std::thread& thread, const std::string& name) { /*some platform specific code to name a thread*/ };
	itc::init(data);
	
	{
		// you can register any std::thread
		auto std_thread = make_std_thread();
		auto std_thread_mapped_id = tpp::register_thread(std_thread.get_id());
		auto std_thread_detached_mapped_id = make_detached_std_thread();

		// or you can use itc's make_thread which will 
		// basically do the same as make_std_thread
		auto itc_thread = tpp::make_thread("some_name");
		auto itc_thread_detached_id = create_detached_itc_thread();

		while(!tpp::this_thread::notified_for_exit())
		{
			// process this_thread's pending tasks if any.
			// Also any other blocking call will try to do so
			tpp::this_thread::process();
			
			/// Sleeps for the specified duration and allow tasks to be processed during
			/// that time.
			//tpp::this_thread::sleep_for(16ms);
			
			/// Blocks until notified with an event and process it.
			//tpp::this_thread::wait();
			
			/// Blocks until specified timeout_duration has elapsed or
			/// notified, whichever comes first.
			//tpp::this_thread::wait_for(16ms);

			int arg = 12;
			// clang-format off
			tpp::invoke(std_thread_mapped_id,
			[](int arg)
			{
				sout() << "on std::thread " << arg;

				tpp::invoke(tpp::main_thread::get_id(),
				[](int arg)
				{
					sout() << "on main_thread from std::thread " << arg;
				}, arg);

			}, i);

			tpp::invoke(std_thread_detached_mapped_id,
			[](int arg)
			{
				sout() << "on detached std::thread " << arg;

				tpp::invoke(itc::main_thread::get_id(),
				[](int arg)
				{
					sout() << "on main_thread from detached std::thread " << arg;
				}, arg);

			}, arg);
			tpp::invoke(itc_thread.get_id(),
			[](int arg)
			{
				sout() << "on tpp::thread " << arg;

				tpp::invoke(tpp::main_thread::get_id(),
				[](int arg)
				{
					sout() << "on main_thread from tpp::thread " << arg;
				}, arg);

			}, arg);
			tpp::invoke(itc_thread_detached_id,
			[](int arg)
			{
				sout() << "on detached itc::thread " << arg;

				tpp::invoke(tpp::main_thread::get_id(),
				[](int arg)
				{
					sout() << "on main_thread from detached tpp::thread " << arg;
				}, arg);

			}, arg);
			// clang-format on
		}

		// for std::thread we have to do this
		tpp::notify_for_exit(std_thread_mapped_id);
		if(std_thread.joinable())
		{
			std_thread.join();
		}
		
		// tpp::thread will behave as a proper RAII type and join in its destructor
	}

	//you can also make use of future/promise and continuations
	{
		auto thread1 = tpp::make_thread("some_name1");
		auto thread2 = tpp::make_thread();

		auto th1_id = thread1.get_id();
		auto th2_id = thread2.get_id();
		auto this_th_id = tpp::this_thread::get_id();

		int iterations = 15;
		for(int i = 0; i < iterations; ++i)
		{

			// clang-format off

			// async + chaining continuations
			{
				// some move only object
				// can pass it by move either to the capture list or as a parameter to async
				auto up = std::make_unique<int>(5);
				auto future = tpp::async(th1_id, [u = std::move(up)](int i)
				{
					tpp::this_thread::sleep_for(20ms);

					if(i % 10 == 0)
					{
						throw std::runtime_error("propagated exception");
					}
					return i;
				}, i);

				auto shared_future = tpp::async(th2_id, [u = std::move(up)](int i)
				{
					tpp::this_thread::sleep_for(20ms);

					if(i % 10 == 0)
					{
						throw std::runtime_error("propagated exception");
					}
					return i;
				}, i).share();

				{
					auto chain = future.then(th1_id, [u = std::move(up)](auto parent)
					{
						auto result = parent.get();
						sout() << "chain then " << result;
						return result;
					})
					.then(this_th_id, [](auto parent)
					{
						auto result = parent.get();
						sout() << "chain then " << result;
						return result;
					})
					.then(th2_id, [](auto parent)
					{
						auto result = parent.get();
						sout() << "chain then " << result;
						return result;
					});

					try
					{
						sout() << "wait on chain\n";
						auto result = chain.get();
						sout() << "woke up on chain with result " << result;
					}
					catch(const std::exception& e)
					{
						sout() << e.what();
					}
				}
			}

			// clang-format on

			sout() << "future woke up for" << i;
			sout() << "ASYNC TEST " << i << " completed\n";
		}
		
		// tpp::thread will behave as a proper RAII type and join in its destructor
	}

	return tpp::shutdown();
}
