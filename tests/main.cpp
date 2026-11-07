#include "threadpp/thread.h"

#include "async_tests.h"
#include "benchmark_tests.h"
#include "when_tests.h"
#include "condition_variable_tests.h"
#include "fututre_promise_tests.h"
#include "invoke_tests.h"
#include "on_process_tests.h"
#include "overhead_tests.h"
#include "reentrancy_tests.h"
#include "scheduling_tests.h"
#include "stress_tests.h"
#include "sync_tests.h"
#include "test_support.h"
#include "thread_pool_tests.h"

#include "utils.hpp"
#include <iostream>
#include <mutex>
#include <string>

// threadpp_test [--iterations N] [--filter TEXT] [--timeout-scale X] [--bench]
// The environment variables THREADPP_TEST_ITERATIONS and THREADPP_TEST_TIMEOUT_SCALE set the defaults.
// With a filter only the scheduling, sync and stress cases whose name contains it run.
int main(int argc, char** argv)
{
	if(!test_support::parse_options(argc, argv))
	{
		return 2;
	}

	tpp::init_data data;
	data.log_error = [](const std::string& msg) { test_support::add_log_line(msg); };
	data.log_info = [](const std::string& msg) { test_support::add_log_line(msg); };
	tpp::init(data);

	if(test_support::get_options().is_benchmark)
	{
		benchmark_tests::run_benchmarks();
		tpp::shutdown();
		return 0;
	}
	if(test_support::get_options().filter.empty())
	{
		overhead_tests::run_tests();
		invoke_tests::run_tests(1000);
		cv_tests::run_tests(50);
		future_promise_tests::run_tests(50);
		async_tests::run_tests(50);
		when_tests::run_tests(50);
		thread_pool_tests::run_tests(50);
		on_process_tests::run_tests();
	}
	scheduling_tests::run_tests();
	reentrancy_tests::run_tests();
	sync_tests::run_tests();
	stress_tests::run_tests();

	tpp::shutdown();
	return 0;
}
