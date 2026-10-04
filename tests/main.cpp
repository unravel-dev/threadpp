#include "threadpp/thread.h"

#include "async_tests.h"
#include "when_tests.h"
#include "condition_variable_tests.h"
#include "fututre_promise_tests.h"
#include "invoke_tests.h"
#include "on_process_tests.h"
#include "overhead_tests.h"
#include "thread_pool_tests.h"

#include "utils.hpp"
#include <iostream>
#include <mutex>
#include <string>

int main()
{
	tpp::init_data data;
	data.log_error = [](const std::string& msg) { sout() << msg << "\n"; };
    data.log_info = [](const std::string& msg) { sout() << msg << "\n"; };
	tpp::init(data);

    overhead_tests::run_tests();
    invoke_tests::run_tests(1000);
    cv_tests::run_tests(50);
    future_promise_tests::run_tests(50);
    async_tests::run_tests(50);
    when_tests::run_tests(50);
    thread_pool_tests::run_tests(50);
    on_process_tests::run_tests();

	tpp::shutdown();
	return 0;
}
