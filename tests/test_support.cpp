#include "test_support.h"
#include "utils.hpp"

#include <suitepp/suite.hpp>

#include <algorithm>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <mutex>

namespace test_support
{
namespace
{
std::mutex log_mutex;

auto read_environment(const char* name) -> std::string
{
#ifdef _WIN32
    char* value = nullptr;
    size_t size = 0;
    if(_dupenv_s(&value, &size, name) != 0 || value == nullptr)
    {
        return {};
    }
    std::string result = value;
    std::free(value);
    return result;
#else
    const char* value = std::getenv(name);
    return value == nullptr ? std::string{} : std::string{value};
#endif
}

std::vector<std::string> log_lines;

/// Ends the process when the guarded scope outlives its limit.
class watchdog
{
public:
    watchdog(const std::string& name, std::chrono::milliseconds limit)
        : thread_(
              [this, name, limit]()
              {
                  std::unique_lock<std::mutex> lock(mutex_);
                  if(!done_event_.wait_for(lock,
                                           limit,
                                           [this]()
                                           {
                                               return is_done_;
                                           }))
                  {
                      std::fprintf(stderr,
                                   "\n[WATCHDOG] '%s' ran longer than %lld ms: a wait never returned.\n",
                                   name.c_str(),
                                   static_cast<long long>(limit.count()));
                      std::fflush(stderr);
                      std::_Exit(3);
                  }
              })
    {
    }

    ~watchdog()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            is_done_ = true;
        }
        done_event_.notify_all();
        thread_.join();
    }

    watchdog(const watchdog&) = delete;
    auto operator=(const watchdog&) -> watchdog& = delete;

private:
    std::mutex mutex_;
    std::condition_variable done_event_;
    bool is_done_{false};
    std::thread thread_;
};
} // namespace

auto get_options() -> options&
{
    static options current;
    return current;
}

auto parse_options(int argc, char** argv) -> bool
{
    auto& current = get_options();
    // Defaults from the environment, for runs that cannot pass arguments (ctest presets).
    const auto iterations = read_environment("THREADPP_TEST_ITERATIONS");
    if(!iterations.empty())
    {
        current.iterations = std::max(1, std::atoi(iterations.c_str()));
    }
    const auto timeout_scale = read_environment("THREADPP_TEST_TIMEOUT_SCALE");
    if(!timeout_scale.empty())
    {
        current.timeout_scale = std::max(1.0, std::atof(timeout_scale.c_str()));
    }
    for(int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        const bool has_value = i + 1 < argc;
        if(arg == "--iterations" && has_value)
        {
            current.iterations = std::max(1, std::atoi(argv[++i]));
        }
        else if(arg == "--filter" && has_value)
        {
            current.filter = argv[++i];
        }
        else if(arg == "--timeout-scale" && has_value)
        {
            current.timeout_scale = std::max(1.0, std::atof(argv[++i]));
        }
        else if(arg == "--bench")
        {
            current.is_benchmark = true;
        }
        else
        {
            std::cerr << "usage: threadpp_test [--iterations N] [--filter TEXT] [--timeout-scale X] [--bench]\n";
            return false;
        }
    }
    return true;
}

auto scaled(std::chrono::milliseconds duration) -> std::chrono::milliseconds
{
    return std::chrono::milliseconds(
        static_cast<std::chrono::milliseconds::rep>(static_cast<double>(duration.count()) * get_options().timeout_scale));
}

void run_case(const std::string& name, const std::function<void()>& body, std::chrono::milliseconds limit)
{
    const auto& current = get_options();
    if(!current.filter.empty() && name.find(current.filter) == std::string::npos)
    {
        return;
    }
    watchdog guard(name, scaled(limit));
    TEST_CASE(name)
    {
        EXPECT_NOTHROWS(body());
    };
}

auto get_worker_counts() -> const std::vector<size_t>&
{
    static const std::vector<size_t> counts{0, 1, 2, 8};
    return counts;
}

auto name_for(size_t worker_count, const std::string& text) -> std::string
{
    return "pool[" + std::to_string(worker_count) + "] : " + text;
}

auto poll_until(const std::function<bool()>& predicate, std::chrono::milliseconds limit) -> bool
{
    const auto deadline = std::chrono::steady_clock::now() + scaled(limit);
    while(!predicate())
    {
        if(std::chrono::steady_clock::now() >= deadline)
        {
            return predicate();
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return true;
}

void add_log_line(const std::string& line)
{
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        log_lines.push_back(line);
    }
    sout() << line;
}

auto get_log_lines() -> std::vector<std::string>
{
    std::lock_guard<std::mutex> lock(log_mutex);
    return log_lines;
}

void clear_log_lines()
{
    std::lock_guard<std::mutex> lock(log_mutex);
    log_lines.clear();
}

auto has_log_line_containing(const std::string& text) -> bool
{
    std::lock_guard<std::mutex> lock(log_mutex);
    return std::any_of(log_lines.begin(),
                       log_lines.end(),
                       [&text](const std::string& line)
                       {
                           return line.find(text) != std::string::npos;
                       });
}

void gate::open()
{
    is_open_ = true;
}

void gate::wait() const
{
    ++waiting_count_;
    while(!is_open_)
    {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    --waiting_count_;
}

auto gate::get_waiting_count() const -> int
{
    return waiting_count_;
}

void order_log::add(int value)
{
    std::lock_guard<std::mutex> lock(mutex_);
    values_.push_back(value);
}

auto order_log::get() const -> std::vector<int>
{
    std::lock_guard<std::mutex> lock(mutex_);
    return values_;
}

} // namespace test_support
