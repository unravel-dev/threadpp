#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

//-----------------------------------------------------------------------------
/// What the scheduling, sync and stress tests share: the command line options,
/// a watchdog that ends the run when a case hangs, captured log lines and small
/// thread helpers. Checks (EXPECT) run on the main thread only: suitepp is not
/// thread-safe, so a test thread records what it saw and the main thread checks.
//-----------------------------------------------------------------------------
namespace test_support
{
using namespace std::chrono_literals;

struct options
{
    /// How many times the repeatable cases run.
    int iterations = 1;
    /// Only the cases whose name contains it run; empty: all.
    std::string filter{};
    /// Multiplies every time limit: sanitizers run the code several times slower.
    double timeout_scale = 1.0;
    /// Runs the timings instead of the tests.
    bool is_benchmark = false;
};

auto get_options() -> options&;

//-----------------------------------------------------------------------------
/// Reads --iterations N, --filter TEXT, --timeout-scale X and --bench. False,
/// with a message, on anything else.
//-----------------------------------------------------------------------------
auto parse_options(int argc, char** argv) -> bool;

//-----------------------------------------------------------------------------
/// A time limit scaled by --timeout-scale.
//-----------------------------------------------------------------------------
auto scaled(std::chrono::milliseconds duration) -> std::chrono::milliseconds;

//-----------------------------------------------------------------------------
/// Runs `body` as a suitepp test case when its name passes the filter. When it
/// runs longer than `limit` (scaled) the process ends with exit code 3 and the
/// case's name: a hang fails the run instead of stalling it.
//-----------------------------------------------------------------------------
void run_case(const std::string& name,
              const std::function<void()>& body,
              std::chrono::milliseconds limit = std::chrono::milliseconds(20000));

//-----------------------------------------------------------------------------
/// The pool sizes every scheduling case runs with: no workers (the host runs
/// everything), one, two and eight.
//-----------------------------------------------------------------------------
auto get_worker_counts() -> const std::vector<size_t>&;

//-----------------------------------------------------------------------------
/// The name of a case for a pool size, e.g. "pool[2] : ...".
//-----------------------------------------------------------------------------
auto name_for(size_t worker_count, const std::string& text) -> std::string;

//-----------------------------------------------------------------------------
/// Polls `predicate` with real sleeps (no tpp work) until it holds or `limit`
/// (scaled) passes; returns its last value.
//-----------------------------------------------------------------------------
auto poll_until(const std::function<bool()>& predicate,
                std::chrono::milliseconds limit = std::chrono::milliseconds(10000)) -> bool;

//-----------------------------------------------------------------------------
/// Log lines tpp wrote through init_data since the last clear.
//-----------------------------------------------------------------------------
void add_log_line(const std::string& line);
auto get_log_lines() -> std::vector<std::string>;
void clear_log_lines();
auto has_log_line_containing(const std::string& text) -> bool;

//-----------------------------------------------------------------------------
/// Holds the threads that call wait() until open(): a job that keeps its
/// worker busy without running any tpp work meanwhile.
//-----------------------------------------------------------------------------
class gate
{
public:
    void open();
    void wait() const;
    auto get_waiting_count() const -> int;

private:
    std::atomic<bool> is_open_{false};
    mutable std::atomic<int> waiting_count_{0};
};

//-----------------------------------------------------------------------------
/// Records the order in which jobs ran, from any thread.
//-----------------------------------------------------------------------------
class order_log
{
public:
    void add(int value);
    auto get() const -> std::vector<int>;

private:
    std::vector<int> values_;
    mutable std::mutex mutex_;
};

} // namespace test_support
