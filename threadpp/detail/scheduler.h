#pragma once
#include "../thread.h"
#include "parker.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

//-----------------------------------------------------------------------------
/// How a blocked thread makes progress, whatever the number of threads. Every
/// blocking call (a future's wait or get, this_thread::sleep_for, the
/// condition_variable and mutex) runs one loop: the thread's own queued tasks,
/// then the pool jobs its wait depends on, then - only when no other thread can -
/// other queued pool jobs; then it parks until something changes.
//-----------------------------------------------------------------------------
namespace tpp
{
namespace detail
{

//-----------------------------------------------------------------------------
/// A thread pool, as the waits see it: the jobs a waiting thread may run itself.
//-----------------------------------------------------------------------------
class job_source
{
public:
    job_source() = default;
    job_source(const job_source&) = delete;
    auto operator=(const job_source&) -> job_source& = delete;
    virtual ~job_source() = default;

    //-----------------------------------------------------------------------------
    /// Runs job `id` on the calling thread when it is queued and no worker should
    /// take it instead: the pool has no workers, or no worker is free and the
    /// wait is untimed or the waiter is one of the pool's workers. A timed wait of
    /// any other thread runs no job, so it returns near its deadline. True when
    /// it ran it.
    //-----------------------------------------------------------------------------
    virtual auto run_waited_job(std::uint64_t id, bool is_timed) -> bool = 0;

    //-----------------------------------------------------------------------------
    /// Runs one queued job while the calling thread waits for something whose
    /// producer is unknown (a promise, a condition variable, a sleep), when no
    /// other thread would: the thread hosts the pool without workers, or it is a
    /// worker and every worker is busy. Jobs scheduled by the job the thread runs
    /// go first, then any. True when it ran one.
    //-----------------------------------------------------------------------------
    virtual auto run_job_to_help(bool is_timed) -> bool = 0;

    //-----------------------------------------------------------------------------
    /// Whether the calling thread may run run_job_to_help at all: it hosts a pool
    /// without workers, or it is one of the pool's workers.
    //-----------------------------------------------------------------------------
    virtual auto can_help() const -> bool = 0;

    //-----------------------------------------------------------------------------
    /// The parker of a thread blocked in a wait that may run this pool's jobs:
    /// scheduling a job wakes it.
    //-----------------------------------------------------------------------------
    virtual void add_parked_helper(const parker_ptr& helper) = 0;
    virtual void remove_parked_helper(const parker_ptr& helper) = 0;

    //-----------------------------------------------------------------------------
    /// The job's name and detail, for reports.
    //-----------------------------------------------------------------------------
    virtual auto describe_job(std::uint64_t id) const -> std::string = 0;
};

//-----------------------------------------------------------------------------
/// The pool job that produces a future's result.
//-----------------------------------------------------------------------------
struct job_link
{
    std::weak_ptr<job_source> source{};
    std::uint64_t id{0};
};

//-----------------------------------------------------------------------------
/// Something a thread can wait for.
//-----------------------------------------------------------------------------
class wait_target
{
public:
    virtual auto is_ready() const -> bool = 0;
    //-----------------------------------------------------------------------------
    /// The parker to unpark when it becomes ready.
    //-----------------------------------------------------------------------------
    virtual void add_waiter(const parker_ptr& waiter) = 0;
    virtual void remove_waiter(const parker_ptr& waiter) = 0;
    //-----------------------------------------------------------------------------
    /// The pool jobs whose work makes it ready, the nearest first, at most `depth`
    /// links away.
    //-----------------------------------------------------------------------------
    virtual void collect_jobs(std::vector<job_link>& jobs, std::size_t depth) const;
    //-----------------------------------------------------------------------------
    /// What it is, for reports.
    //-----------------------------------------------------------------------------
    virtual auto describe() const -> std::string;

protected:
    wait_target() = default;
    wait_target(const wait_target&) = default;
    auto operator=(const wait_target&) -> wait_target& = default;
    ~wait_target() = default;
};

enum class wait_status
{
    ready,
    timeout,
    exit
};

//-----------------------------------------------------------------------------
/// Blocks the calling thread until `target` is ready, the deadline passes or the
/// thread is notified for exit, running work as the namespace comment says. A
/// timed wait of a thread outside a pool with workers runs none of its jobs, so it
/// returns near its deadline; any wait makes one attempt even when out of time.
/// Works on a thread that is not registered, which has no tasks of its own.
//-----------------------------------------------------------------------------
auto wait_for_target(wait_target& target, bool is_timed, clock::time_point deadline) -> wait_status;

//-----------------------------------------------------------------------------
/// The calling thread's parker: its context's, or one of its own when the thread
/// is not registered.
//-----------------------------------------------------------------------------
auto current_parker() -> parker_ptr;

//-----------------------------------------------------------------------------
/// A pool job running on the calling thread: the jobs it schedules belong to its
/// region, and a wait inside it helps with those first.
//-----------------------------------------------------------------------------
class job_scope
{
public:
    job_scope(const job_source* source, std::uint64_t id);
    ~job_scope();
    job_scope(const job_scope&) = delete;
    auto operator=(const job_scope&) -> job_scope& = delete;

    const job_source* source{};
    std::uint64_t id{};
    job_scope* outer{};
};

//-----------------------------------------------------------------------------
/// The job of `source` that runs on the calling thread, the innermost one; 0 when none.
//-----------------------------------------------------------------------------
auto current_job_of(const job_source* source) -> std::uint64_t;

//-----------------------------------------------------------------------------
/// Whether the calling thread runs job `id` of `source` further up its stack.
//-----------------------------------------------------------------------------
auto is_running_here(const job_source* source, std::uint64_t id) -> bool;

//-----------------------------------------------------------------------------
/// Jobs run inside waits nest on the stack; past this depth a wait runs none and
/// only waits.
//-----------------------------------------------------------------------------
constexpr std::uint32_t max_helping_depth = 64;

//-----------------------------------------------------------------------------
/// Makes the calling thread a worker of `source` for as long as it lives.
//-----------------------------------------------------------------------------
class worker_scope
{
public:
    explicit worker_scope(const std::weak_ptr<job_source>& source);
    ~worker_scope();
    worker_scope(const worker_scope&) = delete;
    auto operator=(const worker_scope&) -> worker_scope& = delete;
};

//-----------------------------------------------------------------------------
/// Whether the calling thread is a worker of `source`.
//-----------------------------------------------------------------------------
auto is_worker_of(const job_source* source) -> bool;

//-----------------------------------------------------------------------------
/// Makes the calling thread the host of a pool without workers: its waits and
/// sleeps run the pool's queued jobs when nothing else would.
//-----------------------------------------------------------------------------
void add_hosted_source(const std::weak_ptr<job_source>& source);

//-----------------------------------------------------------------------------
/// Whether the calling thread hosts `source`.
//-----------------------------------------------------------------------------
auto is_host_of(const job_source* source) -> bool;

} // namespace detail
} // namespace tpp
