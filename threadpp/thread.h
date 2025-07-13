#pragma once
#include "detail/utility/apply.hpp"
#include "detail/utility/capture.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace tpp
{
//-----------------------------------------------------------------------------
/// std::thread wrapper handling registration and exit notification
//-----------------------------------------------------------------------------
class thread : public std::thread
{
public:
    using id = std::uint64_t;

    void swap(thread& th) noexcept;

    thread() noexcept;
    thread(thread&&) noexcept = default;
    auto operator=(thread&& th) noexcept -> thread&;

    template<typename F, typename... Args>
    explicit thread(F&& f, Args&&... args) : std::thread(std::forward<F>(f), std::forward<Args>(args)...)
    {
        register_this({});
    }

    template<typename F, typename... Args>
    explicit thread(const std::string& name, F&& f, Args&&... args)
        : std::thread(std::forward<F>(f), std::forward<Args>(args)...)
    {
        register_this(name);
    }

    thread(const thread&) = delete;
    auto operator=(const thread&) -> thread& = delete;

    //-----------------------------------------------------------------------------
    /// Destructs the thread object. Notifies and joins if joinable.
    //-----------------------------------------------------------------------------
    ~thread();

    //-----------------------------------------------------------------------------
    /// Returns the unique id of the thread
    //-----------------------------------------------------------------------------
    auto get_id() const -> id;

    //-----------------------------------------------------------------------------
    /// Notifies and waits for a thread to finish its execution
    //-----------------------------------------------------------------------------
    void join();

private:
    void register_this(const std::string& name);

    /// associated thread id
    id id_ = 0;
};

//-----------------------------------------------------------------------------
/// Gets an invalid thread::id
//-----------------------------------------------------------------------------
constexpr inline auto invalid_id() -> thread::id
{
    return {};
}

constexpr inline auto caller_id() -> thread::id
{
    return invalid_id();
}

using shared_thread = std::shared_ptr<thread>;
using task = std::function<void()>;
using clock = std::chrono::steady_clock;

struct tasks_capacity_config
{
    std::size_t default_reserved_tasks{16};
    std::size_t capacity_shrink_threashold{256};
};

struct init_data
{
    std::function<void(const std::string&)> log_info{};
    std::function<void(const std::string&)> log_error{};
    std::function<void(const std::string&)> set_thread_name{};
    std::function<void(const std::string&)> on_thread_start{};
    tasks_capacity_config tasks_capacity{};
};

//-----------------------------------------------------------------------------
/// Inits the itc with user provided utility callbacks
//-----------------------------------------------------------------------------
void init(const init_data& data = {});

//-----------------------------------------------------------------------------
/// Shutdowns itc and waits for all registered threads to unregister themselves.
/// Returns the number of threads that were not able to shutdown.
/// 0 indicates success.
//-----------------------------------------------------------------------------
auto shutdown(const std::chrono::seconds& wait_time = std::chrono::seconds(5)) -> int;

//-----------------------------------------------------------------------------
/// Queues a task to be executed on the specified thread and notifies it.
//-----------------------------------------------------------------------------
template<typename F, typename... Args>
auto invoke(thread::id id, F&& f, Args&&... args) -> bool;

//-----------------------------------------------------------------------------
/// If the calling thread is the same as the one passed it then
/// execute the task directly, else behave like invoke.
//-----------------------------------------------------------------------------
template<typename F, typename... Args>
auto dispatch(thread::id id, F&& f, Args&&... args) -> bool;

//-----------------------------------------------------------------------------
/// Wakes up a thread if sleeping via any of the itc blocking mechanisms.
//-----------------------------------------------------------------------------
void notify(thread::id id);

//-----------------------------------------------------------------------------
/// Wakes up and notifies a thread that it should start preparing for exit.
/// It does not join the thread.
//-----------------------------------------------------------------------------
void notify_for_exit(thread::id id);

//-----------------------------------------------------------------------------
/// Registers a thread by the native thread id.
/// Returns the unique generated id that can be used with the rest of the api.
//-----------------------------------------------------------------------------
auto register_thread(std::thread::id id, const std::string& name = {}) -> thread::id;

//-----------------------------------------------------------------------------
/// Automatically register and run a thread with a prepared loop ready to be
/// invoked into.
//-----------------------------------------------------------------------------
auto make_thread(const std::string& name = {}) -> thread;

//-----------------------------------------------------------------------------
/// Automatically register and run a thread with a prepared loop ready to be
/// invoked into.
//-----------------------------------------------------------------------------
auto make_shared_thread(const std::string& name = {}) -> shared_thread;

//-----------------------------------------------------------------------------
/// Retrieves all registered thread ids.
//-----------------------------------------------------------------------------
auto get_all_registered_threads() -> std::vector<thread::id>;

//-----------------------------------------------------------------------------
/// Retrieves the count of pending tasks for given thread id.
/// Useful for debug.
//-----------------------------------------------------------------------------
struct task_info
{
    size_t left_to_process{};
    size_t processing{};
    size_t pending{};
    size_t total{};

    std::string thread_name{};
};
auto get_pending_task_count_detailed(thread::id id) -> task_info;

auto get_pending_task_count(thread::id id) -> std::size_t;

namespace main_thread
{
//-----------------------------------------------------------------------------
/// Retrieves the main thread id.
//-----------------------------------------------------------------------------
auto get_id() -> thread::id;
} // namespace main_thread

//-----------------------------------------------------------------------------
/// this_thread namespace describe actions you can do
/// while inside a thread's context
//-----------------------------------------------------------------------------
namespace this_thread
{
//-----------------------------------------------------------------------------
/// Registers this thread and links it for fast access.
//-----------------------------------------------------------------------------
void register_this_thread();
void register_this_thread(const std::string& name);

//-----------------------------------------------------------------------------
/// Unregisters this thread and unlinks it.
//-----------------------------------------------------------------------------
void unregister_this_thread();

//-----------------------------------------------------------------------------
/// Check whether this thread has been notified for exit.
//-----------------------------------------------------------------------------
auto notified_for_exit() -> bool;

//-----------------------------------------------------------------------------
/// Gets the current thread id. Returns invalid id if not registered.
//-----------------------------------------------------------------------------
auto get_id() -> thread::id;

//-----------------------------------------------------------------------------
/// Process all tasks.
//-----------------------------------------------------------------------------
void process();

//-----------------------------------------------------------------------------
/// Process all tasks until specified timeout_duration has elapsed.
//-----------------------------------------------------------------------------
template<typename Rep, typename Period>
void process_for(const std::chrono::duration<Rep, Period>& rtime);

//-----------------------------------------------------------------------------
/// Blocks until notified with an event and process it.
//-----------------------------------------------------------------------------
void wait();

//-----------------------------------------------------------------------------
/// Blocks until specified timeout_duration has elapsed or
/// notified, whichever comes first.
/// Returns value identifies the state of the result.
/// This function may block for longer than timeout_duration
/// due to scheduling or resource contention delays.
//-----------------------------------------------------------------------------
template<typename Rep, typename Period>
auto wait_for(const std::chrono::duration<Rep, Period>& rtime) -> std::cv_status;

//-----------------------------------------------------------------------------
/// Blocks until specified abs_time has been reached or
/// notified, whichever comes first.
/// Returns value identifies the state of the result.
/// This function may block for longer than timeout_duration
/// due to scheduling or resource contention delays.
//-----------------------------------------------------------------------------
template<typename Clock, typename Duration>
auto wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) -> std::cv_status;

//-----------------------------------------------------------------------------
/// Sleeps for the specified duration and allow tasks to be processed during
/// that time.
//-----------------------------------------------------------------------------
template<typename Rep, typename Period>
void sleep_for(const std::chrono::duration<Rep, Period>& rtime);

//-----------------------------------------------------------------------------
/// Sleeps until the specified time has been reached and allow tasks to
/// be processed during that time.
//-----------------------------------------------------------------------------
template<typename Clock, typename Duration>
void sleep_until(const std::chrono::time_point<Clock, Duration>& abs_time);

//-----------------------------------------------------------------------------
/// Gets the current processing stack depth of this thread
//-----------------------------------------------------------------------------
auto get_depth() -> std::uint32_t;
auto is_registered() -> bool;

} // namespace this_thread
} // namespace tpp

//-----------------------------------------------------------------------------
/// Impl
//-----------------------------------------------------------------------------
namespace tpp
{
namespace detail
{

template<typename F, typename... Args>
auto package_simple_task(F&& f, Args&&... args) -> task
{
    return [callable = capture(std::forward<F>(f)), params = capture(std::forward<Args>(args)...)]() mutable
    {
        utility::apply(
            [&callable](std::decay_t<Args>&... args)
            {
                std::forward<F>(std::get<0>(callable.get()))(std::forward<Args>(args)...);
            },
            params.get());
    };
}

auto invoke_packaged_task(thread::id id, task& f) -> bool;
} // namespace detail

// apply perfect forwarding to the callable and arguments
// so that so that using invoke/dispatch will result
// in the same number of calls to constructors
template<typename F, typename... Args>
auto invoke(thread::id id, F&& f, Args&&... args) -> bool
{
    auto task = detail::package_simple_task(std::forward<F>(f), std::forward<Args>(args)...);
    return detail::invoke_packaged_task(id, task);
}

// apply perfect forwarding to the callable and arguments
// so that so that using invoke/dispatch will result
// in the same number of calls to constructors
template<typename F, typename... Args>
auto dispatch(thread::id id, F&& f, Args&&... args) -> bool
{
    if(this_thread::get_id() == id)
    {
        // directly call it
        std::forward<F>(f)(std::forward<Args>(args)...);
        return true;
    }
    else
    {
        return invoke(id, std::forward<F>(f), std::forward<Args>(args)...);
    }
}

// set thread config if you want to use different capasity sizes than default from init
auto set_thread_config(thread::id id, tasks_capacity_config config) -> bool;

namespace this_thread
{
namespace detail
{
auto wait_for(const std::chrono::microseconds& rtime) -> std::cv_status;
void process_for(const std::chrono::microseconds& rtime);
} // namespace detail

template<typename Rep, typename Period>
void process_for(const std::chrono::duration<Rep, Period>& rtime)
{
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(rtime);
    detail::process_for(duration);
}

template<typename Rep, typename Period>
inline auto wait_for(const std::chrono::duration<Rep, Period>& rtime) -> std::cv_status
{
    if(rtime <= rtime.zero())
    {
        return std::cv_status::no_timeout;
    }

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(rtime);

    return detail::wait_for(duration);
}

template<typename Clock, typename Duration>
inline auto wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) -> std::cv_status
{
    return wait_for(abs_time.time_since_epoch() - Clock::now().time_since_epoch());
}

template<typename Rep, typename Period>
inline void sleep_for(const std::chrono::duration<Rep, Period>& rtime)
{
    if(rtime <= rtime.zero())
    {
        return;
    }

    auto now = clock::now();
    auto end_time = now + rtime;

    while(now < end_time)
    {
        if(notified_for_exit())
        {
            return;
        }
        auto time_left = end_time - now;

        wait_for(time_left);

        now = clock::now();
    }
}

template<typename Clock, typename Duration>
inline void sleep_until(const std::chrono::time_point<Clock, Duration>& abs_time)
{
    sleep_for(abs_time.time_since_epoch() - Clock::now().time_since_epoch());
}
} // namespace this_thread

} // namespace tpp
