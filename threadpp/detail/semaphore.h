#pragma once
#include "../thread.h"
#include "parker.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <mutex>
#include <vector>

namespace tpp
{
namespace detail
{

// based on p0660r2
class interrupt_token
{
public:
    explicit interrupt_token() = default;
    explicit interrupt_token(bool b);

    auto interrupt() const noexcept -> bool;
    auto is_interrupted() const noexcept -> bool;
    auto is_same(const interrupt_token& other) const noexcept -> bool;

private:
    std::shared_ptr<std::atomic<bool>> flag_;
};

//-----------------------------------------------------------------------------
/// What condition_variable and mutex block on. A waiting thread runs work as
/// every tpp wait does (scheduler.h) and works whether or not it is registered.
//-----------------------------------------------------------------------------
class semaphore
{
public:
    using callback = std::function<void()>;
    using predicate = std::function<bool()>;

    semaphore() noexcept = default;

    // mutex prevents us from being moveable
    semaphore(semaphore&& rhs) = delete;
    auto operator=(semaphore&& rhs) -> semaphore& = delete;

    semaphore(const semaphore&) = delete;
    auto operator=(const semaphore&) -> semaphore& = delete;

    //-----------------------------------------------------------------------------
    /// If any threads are waiting on *this,
    /// calling notify_one unblocks one of the waiting threads.
    //-----------------------------------------------------------------------------
    void notify_one() noexcept;

    //-----------------------------------------------------------------------------
    /// Unblocks all threads currently waiting for *this.
    //-----------------------------------------------------------------------------
    void notify_all() noexcept;

    //-----------------------------------------------------------------------------
    /// Waits for a notify. before_wait runs once the thread is registered as a
    /// waiter, after_wait once it is not, also when the wait throws.
    //-----------------------------------------------------------------------------
    void wait(const callback& before_wait = nullptr, const callback& after_wait = nullptr) const;

    //-----------------------------------------------------------------------------
    /// Waits for a notify or for `is_done` to hold. It is checked once the thread is
    /// registered as a waiter, so a change notified right before is not missed.
    //-----------------------------------------------------------------------------
    void wait_unless(const predicate& is_done) const;

    //-----------------------------------------------------------------------------
    /// Blocks until specified timeout_duration has elapsed or
    /// notified, whichever comes first.
    /// Returns value identifies the state of the result.
    /// This function may block for longer than timeout_duration
    /// due to scheduling or resource contention delays.
    //-----------------------------------------------------------------------------
    template<class Rep, class Per>
    auto wait_for(const std::chrono::duration<Rep, Per>& timeout_duration,
                  const callback& before_wait = nullptr,
                  const callback& after_wait = nullptr) const -> std::cv_status
    {
        return wait_for_impl(std::chrono::duration_cast<std::chrono::nanoseconds>(timeout_duration),
                             before_wait,
                             after_wait);
    }

    //-----------------------------------------------------------------------------
    /// Blocks until specified abs_time is reached or
    /// notified, whichever comes first.
    /// Returns value identifies the state of the result.
    /// This function may block for longer than timeout_duration
    /// due to scheduling or resource contention delays.
    //-----------------------------------------------------------------------------
    template<class Clock, class Duration>
    auto wait_until(const std::chrono::time_point<Clock, Duration>& abs_time,
                    const callback& before_wait = nullptr,
                    const callback& after_wait = nullptr) const -> std::cv_status
    {
        return wait_for(abs_time - Clock::now(), before_wait, after_wait);
    }

protected:
    struct waiter_info
    {
        interrupt_token token;
        parker_ptr parker;
    };

    auto wait_for_impl(const std::chrono::nanoseconds& timeout_duration,
                       const callback& before_wait,
                       const callback& after_wait) const -> std::cv_status;

    auto wait_impl(bool is_timed,
                   clock::time_point deadline,
                   const predicate& is_done,
                   const callback& before_wait,
                   const callback& after_wait) const -> std::cv_status;

    auto add_waiter() const -> interrupt_token;

    void remove_waiter(const interrupt_token& token) const;

    // wakes the waiter and the other waits of its thread: a wait nested under
    // it on the same stack must end first for it to go on
    void wake(const waiter_info& waiter, std::vector<waiter_info>& woken) const;

    mutable std::mutex mutex_;
    mutable std::vector<waiter_info> waiters_;
};
} // namespace detail
} // namespace tpp
