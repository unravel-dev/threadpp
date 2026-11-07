#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>

namespace tpp
{
namespace detail
{

//-----------------------------------------------------------------------------
/// Puts one thread to sleep until something it may wait for changes. An event
/// count: the thread takes a ticket, checks what it waits for, and sleeps only
/// while no unpark came after its ticket, so a change between the check and the
/// sleep is never missed. Every thread has one (its context's, or one made for
/// a thread that is not registered); only that thread parks on it, and whatever
/// it waits for unparks it. An unpark takes the lock only while the thread sleeps.
//-----------------------------------------------------------------------------
class parker
{
public:
    parker() = default;
    parker(const parker&) = delete;
    auto operator=(const parker&) -> parker& = delete;

    //-----------------------------------------------------------------------------
    /// The ticket to park with: take it before checking what to wait for.
    //-----------------------------------------------------------------------------
    auto prepare() const -> std::uint64_t
    {
        return epoch_.load(std::memory_order_seq_cst);
    }

    //-----------------------------------------------------------------------------
    /// Sleeps until an unpark that came after the ticket.
    //-----------------------------------------------------------------------------
    void park(std::uint64_t ticket)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        // Set before the epoch is read again (both sequentially consistent): either this thread sees the
        // unpark's epoch or the unpark sees it parked and notifies under the lock.
        is_parked_.store(true, std::memory_order_seq_cst);
        wakeup_.wait(lock,
                     [&]()
                     {
                         return epoch_.load(std::memory_order_seq_cst) != ticket;
                     });
        is_parked_.store(false, std::memory_order_relaxed);
    }

    //-----------------------------------------------------------------------------
    /// Sleeps until an unpark that came after the ticket or the deadline; false
    /// when the deadline came first.
    //-----------------------------------------------------------------------------
    auto park_until(std::uint64_t ticket, std::chrono::steady_clock::time_point deadline) -> bool
    {
        std::unique_lock<std::mutex> lock(mutex_);
        is_parked_.store(true, std::memory_order_seq_cst);
        const bool is_unparked = wakeup_.wait_until(lock,
                                                    deadline,
                                                    [&]()
                                                    {
                                                        return epoch_.load(std::memory_order_seq_cst) != ticket;
                                                    });
        is_parked_.store(false, std::memory_order_relaxed);
        return is_unparked;
    }

    //-----------------------------------------------------------------------------
    /// Wakes the thread, or makes its next park with an older ticket return at once.
    //-----------------------------------------------------------------------------
    void unpark()
    {
        epoch_.fetch_add(1, std::memory_order_seq_cst);
        if(is_parked_.load(std::memory_order_seq_cst))
        {
            std::lock_guard<std::mutex> lock(mutex_);
            wakeup_.notify_all();
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable wakeup_;
    std::atomic<std::uint64_t> epoch_{0};
    std::atomic<bool> is_parked_{false};
};

using parker_ptr = std::shared_ptr<parker>;

} // namespace detail
} // namespace tpp
