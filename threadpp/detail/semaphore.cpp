#include "semaphore.h"
#include "scheduler.h"
#include <algorithm>

namespace tpp
{
namespace detail
{
namespace
{
/// A semaphore wait: ready once notified or once its predicate holds. The
/// semaphore keeps the waiter's parker itself.
struct token_target final : wait_target
{
    token_target(const interrupt_token& token, const semaphore::predicate& is_done)
        : token_(token)
        , is_done_(is_done)
    {
    }

    auto is_ready() const -> bool override
    {
        return token_.is_interrupted() || (is_done_ && is_done_());
    }

    void add_waiter(const parker_ptr& /*waiter*/) override
    {
    }

    void remove_waiter(const parker_ptr& /*waiter*/) override
    {
    }

    auto describe() const -> std::string override
    {
        return "a condition variable or mutex";
    }

    const interrupt_token& token_;
    const semaphore::predicate& is_done_;
};

/// Runs `after` when the wait ends, also when it throws.
struct wait_end_scope
{
    wait_end_scope(const std::function<void()>& after)
        : after_(after)
    {
    }

    ~wait_end_scope()
    {
        after_();
    }

    wait_end_scope(const wait_end_scope&) = delete;
    auto operator=(const wait_end_scope&) -> wait_end_scope& = delete;

    const std::function<void()>& after_;
};

auto make_deadline(const std::chrono::nanoseconds& timeout_duration) -> clock::time_point
{
    const auto now = clock::now();
    if(timeout_duration <= std::chrono::nanoseconds::zero())
    {
        return now;
    }
    const auto remaining = clock::time_point::max() - now;
    if(timeout_duration >= remaining)
    {
        return clock::time_point::max();
    }
    return now + std::chrono::duration_cast<clock::duration>(timeout_duration);
}
} // namespace

interrupt_token::interrupt_token(bool b) : flag_{std::make_shared<std::atomic<bool>>(b)}
{
}

auto interrupt_token::interrupt() const noexcept -> bool
{
    assert(flag_ != nullptr);
    return flag_->exchange(true);
}

auto interrupt_token::is_interrupted() const noexcept -> bool
{
    assert(flag_ != nullptr);
    return flag_->load();
}

auto interrupt_token::is_same(const interrupt_token& other) const noexcept -> bool
{
    return flag_ == other.flag_;
}

void semaphore::wake(const waiter_info& waiter, std::vector<waiter_info>& woken) const
{
    waiter.token.interrupt();
    woken.push_back(waiter);
    // a spurious wake for the others, which a condition variable allows
    auto same_thread = std::stable_partition(std::begin(waiters_),
                                             std::end(waiters_),
                                             [&](const waiter_info& other)
                                             {
                                                 return other.parker != waiter.parker;
                                             });
    std::for_each(same_thread,
                  std::end(waiters_),
                  [&](const waiter_info& other)
                  {
                      other.token.interrupt();
                  });
    waiters_.erase(same_thread, std::end(waiters_));
}

void semaphore::notify_one() noexcept
{
    std::vector<waiter_info> woken;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if(!waiters_.empty())
        {
            // The standard doesn't specify any order/priority
            // here. We are fair and wake the oldest one
            auto waiter = std::move(waiters_.front());
            waiters_.erase(waiters_.begin());
            wake(waiter, woken);
        }
    }
    for(const auto& waiter : woken)
    {
        waiter.parker->unpark();
    }
}

void semaphore::notify_all() noexcept
{
    std::vector<waiter_info> woken;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        woken = std::move(waiters_);
        waiters_.clear();
    }
    for(const auto& waiter : woken)
    {
        waiter.token.interrupt();
        waiter.parker->unpark();
    }
}

void semaphore::wait(const callback& before_wait, const callback& after_wait) const
{
    wait_impl(false, clock::time_point::max(), nullptr, before_wait, after_wait);
}

void semaphore::wait_unless(const predicate& is_done) const
{
    wait_impl(false, clock::time_point::max(), is_done, nullptr, nullptr);
}

auto semaphore::wait_for_impl(const std::chrono::nanoseconds& timeout_duration,
                              const callback& before_wait,
                              const callback& after_wait) const -> std::cv_status
{
    return wait_impl(true, make_deadline(timeout_duration), nullptr, before_wait, after_wait);
}

auto semaphore::wait_impl(bool is_timed,
                          clock::time_point deadline,
                          const predicate& is_done,
                          const callback& before_wait,
                          const callback& after_wait) const -> std::cv_status
{
    auto token = add_waiter();
    const std::function<void()> end_wait = [&]()
    {
        remove_waiter(token);
        if(after_wait)
        {
            after_wait();
        }
    };
    wait_end_scope scope(end_wait);
    if(before_wait)
    {
        before_wait();
    }
    token_target target(token, is_done);
    const auto status = wait_for_target(target, is_timed, deadline);
    if(status == wait_status::timeout && !target.is_ready())
    {
        return std::cv_status::timeout;
    }
    return std::cv_status::no_timeout;
}

auto semaphore::add_waiter() const -> interrupt_token
{
    waiter_info info;
    info.token = interrupt_token{false};
    info.parker = current_parker();
    std::lock_guard<std::mutex> waiting_lock(mutex_);
    waiters_.emplace_back(std::move(info));
    return waiters_.back().token;
}

void semaphore::remove_waiter(const interrupt_token& token) const
{
    std::lock_guard<std::mutex> waiting_lock(mutex_);
    waiters_.erase(std::remove_if(std::begin(waiters_),
                                  std::end(waiters_),
                                  [&token](const waiter_info& waiter)
                                  {
                                      return waiter.token.is_same(token);
                                  }),
                   std::end(waiters_));
}

} // namespace detail
} // namespace tpp
