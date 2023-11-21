#pragma once

#include "future.hpp"
#include <map>
#include <memory>

namespace itc
{
using job_id = uint64_t;

struct job_future_storage
{
	job_id id = 0;
};

template <typename T>
struct job_shared_future;

// Just a normal future with
// a job_id member
template <typename T>
struct job_future : future<T>, job_future_storage
{
	job_future(future<T>&& rhs) noexcept
		: future<T>(std::move(rhs))
	{
	}
	job_future() = default;
	job_future(job_future&& rhs) noexcept = default;
	job_future(const job_future&) = delete;
	auto operator=(job_future&& rhs) noexcept -> job_future& = default;
	auto operator=(const job_future&) -> job_future& = delete;

	auto share() -> job_shared_future<T>
	{
		return job_shared_future<T>(std::move(*this));
	}

	auto use_count() const noexcept -> decltype(auto)
	{
		return this->state_.use_count();
	}
};

template <typename T>
struct job_shared_future : shared_future<T>, job_future_storage
{
	job_shared_future(job_future<T>&& uf) noexcept
		: shared_future<T>(static_cast<future<T>&&>(uf))
		, job_future_storage(static_cast<job_future_storage&&>(uf))
	{
	}
	job_shared_future() noexcept = default;
	job_shared_future(const job_shared_future& sf) = default;
	job_shared_future(job_shared_future&& sf) noexcept = default;

	auto operator=(const job_shared_future& sf) -> job_shared_future& = default;
	auto operator=(job_shared_future&& sf) noexcept -> job_shared_future& = default;

	auto use_count() const noexcept -> decltype(auto)
	{
		return this->state_.use_count();
	}
};

template <typename F, typename... Args>
using job_ret_type = callable_ret_type<F, Args...>;

namespace priority
{

enum class category : size_t
{
	normal,
	high,
	critical
};

struct group
{
	group() = default;
	group(category c, size_t pr)
		: level(c)
		, priority(pr)
	{
	}

	category level = category::normal;
	size_t priority = 0;
};

inline auto operator==(const group& lhs, const group& rhs) -> bool
{
	return lhs.level == rhs.level && lhs.priority == rhs.priority;
}

inline auto normal(size_t priority = 0) -> group
{
	return {category::normal, priority};
}
inline auto high(size_t priority = 0) -> group
{
	return {category::high, priority};
}
inline auto critical(size_t priority = 0) -> group
{
	return {category::critical, priority};
}

} // namespace priority

//-----------------------------------------------------------------------------
/// Thread pool class. Can have multiple priority groups.
//-----------------------------------------------------------------------------
class thread_pool
{
public:
	//-----------------------------------------------------------------------------
	/// Creates a thread_pool with specified workers per priority level.
	/// E.g
	/// itc::thread_pool pool({{itc::priority::category::normal, 2},
	///					       {itc::priority::category::high, 1},
	///					       {itc::priority::category::critical, 1}});
	//-----------------------------------------------------------------------------
	thread_pool(const std::map<priority::category, size_t>& workers_per_priority_level,
				tasks_capacity_config config = {});
	thread_pool();
	thread_pool(thread_pool&&) = default;
	auto operator=(thread_pool&&) -> thread_pool& = default;
	~thread_pool();

	thread_pool(const thread_pool&) = delete;
	auto operator=(const thread_pool&) -> thread_pool& = delete;

	//-----------------------------------------------------------------------------
	/// Adds a job for a certain priority level.
	/// Returns a future to the job.
	//-----------------------------------------------------------------------------
	template <typename F, typename... Args>
	auto schedule(priority::group group, F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

	//-----------------------------------------------------------------------------
	/// Adds a job with default priority level.
	/// Returns a future to the job.
	//-----------------------------------------------------------------------------
	template <typename F, typename... Args>
	auto schedule(F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>;

	//-----------------------------------------------------------------------------
	/// Changes the priority level of the specified job.
	/// Increasing the priority will cause the job to be executed sooner.
	//-----------------------------------------------------------------------------
	void change_priority(job_id id, priority::group group);

	//-----------------------------------------------------------------------------
	/// Attempt to stop a job by its id. If the job is already executing
	/// this function will do nothing.
	//-----------------------------------------------------------------------------
	void stop(job_id id);

	//-----------------------------------------------------------------------------
	/// Stop all pending jobs. This call will not stop any jobs that
	/// are currently running.
	//-----------------------------------------------------------------------------
	void stop_all();

	//-----------------------------------------------------------------------------
	/// Blocks until the job matching the passed id is ready.
	/// It is recommended that you use the future returned by 'schedule'
	/// function as it gives richer options and can also retrieve a return value,
	/// query the readiness and etc.
	//-----------------------------------------------------------------------------
	void wait(job_id id);

	//-----------------------------------------------------------------------------
	/// Blocks until all active jobs are ready.
	//-----------------------------------------------------------------------------
	void wait_all();

	//-----------------------------------------------------------------------------
	/// Returns the number of jobs left.
	//-----------------------------------------------------------------------------
	auto get_jobs_count() const -> size_t;

private:
	auto add_job(task& job, priority::group group) -> job_id;

	class impl;
	/// pimpl idiom
	std::unique_ptr<impl> impl_;
};

template <typename F, typename... Args>
auto thread_pool::schedule(priority::group group, F&& f, Args&&... args)
	-> job_future<job_ret_type<F, Args...>>
{
	auto packaged_task = detail::package_future_task(std::forward<F>(f), std::forward<Args>(args)...);
	job_future<async_ret_type<F, Args...>> fut(std::move(packaged_task.callable_future));
	fut.id = add_job(packaged_task.callable, group);
	return fut;
}

template <typename F, typename... Args>
auto thread_pool::schedule(F&& f, Args&&... args) -> job_future<job_ret_type<F, Args...>>
{
	return schedule(priority::normal(), std::forward<F>(f), std::forward<Args>(args)...);
}

} // namespace itc
