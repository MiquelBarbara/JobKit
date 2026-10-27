#pragma once
#include "ThreadQueue.h"
#include "Task.h"
#include "TaskObserver.h"
#include "Cancellation.h"
#include <atomic>
#include <functional>
#include <thread>
#include <vector>
#include <cstddef>
#include <mutex>
#include <condition_variable>
#include <exception>
#include <future>
#include <memory>
#include <type_traits>
#include <utility>

namespace JobKit
{

class JobGroup;

// Use at least one worker. Task exceptions are caught and the first recorded failure
// is rethrown by wait() after all work finishes. That wait() consumes the stored error.
// Stop submitting before destroying the pool. Call wait() from outside its workers:
// a running task is itself unfinished, so waiting for the whole pool would deadlock.
class ThreadPool
{
private:
	struct ScheduledTask
	{
		Task task;
		JobClock::time_point queuedAt{};
		bool observed = false;
	};
	void decreaseUnfinishedTasks();
	std::jthread makeThreadHandler(std::size_t workerIndex);

public:
	ThreadPool(std::size_t numThreads, std::shared_ptr<TaskObserver> observer = {});
	~ThreadPool();

	bool push(Task const& task);
	// Captures own the input. Callable errors belong to the returned future, not wait().
	// The shared packaged task lets move-only callables/results use the copyable queue.
	template <class Function>
	auto submit(Function&& function) -> std::future<std::invoke_result_t<std::decay_t<Function>&>>
	{
		using Result = std::invoke_result_t<std::decay_t<Function>&>;
		auto task = std::make_shared<std::packaged_task<Result()>>(std::forward<Function>(function));
		auto result = task->get_future();
		push({[task] { (*task)(); }});
		return result;
	}

	template <class Function>
	auto submit(std::stop_token token, Function&& function)
	    -> std::future<std::invoke_result_t<std::decay_t<Function>&, std::stop_token>>
	{
		return submit(cancellable(token, std::forward<Function>(function)));
	}

	void dispatch(std::size_t elementCount, std::size_t batchSize, std::function<void(std::size_t, std::size_t)> operation, JobGroup* group = nullptr);
	void wait();
	static bool isWorkerThread() noexcept;
	// Applies to new submissions. Already observed tasks finish their hook pair.
	void setObserverEnabled(bool enabled) noexcept;
	// Queued plus executing entries; a live gauge, not a stable idle barrier.
	std::size_t unfinishedTaskCount();
private:
	static thread_local bool _isWorkerThread;

	std::shared_ptr<TaskObserver> _observer;
	std::atomic<bool> _observerEnabled{true};
	ThreadQueue<ScheduledTask> _queue;
	// Completion uses separate synchronization from the queue's "work available" wait.
	std::condition_variable _condition;
	std::mutex _mutex;
	// Counts queued AND executing entries.
	int _unfinished_tasks = 0;

	std::exception_ptr _exception;
	std::mutex _exception_mutex;

	// Keep LAST: workers join before the queue and completion state are destroyed.
	std::vector<std::jthread> _threads;
};

} // namespace JobKit
