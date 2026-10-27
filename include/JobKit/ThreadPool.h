#pragma once
#include "ThreadQueue.h"
#include "Task.h"
#include <functional>
#include <thread>
#include <vector>
#include <cstddef>
#include <mutex>
#include <condition_variable>
#include <exception>

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
	void decreaseUnfinishedTasks();
	std::jthread makeThreadHandler(ThreadQueue<Task>& queue);

public:
	ThreadPool(std::size_t numThreads);
	~ThreadPool();

	bool push(Task const& task);
	void dispatch(std::size_t elementCount, std::size_t batchSize, std::function<void(std::size_t, std::size_t)> operation, JobGroup* group = nullptr);
	void wait();
	static bool isWorkerThread() noexcept;
private:
	static thread_local bool _isWorkerThread;

	ThreadQueue<Task> _queue;
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
