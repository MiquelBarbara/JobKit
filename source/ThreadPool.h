#pragma once
#include <functional>
#include <thread>
#include <vector>
#include <cstddef>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <stdexcept>
#include <exception>


#include "ThreadQueue.h"
#include "JobGroup.h"
#include "Task.h"

namespace JobKit
{

// Use at least one worker. Task exceptions are caught and the first recorded failure
// is rethrown by wait() after all work finishes. That wait() consumes the stored error.
// Stop submitting before destroying the pool. Call wait() from outside its workers:
// a running task is itself unfinished, so waiting for the whole pool would deadlock.
class ThreadPool
{
private:

	void decreaseUnfinishedTasks()
	{
		// Use the same mutex as push() and wait() for every access to the counter.
		std::unique_lock<std::mutex> lock(_mutex);
		_unfinished_tasks--;
		if (_unfinished_tasks == 0)
		{
			// Every caller waiting for an idle pool can now recheck its predicate.
			_condition.notify_all();
		}
	}

	std::jthread makeThreadHandler(ThreadQueue<Task>& queue) {
		return std::jthread{
			// The queue and pool must outlive this worker's captured references/pointer.
			[&queue, this] {
				while (true)
				{
					auto const entry = queue.pop();
					if (!entry)
					{
						return;
					}

					const Task& elem = *entry;

					std::exception_ptr taskError = nullptr;
					try
					{
						elem.task(elem.arguments);
					}
					catch (...)
					{
						taskError = std::current_exception();

						std::unique_lock<std::mutex> lock(_exception_mutex);
						if (!_exception)
						{
							_exception = taskError;
						}
					}

					if (elem.group)
					{
						elem.group->finishTask(taskError);
					}

					decreaseUnfinishedTasks();
				}
			}
		};
	}

public:

	ThreadPool(std::size_t numThreads)
	{
		if(numThreads == 0)
		{
			throw std::invalid_argument("Number of threads must be positive");
		}

		_threads.reserve(numThreads);
		try {
			// Create workers once; each worker repeatedly takes entries from the queue.
			for (std::size_t i = 0; i < numThreads; i++)
			{
				_threads.push_back(makeThreadHandler(_queue));
			}
		}
		catch (...) {
			_queue.close();
			throw;
		}
	}

	~ThreadPool()
	{
		_queue.close();
	}
	
	bool push(Task const& task)
	{

		std::unique_lock<std::mutex> lock(_mutex);

		if (task.group)
		{
			task.group->addTask();
		}

		try
		{
			_queue.push(task);
		}
		catch (...)
		{
			if (task.group)
			{
				task.group->finishTask();
			}
			throw;
		}


		_unfinished_tasks++;
		return true;
	}

	void dispatch( std::size_t elementCount, std::size_t batchSize, std::function<void(std::size_t, std::size_t)> operation, JobGroup* group = nullptr)
	{
		// This version treats both empty input and zero batch size as a no-op.
		if(elementCount == 0 || batchSize == 0)
		{
			return;
		}

		for(std::size_t i = 0; i < elementCount;)
		{
			// Each callback receives [i, end): begin is included, end is excluded.
			// The last batch may be smaller; subtract first to keep end <= elementCount.
			const std::size_t count = std::min(batchSize, elementCount - i);
			const std::size_t end = i + count;

			push({
				// Own a copy of the operation and range; the loop may finish before execution.
				// References captured inside operation still need to outlive the task.
				[operation, i, end](std::vector<Parameter> const&) {
					// The caller's operation loops over the elements in this range.
					operation(i, end);
				},
				{},
				group
			});

			i = end;
		}
	}

	void wait()
	{
		// An empty queue is not enough: removed tasks may still be executing.
		// With submissions paused, zero unfinished entries means all work has finished.
		std::unique_lock<std::mutex> lock(_mutex);
		// Waiting releases this mutex so workers can decrement and notify us.
		// If already idle, the predicate succeeds immediately. The pool stays reusable.
		_condition.wait(lock, [this] { return _unfinished_tasks == 0; });

		std::exception_ptr exception_copy;
		{
			std::unique_lock<std::mutex> lock(_exception_mutex);
			exception_copy = _exception;
			_exception = nullptr;
		}

		if (exception_copy)
		{
			std::rethrow_exception(exception_copy);
		}
	}

private:

	ThreadQueue<Task> _queue;
	// Completion uses separate synchronization from the queue's "work available" wait.
	std::condition_variable _condition;
	std::mutex _mutex;
	// Counts queued AND executing entries, including shutdown signals.
	int _unfinished_tasks = 0;

	std::exception_ptr _exception;
	std::mutex _exception_mutex;

	// Keep LAST: members are destroyed in reverse order, so workers join before
	// the queue, counter, mutex, and condition variable they access are destroyed.
	std::vector<std::jthread> _threads;


};

} // namespace JobKit
