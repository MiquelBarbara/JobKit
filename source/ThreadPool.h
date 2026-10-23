#pragma once
#include <functional>
#include <thread>
#include <vector>
#include <cstddef>
#include <mutex>
#include <condition_variable>
#include <algorithm>

#include "ThreadQueue.h"
#include "Task.h"

// Current contract: use at least one worker and submit callables that do not throw.
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
					auto const elem = queue.pop();
					switch (elem.type)
					{
					case TaskType::Execute:
						// Execute without either mutex held so other tasks can run or submit.
						// An exception escaping this worker currently terminates the program.
						elem.task(elem.arguments);
						decreaseUnfinishedTasks();
						break;

					case TaskType::Stop:
						// push() counts Stop entries too, so balance the counter before exiting.
						decreaseUnfinishedTasks();
						return;
					}
				}
			}
		};
	}

public:

	ThreadPool(std::size_t numThreads)
	{
		// Create workers once; each worker repeatedly takes entries from the queue.
		for (std::size_t i = 0; i < numThreads; i++)
		{
			_threads.push_back(makeThreadHandler(_queue));
		}
	}

	~ThreadPool()
	{
		// One Stop entry per worker. FIFO order puts these after already queued work.
		// Workers must not submit more work during shutdown, as it could follow Stop.
		const Task stop_task{ TaskType::Stop, {}, {} };
		for (std::size_t i = 0; i < _threads.size(); i++)
		{
			push(stop_task);
		}
		// After this body, destroying _threads joins workers before queue/state cleanup.
		// jthread's stop request alone does not wake our queue; Stop entries do that.
	}
	
	bool push(Task const& task)
	{
		std::unique_lock<std::mutex> lock(_mutex);
		// Register work BEFORE publishing it: a worker may execute it immediately.
		// TODO: roll back this increment if copying/enqueueing the task throws.
		_unfinished_tasks++;
		_queue.push(task);
		return true;
	}

	void dispatch( std::size_t elementCount, std::size_t batchSize, std::function<void(std::size_t, std::size_t)> operation)
	{
		// This version treats both empty input and zero batch size as a no-op.
		if(elementCount == 0 || batchSize == 0)
		{
			return;
		}

		// TODO: advance with i = end to avoid increment overflow near size_t's limit.
		for(std::size_t i = 0; i < elementCount; i += batchSize)
		{
			// Each callback receives [i, end): begin is included, end is excluded.
			// The last batch may be smaller; subtract first to keep end <= elementCount.
			const std::size_t count = std::min(batchSize, elementCount - i);
			const std::size_t end = i + count;

			push({
				TaskType::Execute,
				// Own a copy of the operation and range; the loop may finish before execution.
				// References captured inside operation still need to outlive the task.
				[operation, i, end](std::vector<Parameter> const&) {
					// The caller's operation loops over the elements in this range.
					operation(i, end);
				},
				{}
			});
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
	}

private:

	ThreadQueue<Task> _queue;
	// Completion uses separate synchronization from the queue's "work available" wait.
	std::condition_variable _condition;
	std::mutex _mutex;
	// Counts queued AND executing entries, including shutdown signals.
	int _unfinished_tasks = 0;

	// Keep LAST: members are destroyed in reverse order, so workers join before
	// the queue, counter, mutex, and condition variable they access are destroyed.
	std::vector<std::jthread> _threads;
};
