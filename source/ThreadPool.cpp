#include <JobKit/ThreadPool.h>
#include <JobKit/JobGroup.h>
#include <algorithm>
#include <stdexcept>

namespace JobKit
{

thread_local bool ThreadPool::_isWorkerThread = false;

void ThreadPool::decreaseUnfinishedTasks()
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

std::jthread ThreadPool::makeThreadHandler(std::size_t workerIndex)
{
	return std::jthread{
		// The queue and pool must outlive this worker's captured references/pointer.
		[this, workerIndex] {
				_isWorkerThread = true;
			while (true)
			{
				auto const entry = _queue.pop();
				if (!entry)
				{
					return;
				}

				const Task& elem = entry->task;
				TaskTiming timing;
				if (entry->observed)
				{
					timing = {workerIndex, elem.group, entry->queuedAt, JobClock::now(), {}};
					_observer->onTaskStarted(timing);
				}

				std::exception_ptr taskError = nullptr;
				try
				{
					elem.task();
				}
				catch (...)
				{
					taskError = std::current_exception();
				}
				if (entry->observed)
				{
					timing.finishedAt = JobClock::now();
					_observer->onTaskFinished(timing);
				}
				if (taskError)
				{
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

ThreadPool::ThreadPool(std::size_t numThreads, std::shared_ptr<TaskObserver> observer) : _observer(std::move(observer))
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
			_threads.push_back(makeThreadHandler(i));
		}
	}
	catch (...) {
		_queue.close();
		throw;
	}
}

ThreadPool::~ThreadPool()
{
	_queue.close();
}

bool ThreadPool::push(Task const& task)
{

	std::unique_lock<std::mutex> lock(_mutex);

	if (task.group)
	{
		task.group->addTask();
	}

	try
	{
		const bool observed = _observer && _observerEnabled.load(std::memory_order_relaxed);
		ScheduledTask entry{task, observed ? JobClock::now() : JobClock::time_point{}, observed};
		_queue.push(std::move(entry));
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

void ThreadPool::dispatch(std::size_t elementCount, std::size_t batchSize, std::function<void(std::size_t, std::size_t)> operation, JobGroup* group)
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
			[operation, i, end] {
				// The caller's operation loops over the elements in this range.
				operation(i, end);
			},
			group
		});

		i = end;
	}
}

void ThreadPool::wait()
{
	if(isWorkerThread())
	{
		throw std::logic_error("ThreadPool::wait() must not be called from a worker thread");
	}

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

bool ThreadPool::isWorkerThread() noexcept
{
	return _isWorkerThread;
}

void ThreadPool::setObserverEnabled(bool enabled) noexcept
{
	_observerEnabled.store(enabled, std::memory_order_relaxed);
}
std::size_t ThreadPool::unfinishedTaskCount()
{
	std::lock_guard lock(_mutex);
	return static_cast<std::size_t>(_unfinished_tasks);
}

} // namespace JobKit
