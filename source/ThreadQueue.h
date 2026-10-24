#pragma once
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <utility>

template <typename T>
// Shared FIFO: the mutex protects queue access, not the data used by a task.
class ThreadQueue
{
public:
	void push(T const& task) 
	{
		std::lock_guard<std::mutex> lock(_mutex);
		_queue.push(task);
		// Publish the entry before waking one consumer. Notification under the lock
		// is valid; the consumer must reacquire the mutex before taking the entry.
		_condition.notify_one();
	}

	T pop()
	{
		std::unique_lock<std::mutex> lock(_mutex);
		// wait() releases the mutex while sleeping and reacquires it before checking
		// the predicate. Rechecking handles spurious wakeups and competing consumers.
		_condition.wait(lock, [this] { return !_queue.empty(); });
		// Checking and removing the front entry happen under the same lock.
		T task = std::move(_queue.front());
		_queue.pop();
		// Returning releases the lock; the worker executes its local task afterward.
		return task;
	}



private:
	std::queue<T> _queue;
	mutable std::mutex _mutex;
	std::condition_variable _condition;
};
