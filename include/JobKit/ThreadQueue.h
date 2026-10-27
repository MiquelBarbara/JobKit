#pragma once
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <utility>
#include <optional>
#include <stdexcept>

namespace JobKit
{

// Template definitions must remain visible to translation units using the queue.
template <typename T>
// Shared FIFO: the mutex protects queue access, not the data used by a task.
class ThreadQueue
{
public:
	void push(T const& task) 
	{
		pushEntry(task);
	}
	void push(T&& task)
	{
		pushEntry(std::move(task));
	}
private:
	template <class Entry> void pushEntry(Entry&& task)
	{
		std::lock_guard<std::mutex> lock(_mutex);
		if(_closed)
		{
			throw std::runtime_error("Cannot push to a closed queue");
		}
		_queue.push(std::forward<Entry>(task));
		// Publish the entry before waking one consumer. Notification under the lock
		// is valid; the consumer must reacquire the mutex before taking the entry.
		_condition.notify_one();
	}

public:
	std::optional<T> pop()
	{
		std::unique_lock<std::mutex> lock(_mutex);
		// wait() releases the mutex while sleeping and reacquires it before checking
		// the predicate. Rechecking handles spurious wakeups and competing consumers.
		_condition.wait(lock, [this] { return !_queue.empty() || _closed; });
		// Checking and removing the front entry happen under the same lock.
		if (!_queue.empty())
		{
			T task = std::move(_queue.front());
			_queue.pop();
			// Returning releases the lock; the worker executes its local task afterward.
			return task;
		}
		return std::nullopt;
	}

	void close()
	{
		std::lock_guard<std::mutex> lock(_mutex);
		_closed = true;
		_condition.notify_all();
	}

private:
	std::queue<T> _queue;
	mutable std::mutex _mutex;
	std::condition_variable _condition;
	bool _closed = false;
};

} // namespace JobKit
