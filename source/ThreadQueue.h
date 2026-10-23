#pragma once
#include <queue>
#include <atomic>
#include <mutex>
#include <condition_variable>

template <typename T>
class ThreadQueue
{
public:
	void push(T const& task) 
	{
		std::lock_guard<std::mutex> lock(_mutex);
		_queue.push(task);
		_condition.notify_one();
	}

	T pop()
	{
		std::unique_lock<std::mutex> lock(_mutex);
		_condition.wait(lock, [this] { return !_queue.empty(); });
		T task = _queue.front();
		_queue.pop();
		return task;
	}

	bool empty() const
	{
		std::lock_guard<std::mutex> lock(_mutex);
		return _queue.empty();
	}

private:
	std::queue<T> _queue;
	std::mutex _mutex;
	std::condition_variable _condition;
};