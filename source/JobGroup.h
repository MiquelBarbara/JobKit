#pragma once
#include <mutex>
#include <condition_variable>
#include <exception>

namespace JobKit
{

class ThreadPool;
class JobGraph;

class JobGroup
{
public:
	friend class ThreadPool;
	friend class JobGraph;

	void wait()
	{
		std::unique_lock<std::mutex> lock(_mutex);
		_condition.wait(lock, [this] { return _unfinishedCount == 0; });

		std::exception_ptr exception_copy;
		exception_copy = _exception;
		_exception = nullptr;

		lock.unlock();

		if (exception_copy)
		{
			std::rethrow_exception(exception_copy);
		}
	}
private:
	void addTask()
	{
		std::unique_lock<std::mutex> lock(_mutex);
		_unfinishedCount++;
	}

	void finishTask(std::exception_ptr exception = nullptr)
	{
		std::unique_lock<std::mutex> lock(_mutex);
		if (exception && !_exception)
		{
			_exception = exception;
		}

		_unfinishedCount--;
		if (_unfinishedCount == 0)
		{
			_condition.notify_all();
		}
	}
private:
	int _unfinishedCount = 0;
	std::mutex _mutex;
	std::condition_variable _condition;

	std::exception_ptr _exception;
};

} // namespace JobKit
