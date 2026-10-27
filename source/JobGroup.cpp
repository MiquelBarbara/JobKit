#include <JobKit/JobGroup.h>
#include <JobKit/ThreadPool.h>
#include <stdexcept>

namespace JobKit
{

void JobGroup::wait()
{
	if(ThreadPool::isWorkerThread())
	{
		throw std::logic_error("JobGroup::wait() must not be called from a worker thread");
	}

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

void JobGroup::waitUntilComplete()
{
	std::unique_lock<std::mutex> lock(_mutex);
	_condition.wait(lock, [this] { return _unfinishedCount == 0; });
}

void JobGroup::addTask()
{
	std::unique_lock<std::mutex> lock(_mutex);
	_unfinishedCount++;
}

void JobGroup::finishTask(std::exception_ptr exception)
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

} // namespace JobKit
