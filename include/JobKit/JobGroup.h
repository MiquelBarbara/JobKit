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

	void wait();

private:
	void addTask();
	void finishTask(std::exception_ptr exception = nullptr);

	int _unfinishedCount = 0;
	std::mutex _mutex;
	std::condition_variable _condition;
	std::exception_ptr _exception;
};

} // namespace JobKit
