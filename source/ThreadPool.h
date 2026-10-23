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

class ThreadPool
{
private:

	void decreaseUnfinishedTasks()
	{
		std::unique_lock<std::mutex> lock(_mutex);
		_unfinished_tasks--;
		if (_unfinished_tasks == 0)
		{
			_condition.notify_all();
		}
	}

	std::jthread makeThreadHandler(ThreadQueue<Task>& queue) {
		return std::jthread{
			[&queue, this] {
				while (true)
				{
					auto const elem = queue.pop();
					switch (elem.type)
					{
					case TaskType::Execute:
						elem.task(elem.arguments);
						decreaseUnfinishedTasks();
						break;

					case TaskType::Stop:
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
		for (std::size_t i = 0; i < numThreads; i++)
		{
			_threads.push_back(makeThreadHandler(_queue));
		}
	}

	~ThreadPool()
	{
		const Task stop_task{ TaskType::Stop, {}, {} };
		for (std::size_t i = 0; i < _threads.size(); i++)
		{
			push(stop_task);
		}
	}
	
	bool push(Task const& task)
	{
		std::unique_lock<std::mutex> lock(_mutex);
		_unfinished_tasks++;
		_queue.push(task);
		return true;
	}

	void dispatch( std::size_t elementCount, std::size_t batchSize, std::function<void(std::size_t, std::size_t)> operation)
	{
		if(elementCount == 0 || batchSize == 0)
		{
			return;
		}

		for(std::size_t i = 0; i < elementCount; i += batchSize)
		{
			const std::size_t count = std::min(batchSize, elementCount - i);
			const std::size_t end = i + count;

			push({
				TaskType::Execute,
				[operation, i, end](std::vector<Parameter> const&) {
					operation(i, end);
				},
				{}
			});
		}
	}

	void wait()
	{
		std::unique_lock<std::mutex> lock(_mutex);
		_condition.wait(lock, [this] { return _unfinished_tasks == 0; });
	}

private:

	ThreadQueue<Task> _queue;
	std::condition_variable _condition;
	std::mutex _mutex;
	int _unfinished_tasks = 0;

	std::vector<std::jthread> _threads;
};