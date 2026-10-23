#pragma once
#include <functional>
#include <thread>
#include <vector>
#include <cstddef>
#include "ThreadQueue.h"
#include "Task.h"

class ThreadPool
{
public:

	std::jthread makeThreadHandler(ThreadQueue<Task>& queue) {
		return std::jthread{
			[&queue] {
				while (true)
				{
					auto const elem = queue.pop();
					switch (elem.type)
					{
					case TaskType::Execute:
						elem.task(elem.arguments);
						break;

					case TaskType::Stop:
						return;
					}
				}
			}
		};
	}

	ThreadPool(size_t numThreads)
	{
		for (size_t i = 0; i < numThreads; i++)
		{
			_threads.push_back(makeThreadHandler(_queue));
		}
	}

	~ThreadPool()
	{
		const Task stop_task{ TaskType::Stop, {}, {} };
		for (size_t i = 0; i < _threads.size(); i++)
		{
			push(stop_task);
		}
	}
	
	bool push(Task const& task)
	{
		_queue.push(task);
		return true;
	}


private:
	ThreadQueue<Task> _queue;
	std::vector<std::jthread> _threads;
};