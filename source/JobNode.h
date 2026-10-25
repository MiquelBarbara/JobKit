#pragma once
#include "Task.h"
#include <atomic>
#include <vector>
#include <cstddef>
#include <stdexcept>
#include <utility>

enum class JobState
{
	Pending,
	Running,
	Succeeded,
	Failed,
	Skipped
};

class JobNode
{
public:
	friend class JobGraph;

	explicit JobNode(Task task)
		: task(std::move(task))
	{
	}

	void dependsOn(JobNode& prerequisite)
	{
		if (_sealed || prerequisite._sealed)
		{
			throw std::logic_error("Cannot add dependencies after submission");
		}

		if (&prerequisite == this)
		{
			throw std::invalid_argument("A job cannot depend on itself");
		}

		prerequisite.dependents.push_back(this);
		prerequisiteCounter++;
	}

private:
	Task task;
	bool _sealed = false;
	std::atomic<JobState> state{ JobState::Pending };
	std::atomic<bool> failedPrerequisite{ false };

	std::atomic<std::size_t> prerequisiteCounter{ 0 };
	std::vector<JobNode*> dependents;
};