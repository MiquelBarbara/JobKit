#pragma once
#include <vector>
#include <memory>
#include <unordered_map>
#include <cstddef>
#include <stdexcept>
#include <utility>
#include <exception>

#include "JobNode.h"
#include "JobGroup.h"
#include "ThreadPool.h"

namespace JobKit
{

class JobGraph
{
private:
	struct ValidationState
	{
		std::unordered_map<const JobNode*, std::size_t> remaining;
		std::vector<const JobNode*> readyNodes;
	};

	ValidationState buildValidationState() const
	{
		ValidationState state;
		// Build temporary counts and initial ready list
		for (const auto& node : _nodes)
		{
			const auto count = node->prerequisiteCounter.load();
			state.remaining.emplace(node.get(), count);
			if (count == 0)
			{
				state.readyNodes.push_back(node.get());
			}
		}
		return state;
	}

	void validateLinks(const std::unordered_map<const JobNode*, std::size_t>& remaining) const
	{
		std::unordered_map<const JobNode*, std::size_t> incoming;
		for (const auto& node : _nodes)
		{
			incoming.emplace(node.get(), 0);
		}

		// Check every dependency destination
		for (const auto& node : _nodes)
		{
			for (const JobNode* dependent : node->dependents)
			{
				if (!remaining.contains(dependent))
				{
					throw std::invalid_argument("A dependency points outside this graph");
				}
				++incoming.at(dependent);
			}
		}

		// A prerequisite outside this graph contributes to the stored counter,
		// but cannot appear in the incoming links counted from our owned nodes.
		for (const auto& node : _nodes)
		{
			if (incoming.at(node.get()) != remaining.at(node.get()))
			{
				throw std::invalid_argument("A prerequisite is outside this graph");
			}
		}
	}

	void validateAcyclic(ValidationState& state) const
	{
		std::size_t visitedCount = 0;
		while (!state.readyNodes.empty())
		{
			const JobNode* node = state.readyNodes.back();
			state.readyNodes.pop_back();
			visitedCount++;

			for (JobNode* dependent : node->dependents)
			{	
				auto& counter = state.remaining.at(dependent);
				if (--counter == 0)
				{
					state.readyNodes.push_back(dependent);
				}
			}
		}

		//Check visited count
		if (visitedCount != _nodes.size())
		{
			throw std::runtime_error("JobGraph contains a cycle");
		}
	}

	std::vector<JobNode*> prepareSubmission()
	{
		if (_submitted)
		{
			throw std::runtime_error("Cannot submit a JobGraph more than once");
		}

		validate();
		std::vector<JobNode*> roots;
		for (const auto& node : _nodes)
		{
			if (node->prerequisiteCounter.load() == 0)
			{
				roots.push_back(node.get());
			}

		}

		_submitted = true;
		for (const auto& node : _nodes)
		{
			node->_sealed = true;
			_completion.addTask();


			if (node->task.group)
			{
				node->task.group->addTask();
			}
		}

		return roots;
	}

	void executeNode(ThreadPool& pool, JobNode& node) 
	{
		node.state.store(JobState::Running);
		JobState result = JobState::Succeeded;
		std::exception_ptr error = nullptr;

		try 
		{
			node.task.task(node.task.arguments);
		}
		catch (...)
		{
			result = JobState::Failed;
			error = std::current_exception();
		}

		finishNode(pool, node, result, error);
	}

	void finishNode(ThreadPool& pool, JobNode& node, JobState result, std::exception_ptr error = nullptr) 
	{
		node.state.store(result);
		for (const auto& dependent : node.dependents)
		{
			if (result == JobState::Failed || result == JobState::Skipped)
			{
				dependent->failedPrerequisite.store(true);
			}

			auto& counter = dependent->prerequisiteCounter;
			if (counter.fetch_sub(1) == 1)
			{
				if (dependent->failedPrerequisite.load())
				{
					finishNode(pool, *dependent, JobState::Skipped);
				}
				else
				{
					scheduleNode(pool, *dependent);
				}
			}
		}


		if (node.task.group)
		{
			node.task.group->finishTask(error);
		}

		_completion.finishTask(error);
	}

	void scheduleNode(ThreadPool& pool, JobNode& node)
	{
		try
		{
			pool.push({
				[this, &pool, &node](std::vector<Parameter> const&) {
					executeNode(pool, node);
				},
				{},
				nullptr
				});

		}
		catch (...)
		{
			finishNode(pool, node, JobState::Failed, std::current_exception());
		}
	}

public:
	JobNode& createJob(Task task)
	{
		if (_submitted)
		{
			throw std::runtime_error("Cannot add jobs after submission");
		}
		_nodes.push_back(std::make_unique<JobNode>(std::move(task)));
		return *_nodes.back();
	}

	void submit(ThreadPool& pool)
	{
		auto roots = prepareSubmission();
		for (JobNode* node : roots)
		{
			scheduleNode(pool, *node);
		}
	}

	void wait()
	{
		_completion.wait();
	}


	void validate() const
	{
		auto state = buildValidationState();
		validateLinks(state.remaining);
		validateAcyclic(state);
	}
		
private:
	bool _submitted = false;

	JobGroup _completion;
	std::vector<std::unique_ptr<JobNode>> _nodes;
};

} // namespace JobKit
