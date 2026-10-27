#pragma once
#include <functional>

namespace JobKit
{

class JobGroup; // Declares the type used by Task's non-owning group pointer.

struct Task
{
	// Owning the callable does not extend the lifetime of objects captured by reference.
	std::function<void()> task;

	JobGroup* group = nullptr; // Optional: a task can belong to a JobGroup for wait() synchronization.
};

} // namespace JobKit
