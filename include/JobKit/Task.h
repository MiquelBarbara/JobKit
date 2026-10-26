#pragma once
#include <variant>
#include <functional>
#include <vector>
#include <string>

namespace JobKit
{

// Data-only task types stay in the public header.
// Explicit runtime arguments supported by this learning implementation.
using Parameter = std::variant<int, float, std::string, bool>;

class JobGroup; // Declares the type used by Task's non-owning group pointer.

struct Task
{
	// A lambda can also carry data via captures, as dispatch does for its range.
	// Owning the callable does not extend the lifetime of objects captured by reference.
	std::function<void(std::vector<Parameter>)> task;
	// Passed by value to the callable, so executing a task currently copies this vector.
	std::vector<Parameter> arguments;

	JobGroup* group = nullptr; // Optional: a task can belong to a JobGroup for wait() synchronization.
};

} // namespace JobKit
