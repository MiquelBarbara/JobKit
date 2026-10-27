#pragma once

#include <exception>
#include <functional>
#include <stop_token>
#include <type_traits>
#include <utility>

namespace JobKit
{
	class TaskCancelled final : public std::exception
	{
	  public:
		const char* what() const noexcept override
		{
			return "JobKit task cancelled";
		}
	};

	inline void throwIfCancellationRequested(std::stop_token token)
	{
		if (token.stop_requested())
		{
			throw TaskCancelled{};
		}
	}

	// Adapt a token-taking operation to the ordinary zero-argument Task/submit API.
	// Accepted entries remain queued/count as unfinished until a worker handles them.
	// The operation must poll at safe checkpoints; cancellation never interrupts it.
	template <class Function> auto cancellable(std::stop_token token, Function&& function)
	{
		return [token, operation = std::forward<Function>(
		                   function)]() mutable -> std::invoke_result_t<std::decay_t<Function>&, std::stop_token>
		{
			using Result = std::invoke_result_t<std::decay_t<Function>&, std::stop_token>;
			throwIfCancellationRequested(token);
			if constexpr (std::is_void_v<Result>)
			{
				std::invoke(operation, std::stop_token(token));
				throwIfCancellationRequested(token);
			}
			else
			{
				Result result = std::invoke(operation, std::stop_token(token));
				throwIfCancellationRequested(token);
				return std::forward<Result>(result);
			}
		};
	}
} // namespace JobKit
