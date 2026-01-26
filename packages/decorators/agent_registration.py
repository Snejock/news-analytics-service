from typing import Awaitable, Callable, Any
import asyncio

agent_fn = Callable[[Any], Awaitable[None]]
agent_registry: dict[str, agent_fn] = {}


def agent_registration(agent_nm: str):
    """Декоратор, который регистрирует асинхронный агент под указанным именем."""
    if not isinstance(agent_nm, str) or not agent_nm.strip():
        raise ValueError("Agent name must be a non-empty string")
    agent_nm = agent_nm.strip()

    def decorator(fn: agent_fn) -> agent_fn:
        if not asyncio.iscoroutinefunction(fn):
            raise TypeError(
                f"Agent '{agent_nm}' must be an async function (async def)"
            )
        if agent_nm in agent_registry:
            raise KeyError(f"Agent '{agent_nm}' is already registered")

        agent_registry[agent_nm] = fn
        return fn

    return decorator
