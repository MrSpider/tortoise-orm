import asyncio
import collections
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar, cast

from tortoise.exceptions import ParamsError

current_transaction_map = collections.defaultdict(lambda: collections.deque())

if TYPE_CHECKING:  # pragma: nocoverage
    from tortoise.backends.base.client import BaseDBAsyncClient, TransactionContext

FuncType = Callable[..., Any]
F = TypeVar("F", bound=FuncType)


def get_connection(connection_name: Optional[str]) -> "BaseDBAsyncClient":
    from tortoise import Tortoise

    if not connection_name and len(Tortoise._connections) != 1:
        raise ParamsError(
            "You are running with multiple databases, so you should specify"
            f" connection_name: {list(Tortoise._connections.keys())}"
        )

    if not connection_name:
        connection_name = list(Tortoise._connections.keys())[0]

    if not asyncio.get_event_loop().is_running():
        return Tortoise._connections[connection_name]

    # Each asyncio task has its own transaction stack
    key = connection_name + str(id(asyncio.current_task()))
    if key in current_transaction_map and current_transaction_map[key]:
        return current_transaction_map[key][-1]

    return Tortoise._connections[connection_name]


def push_transaction(connection_name: str, connection: "BaseDBAsyncClient"):
    key = connection_name + str(id(asyncio.current_task()))
    current_transaction_map[key].append(connection)


def pop_transaction(connection_name: str):
    key = connection_name + str(id(asyncio.current_task()))
    current_transaction_map[key].pop()

    # Clean up map to save memory
    if not current_transaction_map[key]:
        current_transaction_map.pop(key)


def in_transaction(connection_name: Optional[str] = None) -> "TransactionContext":
    """
    Transaction context manager.

    You can run your code inside ``async with in_transaction():`` statement to run it
    into one transaction. If error occurs transaction will rollback.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """
    connection = get_connection(connection_name)
    return connection._in_transaction()


def atomic(connection_name: Optional[str] = None) -> Callable[[F], F]:
    """
    Transaction decorator.

    You can wrap your function with this decorator to run it into one transaction.
    If error occurs transaction will rollback.

    :param connection_name: name of connection to run with, optional if you have only
                            one db connection
    """

    def wrapper(func: F) -> F:
        @wraps(func)
        async def wrapped(*args, **kwargs):
            async with in_transaction(connection_name):
                return await func(*args, **kwargs)

        return cast(F, wrapped)

    return wrapper
