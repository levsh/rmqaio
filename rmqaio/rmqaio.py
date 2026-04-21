import asyncio
import gettext
import itertools
import logging
import weakref

from asyncio import FIRST_COMPLETED, CancelledError, Lock, create_task, gather, get_running_loop, sleep, wait, wait_for
from collections.abc import Hashable, MutableSequence
from contextlib import suppress
from dataclasses import dataclass, field
from enum import StrEnum
from functools import partial, wraps
from os import environ
from pathlib import Path
from ssl import SSLContext
from typing import Any, Awaitable, Callable, Generic, Iterable, Literal, Protocol, TypeAlias, TypeVar, cast, overload
from uuid import uuid4

import aiormq
import aiormq.exceptions
import yarl


gettext.bindtextdomain(
    "rmqaio",
    localedir=Path(__file__).parent / "locales",
)
gettext.textdomain("rmqaio")
_ = gettext.gettext


logger = logging.getLogger("rmqaio")

callback_logger = logging.getLogger("rmqaio.callback")


BasicProperties = aiormq.spec.Basic.Properties

Number = int | float
"""Numeric type alias for integers or floats."""


class RmqAioError(Exception):
    pass


class ConnectionInvalidStateError(RmqAioError):
    pass


class OperationError(RmqAioError):
    pass


def _env_var_as_bool(env_var_name: str, default: bool = False) -> bool:
    """
    Retrieve an environment variable as a boolean.

    Args:
        env_var_name: The environment variable name.
        default: The default value if the environment variable is not set.

    Returns:
        The boolean value of the environment variable.
    """
    value = environ.get(env_var_name)
    if value is None:
        return default
    if value.lower() in ("1", "true", "yes", "y"):
        return True
    if value.lower() in ("0", "false", "no", "n"):
        return False
    return default


def _env_var_as_int(env_var_name: str, default: int) -> int:
    """
    Retrieve an environment variable as an integer.

    Args:
        env_var_name: The environment variable name.
        default: The default value if the environment variable is not set.

    Returns:
        The integer value of the environment variable.

    Raises:
        ValueError: If the environment variable is set to an invalid value.
    """
    value = environ.get(env_var_name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(_("invalid value '{}' for environment variable {}").format(value, env_var_name))


def _as_int_or_none(value) -> int | None:
    """
    Convert a value to int or return None if conversion fails.

    Args:
        value: The value to convert to int.

    Returns:
        The int value if conversion succeeds, None otherwise.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return


@dataclass
class Config:
    """
    Configuration for rmqaio library.

    Attributes:
        log_sanitize: Flag indicating whether to sanitize logs by replacing
            user data with `<hidden>`.
        log_data_truncate_size: Maximum size of data to log before truncation.
    """

    log_sanitize: bool = field(default_factory=lambda: _env_var_as_bool("RMQAIO_LOG_SANITIZE", True))
    """Flag indicating whether to sanitize logs by replacing user data with `<hidden>`."""

    log_data_truncate_size: int = field(default_factory=lambda: _env_var_as_int("RMQAIO_LOG_DATA_TRUNCATE_SIZE", 10000))
    """Maximum size of data to log before truncation."""


config = Config()


class Repeat:
    """
    Represents a fixed delay value for retry operations.

    Supports hashing and equality comparison, making it suitable for use as
    a dictionary key or in sets.

    Attributes:
        value: The value to repeat.

    Examples:
        >>> repeat = Repeat(5)
        >>> for delay in repeat:
        ...     print(delay)
    """

    def __init__(self, value: Number):
        """
        Initialize repeat with a value.

        Args:
            value: The value to repeat.
        """
        self.value = value

    def __str__(self):
        return f"{self.__class__.__name__}({self.value})"

    def __iter__(self):
        return iter(itertools.repeat(self.value))

    def __hash__(self):
        return hash((self.__class__, float(self.value)))

    def __eq__(self, other):
        return other.__class__ == Repeat and self.value == other.value

    def __ne__(self, other):
        return other.__class__ != Repeat or self.value != other.value


Delay: TypeAlias = list[Number] | Repeat


@dataclass(slots=True, frozen=True)
class RetryPolicy:
    """
    Defines retry policy for handling exceptions and retries.

    Attributes:
        delays: A sequence of delays (in seconds) for attempts.
        exc_filter: A tuple of exception types or a callable function to
            filter which exceptions should be retried.
    """

    delays: Delay = field(default_factory=lambda: Repeat(5))
    exc_filter: tuple[type[Exception], ...] | Callable[[Exception], bool] = (
        asyncio.TimeoutError,
        ConnectionError,
        aiormq.exceptions.AMQPConnectionError,
    )

    def __hash__(self):
        return hash(
            (
                self.__class__,
                self.delays if isinstance(self.delays, Repeat) else tuple(self.delays or ()),
                self.exc_filter,
            )
        )

    def is_retryable(self, e: Exception) -> bool:
        """
        Determine if an exception is retryable.

        Args:
            e: The exception to check.

        Returns:
            `True` if the exception is retryable, `False` otherwise.
            Delegates to `exc_filter` callable if provided.
        """
        if callable(self.exc_filter):
            return self.exc_filter(e)

        return isinstance(e, self.exc_filter)


def retry(
    retry_policy: RetryPolicy,
    *,
    msg: str | None = None,
    on_error: Callable[[Exception], Awaitable] | None = None,
):
    """
    Create a retry decorator for async functions.

    The decorated coroutine will be retried using the provided delay
    sequence if the raised exception matches `exc_filter`.

    Args:
        retry_policy: Retry policy.
        msg: Optional message to log before retrying. If not provided,
            the function object is logged.
        on_error: Optional async callback executed after a retryable
            exception is caught and before sleeping.

    Returns:
        Decorator for async functions.
    """

    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwds):
            timeouts = iter(retry_policy.delays)
            attempt = 0
            while True:
                try:
                    return await fn(*args, **kwds)
                except Exception as e:
                    if not retry_policy.is_retryable(e):
                        raise
                    try:
                        t = next(timeouts)
                        attempt += 1
                        logger.warning(
                            _("%s (%s %s) retry(%s) in %s second(s)"),
                            msg or fn,
                            e.__class__,
                            e,
                            attempt,
                            t,
                        )
                        if on_error:
                            await on_error(e)
                        await sleep(t)
                    except StopIteration:
                        raise e  # raise original exception instead of StopIteration

        return wrapper

    return decorator


async def _wait_first_and_cancel_pending(
    tasks: list[asyncio.Task],
    timeout: Number | None = None,
) -> tuple[set[asyncio.Task], set[asyncio.Task]]:
    """
    Wait until the first task completes, then cancel the remaining tasks.

    Args:
        tasks: Tasks to wait on.
        timeout: Optional timeout in seconds.

    Returns:
        A tuple of completed and pending tasks returned by `asyncio.wait`.
    """
    done, pending = await wait(tasks, timeout=timeout, return_when=FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    if pending:
        await gather(*pending, return_exceptions=True)
    return done, pending


class ConnectionState(StrEnum):
    """Enum for representing the state of a connection."""

    INITIAL = "initial"
    """Connection is created and ready to open."""

    CONNECTING = "connecting"
    """Connection is in the process of being established."""

    CONNECTED = "connected"
    """Connection is established and operational."""

    REFRESHING = "refreshing"
    """Connection is in the process of being refreshed."""

    CLOSING = "closing"
    """Connection is in the process of being closed."""

    CLOSED = "closed"
    """Connection is closed."""


class ConnectionProtocol(Protocol):
    """Protocol describing a RabbitMQ connection interface."""

    @property
    def url(self) -> str: ...

    @property
    def is_open(self) -> bool: ...

    @property
    def is_closed(self) -> bool: ...

    async def open(self, timeout: Number | None = None): ...

    async def refresh(self, timeout: Number | None = None): ...

    async def close(self, timeout: Number | None = None): ...

    async def new_channel(self, timeout: Number | None = None) -> aiormq.abc.AbstractChannel: ...

    async def channel(self, timeout: Number | None = None) -> aiormq.abc.AbstractChannel: ...

    def set_callback(
        self,
        name: str,
        callback: Callable[[ConnectionState, ConnectionState], Awaitable],
    ): ...

    async def remove_callback(self, name: str): ...


class Connection:
    """
    RabbitMQ connection with automatic reconnection on connection loss.

    Attributes:
        id: Connection Id.
        url: Connection URL to RabbitMQ.
        ssl_context: SSL context for TLS connections.
        open_retry_policy: Policy for first connection attempts.
        reopen_retry_policy: Policy for reconnection attempts.
        is_open: Whether connection is open and operational.
        is_closed: Whether connection is closed.

    Examples:
        >>> conn = Connection("amqp://localhost")
        >>> await conn.open()
        >>> channel = await conn.channel()
        >>> await conn.close()
    """

    def __init__(
        self,
        url: str,
        ssl_context: SSLContext | None = None,
        open_retry_policy: RetryPolicy | None = None,
        reopen_retry_policy: RetryPolicy | None = None,
    ):
        """
        Initialize connection.

        Args:
            url: AMQP connection URL (e.g., "amqp://localhost").
            ssl_context: Optional SSL context for TLS connections.
            open_retry_policy: Reconnection policy for handling first connection errors.
            reopen_retry_policy: Reconnection policy for handling reconnection errors.
        """
        self._id = uuid4().hex[-4:]
        self._url = url
        self._ssl_context = ssl_context
        self._open_retry_policy = open_retry_policy
        self._reopen_retry_policy = reopen_retry_policy or RetryPolicy()

        self._connect_timeout = self._extract_connect_timeout(url)
        self._state = ConnectionState.INITIAL
        self._conn: aiormq.Connection | None = None
        self._channel: aiormq.abc.AbstractChannel | None = None
        self._connected_event = asyncio.Event()
        self._closed_event = asyncio.Event()
        self._loop_task: asyncio.Task | None = None
        self._exc: BaseException | Exception | None = None
        self._callbacks: dict[str, Callable[[ConnectionState, ConnectionState], Awaitable]] = {}

    def _extract_connect_timeout(self, url: str) -> int | None:
        """
        Extract connection timeout from URL.

        Args:
            url: AMQP connection URL.

        Returns:
            Connection timeout in seconds, or None if not specified.
        """
        value = _as_int_or_none(yarl.URL(url).query.get("connection_timeout"))
        if value:
            value /= 1000  # milliseconds to seconds
        return cast(int, value)

    def __str__(self):
        url = yarl.URL(self._url)
        if url.port:
            return f"{self.__class__.__name__}[{url.host}:{url.port}][{self._id}]"
        return f"{self.__class__.__name__}[{url.host}][{self._id}]"

    def __repr__(self):
        return self.__str__()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @property
    def id(self) -> str:
        """Connection Id."""
        return self._id

    @property
    def url(self) -> str:
        """Connection URL."""
        return self._url

    @property
    def ssl_context(self) -> SSLContext | None:
        """SSL context for secure connections."""
        return self._ssl_context

    @property
    def open_retry_policy(self) -> RetryPolicy | None:
        """Reconnection policy for handling first connection errors."""
        return self._open_retry_policy

    @property
    def reopen_retry_policy(self) -> RetryPolicy | None:
        """Reconnection policy for handling reconnection errors."""
        return self._reopen_retry_policy

    @property
    def is_open(self) -> bool:
        """Whether connection is open and operational."""
        return self._state == ConnectionState.CONNECTED

    @property
    def is_closed(self) -> bool:
        """Whether connection is closed."""
        return self._state in [ConnectionState.CLOSED, ConnectionState.CLOSING]

    async def _start_loop(self):
        if self._state != ConnectionState.INITIAL:
            raise ConnectionInvalidStateError(_("invalid connection state"))
        if not self._loop_task:
            self._loop_task = asyncio.create_task(self._loop())

    async def _wait_open(self, timeout: Number | None = None):
        done = (
            await _wait_first_and_cancel_pending(
                [
                    create_task(self._connected_event.wait()),
                    create_task(self._closed_event.wait()),
                    create_task(wait([cast(asyncio.Task, self._loop_task)])),
                ],
                timeout=timeout,
            )
        )[0]
        if not done and self._loop_task:
            self._loop_task.cancel()
            with suppress(CancelledError):
                await self._loop_task

    def _check_open_result(self):
        if self._exc:
            raise self._exc
        if self._state == ConnectionState.CONNECTED:
            return
        raise ConnectionInvalidStateError(_("invalid connection state"))

    async def _start_refresh(self):
        await self._set_state(ConnectionState.REFRESHING)
        await cast(aiormq.Connection, self._conn).close()

    async def _wait_refresh(self, timeout: Number | None = None):
        await asyncio.wait_for(self._connected_event.wait(), timeout)

    async def _start_close(self):
        await self._set_state(ConnectionState.CLOSING)
        if self._loop_task:
            self._loop_task.cancel()

    async def _wait_close(self, timeout: Number | None = None):
        if self._loop_task:
            with suppress(CancelledError):
                await asyncio.wait_for(self._loop_task, timeout)

    async def open(self, timeout: Number | None = None):
        """
        Open connection to RabbitMQ.

        Establishes connection to RabbitMQ broker. If connection is lost,
        automatically attempts to reconnect based on retry policy.

        Args:
            timeout: Operation timeout in seconds.
        """
        if self._state == ConnectionState.CONNECTED:
            return

        if self._state in (ConnectionState.CONNECTING, ConnectionState.REFRESHING):
            await self._wait_open(timeout=timeout)
            self._check_open_result()
            return

        if self._state in [ConnectionState.CLOSING, ConnectionState.CLOSED]:
            raise ConnectionInvalidStateError("can not reopen closed connection")

        await self._start_loop()

        await self._wait_open(timeout=timeout)
        self._check_open_result()

    async def refresh(self, timeout: Number | None = None):
        """
        Refresh the underlying connection by reopening it.

        Args:
            timeout: Operation timeout in seconds.
        """
        if self._state in [ConnectionState.CLOSING, ConnectionState.CLOSED]:
            raise ConnectionInvalidStateError("can not refresh closed connection")

        if self._state != ConnectionState.CONNECTED:
            return

        await self._start_refresh()
        await self._wait_refresh(timeout=timeout)

        logger.info(_("%s refreshed"), self)

    async def close(self, timeout: Number | None = None):
        """
        Gracefully closes the connection and all associated channels.

        Args:
            timeout: Operation timeout in seconds.
        """
        if self._state in (ConnectionState.CLOSING, ConnectionState.CLOSED):
            return

        await self._start_close()
        await self._wait_close(timeout=timeout)

        logger.info(_("%s closed"), self)

    async def new_channel(self, timeout: Number | None = None) -> aiormq.abc.AbstractChannel:
        """
        Create a new channel.

        Args:
            timeout: Operation timeout in seconds.

        Returns:
            A new RabbitMQ channel.
        """
        if not self.is_open:
            await self.open()
        return await cast(aiormq.abc.AbstractConnection, self._conn).channel(timeout=timeout)

    async def channel(self, timeout: Number | None = None) -> aiormq.abc.AbstractChannel:
        """
        Get or create a channel.

        Args:
            timeout: Operation timeout in seconds.

        Returns:
            An open RabbitMQ channel.
        """
        if self._channel is None or self._channel.is_closed:
            await self.open()
            self._channel = await cast(aiormq.abc.AbstractConnection, self._conn).channel(timeout=timeout)
        return self._channel

    def set_callback(
        self,
        name: str,
        callback: Callable[[ConnectionState, ConnectionState], Awaitable],
    ):
        """
        Set a callback for connection events.

        Args:
            name: Unique name to identify the callback.
            callback: Callable to execute on event.

        If a callback with this name is already registered, it will be overridden.
        """
        self._callbacks[name] = callback

    async def remove_callback(self, name: str):
        """
        Remove a callback.

        Args:
            name: Callback name to remove.
        """
        self._callbacks.pop(name, None)

    async def _execute_callbacks(self, state_from: ConnectionState, state_to: ConnectionState):
        """
        Execute all registered callbacks.

        Args:
            state_from: Connection state before changing.
            state_to: Connection state after changing.
        """
        for name, callback in list(self._callbacks.items()):
            callback_logger.debug(
                _("%s execute callback[name=%s] %s -> %s"),
                self,
                name,
                state_from,
                state_to,
            )
            try:
                await callback(state_from, state_to)
            except Exception:
                callback_logger.exception(_("%s callback[name=%s, callback=%s] error"), self, name, callback)

    async def _set_state(self, state: ConnectionState):
        state_from, state_to, self._state = self._state, state, state
        if state_to != state_from:
            await self._execute_callbacks(state_from, state_to)

    async def _loop(self):
        """Main connection loop that manages connection lifecycle."""
        try:
            retry_policy = self._open_retry_policy
            delay_iter = iter(retry_policy.delays if retry_policy else [])

            while self._state not in [ConnectionState.CLOSING, ConnectionState.CLOSED]:
                try:
                    logger.info(_("%s connecting[timeout=%s]..."), self, self._connect_timeout)

                    await self._set_state(ConnectionState.CONNECTING)

                    self._conn = cast(
                        aiormq.Connection,
                        await wait_for(
                            aiormq.connect(self._url, context=self._ssl_context),
                            timeout=self._connect_timeout,
                        ),
                    )

                    self._channel = None
                    self._exc = None
                    self._connected_event.set()

                    logger.info(_("%s connected"), self)

                    await self._set_state(ConnectionState.CONNECTED)

                    retry_policy = self._reopen_retry_policy
                    delay_iter = iter(retry_policy.delays if retry_policy else [])

                    aws = [self._conn.closing, create_task(self._closed_event.wait())]
                    while True:
                        done = (await wait(aws, timeout=5, return_when=FIRST_COMPLETED))[0]
                        if done or (self._conn and self._conn.is_connection_was_stuck):
                            break

                    if not aws[1].done():
                        aws[1].cancel()
                        with suppress(CancelledError, RuntimeError):
                            await aws[1]

                    self._connected_event.clear()

                    if self._state in [ConnectionState.CLOSING, ConnectionState.CLOSED]:
                        break
                    elif self._state == ConnectionState.REFRESHING:
                        logger.warning(_("%s refreshing"), self)
                    else:
                        logger.warning(_("%s connection lost"), self)

                except asyncio.CancelledError as e:
                    self._exc = e

                    if self._conn and not self._conn.is_closed:
                        await self._conn.close()

                    raise e

                except Exception as e:
                    self._exc = e

                    if not retry_policy or not retry_policy.is_retryable(e):
                        break

                    try:
                        delay = next(delay_iter)
                    except StopIteration:
                        break

                    logger.warning(_("%s %s %s"), self, e.__class__, e)
                    logger.info(_("%s reconnecting in %.1f seconds"), self, delay)

                    await sleep(delay)

        except Exception as e:
            logger.exception(e)

        finally:
            with suppress(RuntimeError):
                self._connected_event.clear()

            self._conn = None
            self._channel = None
            self._loop_task = None

            if self._state == ConnectionState.CLOSING:
                self._closed_event.set()
                await self._set_state(ConnectionState.CLOSED)
            else:
                await self._set_state(ConnectionState.INITIAL)


@dataclass
class _SharedItem:
    """
    Internal class to track shared connection references.

    Attributes:
        lock: Lock for access synchronization.
        conn: The Connection instance being shared.
        refs: Number of references to this connection.
    """

    lock: Lock
    conn: Connection
    refs: int


class SharedConnection:
    """
    Connection wrapper that shares a single underlying connection.

    Instances created with identical parameters share the same `Connection` object.
    The underlying connection is closed only when all shared references are released.

    Attributes:
        url: Connection URL to RabbitMQ.
        ssl_context: SSL context for TLS connections.
        open_retry_policy: Policy for first connection attempts.
        reopen_retry_policy: Policy for reconnection attempts.
        is_open: Whether connection is open and operational.
        is_closed: Whether connection is closed.

    Examples:
        >>> conn1 = SharedConnection("amqp://localhost")
        >>> conn2 = SharedConnection("amqp://localhost")
        >>> # conn1 and conn2 share the same underlying connection
    """

    _shared = weakref.WeakValueDictionary()

    def __init__(
        self,
        url: str,
        ssl_context: SSLContext | None = None,
        open_retry_policy: RetryPolicy | None = None,
        reopen_retry_policy: RetryPolicy | None = None,
    ):
        """
        Initialize SharedConnection.

        Args:
            url: AMQP connection URL (e.g., "amqp://localhost").
            ssl_context: Optional SSL context for TLS connections.
            open_retry_policy: Reconnection policy for handling first connection errors.
            reopen_retry_policy: Reconnection policy for handling reconnection errors.
        """
        event_loop = get_running_loop()
        self._key = (
            event_loop,
            url,
            ssl_context,
            open_retry_policy,
            reopen_retry_policy,
        )
        if self._key not in self.__class__._shared:
            self._lock = Lock()
            self._conn = Connection(
                url,
                open_retry_policy=open_retry_policy,
                reopen_retry_policy=reopen_retry_policy,
                ssl_context=ssl_context,
            )
            self._shared_item = _SharedItem(lock=self._lock, conn=self._conn, refs=0)
            self.__class__._shared[self._key] = self._shared_item
        else:
            self._shared_item = self.__class__._shared[self._key]
            self._lock = self._shared_item.lock
            self._conn = self._shared_item.conn
        self._channel: aiormq.abc.AbstractChannel | None = None
        self._is_open = asyncio.Event()
        self._is_closed = asyncio.Event()

    def __str__(self):
        return f"{self.__class__.__name__}[{self._conn}]"

    def __repr__(self):
        return self.__str__()

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @property
    def url(self) -> str:
        """Connection URL."""
        return self._conn.url

    @property
    def ssl_context(self) -> SSLContext | None:
        """SSL context for secure connections."""
        return self._conn.ssl_context

    @property
    def open_retry_policy(self) -> RetryPolicy | None:
        """Reconnection policy for handling first connection errors."""
        return self._conn._open_retry_policy

    @property
    def reopen_retry_policy(self) -> RetryPolicy | None:
        """Reconnection policy for handling reconnection errors."""
        return self._conn._reopen_retry_policy

    @property
    def is_open(self) -> bool:
        """Whether connection is open and operational."""
        return self._is_open.is_set() and self._conn.is_open

    @property
    def is_closed(self) -> bool:
        """Whether connection is closed."""
        return self._is_closed.is_set()

    async def open(self, timeout: Number | None = None):
        """
        Open connection to RabbitMQ.

        Establishes connection to RabbitMQ broker. If connection is lost,
        automatically attempts to reconnect based on retry policy.

        Args:
            timeout: Operation timeout in seconds.

        Raises:
            Exception: If connection fails, is closed, or reopened after close.
        """
        if self._is_closed.is_set():
            raise ConnectionInvalidStateError("can not reopen closed connection")

        async with self._lock:
            if self._is_open.is_set():
                return
            await self._conn.open(timeout=timeout)
            self._is_open.set()
            self._shared_item.refs += 1

    async def refresh(self, timeout: Number | None = None):
        """
        Refresh the underlying connection by reopening it.

        Args:
            timeout: Operation timeout in seconds.
        """
        async with self._lock:
            await self._conn.refresh(timeout=timeout)

    async def close(self, timeout: Number | None = None):
        """
        Gracefully closes the connection and all associated channels.

        Args:
            timeout: Operation timeout in seconds.
        """
        async with self._lock:
            if self._is_closed.is_set():
                return
            self._shared_item.refs = max(0, self._shared_item.refs - 1)
            if self._shared_item.refs == 0:
                await self._conn.close(timeout=timeout)
            self._is_closed.set()

    async def new_channel(self, timeout: Number | None = None) -> aiormq.abc.AbstractChannel:
        """
        Create a new channel.

        Args:
            timeout: Operation timeout in seconds.

        Returns:
            A new RabbitMQ channel.
        """
        await self.open()
        return await cast(Connection, self._conn).new_channel(timeout=timeout)

    async def channel(self, timeout: Number | None = None) -> aiormq.abc.AbstractChannel:
        """
        Get or create a channel.

        Args:
            timeout: Operation timeout in seconds.

        Returns:
            An open RabbitMQ channel.
        """
        if self._channel is None or self._channel.is_closed:
            await self.open()
            self._channel = await self._conn.new_channel(timeout=timeout)
        return self._channel

    def set_callback(
        self,
        name: str,
        callback: Callable[[ConnectionState, ConnectionState], Awaitable],
    ):
        """
        Set a callback for connection events.

        Args:
            name: Unique name to identify the callback.
            callback: Callable to execute on event.

        If a callback with this name is already registered, it will be overridden.
        """
        return self._conn.set_callback(name, callback)

    async def remove_callback(self, name: str):
        """
        Remove a callback.

        Args:
            name: Callback name to remove.
        """
        return await self._conn.remove_callback(name)


ExchangeType: TypeAlias = Literal["direct", "fanout", "topic", "headers"]


class Spec(Protocol):
    @property
    def kind(self) -> str: ...

    @property
    def name(self) -> str: ...


@dataclass(frozen=True, slots=True)
class DefaultExchangeSpec:
    """
    Default exchange specification.

    Attributes:
        name: Always empty string.
        kind: Always "default".
        type: Exchange type.
    """

    name: Literal[""] = field(init=False, default="")
    kind: Literal["default"] = field(init=False, default="default")
    type: ExchangeType = "direct"


@dataclass(frozen=True, slots=True)
class BaseExchangeArgs:
    """
    Base exchange arguments.

    Attributes:
        alternate_exchange: Alternate exchange name.
        internal: Whether exchange is internal.
        custom: Custom arguments.
    """

    alternate_exchange: str | None = None
    internal: bool | None = None
    custom: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        args: dict[str, Any] = {}

        if self.alternate_exchange is not None:
            args["alternate-exchange"] = self.alternate_exchange

        if self.internal is not None:
            args["internal"] = self.internal

        if self.custom is not None:
            args.update(self.custom)

        return args


@dataclass(frozen=True, slots=True)
class BaseExchangeSpec:
    """
    Base exchange specification.

    Attributes:
        name: Exchange name.
        kind: Exchange kind ("normal" or "read-only").
        type: Exchange type.
        durable: Whether exchange is durable.
        auto_delete: Whether to delete when no longer used.
        internal: Whether exchange is internal.
        arguments: Exchange arguments.
    """

    name: str
    kind: str
    type: str
    durable: bool = True
    auto_delete: bool = False
    internal: bool = False

    arguments: BaseExchangeArgs = field(default_factory=BaseExchangeArgs)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True, slots=True)
class ExchangeArgs(BaseExchangeArgs):
    """
    Exchange arguments.

    Inherits all attributes from BaseExchangeArgs.
    """


@dataclass(frozen=True, slots=True)
class ExchangeSpec(BaseExchangeSpec):
    """
    Exchange specification.

    Attributes:
        kind: Always "normal" or "read-only".
        type: Exchange type.
        arguments: Exchange arguments.
    """

    kind: Literal["normal", "read-only"] = field(default="normal")
    type: ExchangeType = "direct"

    arguments: ExchangeArgs = field(default_factory=ExchangeArgs)

    def __post_init__(self):
        if self.name == "":
            raise ValueError("use DefaultExchangeSpec for default exchange instead of name=''")


DelayedExchangeType: TypeAlias = Literal["direct", "fanout", "topic"]


@dataclass(frozen=True, slots=True)
class DelayedExchangeArgs(BaseExchangeArgs):
    delayed_type: DelayedExchangeType = "direct"

    def to_dict(self) -> dict[str, Any]:
        args = BaseExchangeArgs.to_dict(self)
        args["x-delayed-type"] = self.delayed_type
        return args


@dataclass(frozen=True, slots=True)
class DelayedExchangeSpec(BaseExchangeSpec):
    name: str
    kind: Literal["normal", "read-only"] = field(default="normal")
    type: Literal["x-delayed-message"] = field(init=False, default="x-delayed-message")
    durable: bool = True
    auto_delete: bool = False
    internal: bool = False

    arguments: DelayedExchangeArgs = field(default_factory=DelayedExchangeArgs)

    def __post_init__(self):
        if self.name == "":
            raise ValueError("use DefaultExchangeSpec for default exchange instead of name=''")


QueueType: TypeAlias = Literal["classic", "quorum", "stream"]

OverflowPolicy: TypeAlias = Literal["drop-head", "reject-publish", "reject-publish-dlx"]


@dataclass(frozen=True, slots=True)
class BaseQueueArgs:
    """
    Base queue arguments.

    Attributes:
        queue_type: Queue type (e.g., "classic", "quorum", "stream").
        dead_letter_exchange: Dead letter exchange name.
        dead_letter_routing_key: Dead letter routing key.
        message_ttl: Message TTL in milliseconds.
        max_length: Maximum queue length.
        overflow: Overflow policy.
        single_active_consumer: Enable single active consumer.
        custom: Custom arguments.
    """

    queue_type: str = ""

    dead_letter_exchange: str | None = None
    dead_letter_routing_key: str | None = None

    message_ttl: int | None = None
    max_length: int | None = None

    overflow: OverflowPolicy | None = None

    single_active_consumer: bool | None = None

    custom: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        args: dict[str, Any] = {}

        args["x-queue-type"] = self.queue_type

        if self.dead_letter_exchange is not None:
            args["x-dead-letter-exchange"] = self.dead_letter_exchange

        if self.dead_letter_routing_key is not None:
            args["x-dead-letter-routing-key"] = self.dead_letter_routing_key

        if self.message_ttl is not None:
            args["x-message-ttl"] = self.message_ttl

        if self.max_length is not None:
            args["x-max-length"] = self.max_length

        if self.overflow is not None:
            args["x-overflow"] = self.overflow

        if self.single_active_consumer is not None:
            args["x-single-active-consumer"] = self.single_active_consumer

        if self.custom is not None:
            args.update(self.custom)

        return args


@dataclass(frozen=True, slots=True)
class BaseQueueSpec:
    """
    Base queue specification.

    Attributes:
        name: Queue name.
        kind: Queue kind ("normal" or "read-only").
        durable: Whether queue is durable.
        exclusive: Whether queue is exclusive.
        auto_delete: Whether to delete when no longer used.
        arguments: Queue arguments.
    """

    name: str
    kind: str

    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False

    arguments: BaseQueueArgs = field(default_factory=BaseQueueArgs)

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True, slots=True)
class QueueArgs(BaseQueueArgs):
    """
    Queue arguments.

    Attributes:
        queue_type: Queue type (defaults to "classic").

    Inherits all attributes from BaseQueueArgs.
    """

    queue_type: QueueType = "classic"


@dataclass(frozen=True, slots=True)
class QueueSpec(BaseQueueSpec):
    """
    Queue specification.

    Attributes:
        kind: Always "normal" or "read-only".
        arguments: Queue arguments.
    """

    kind: Literal["normal", "read-only"] = field(default="normal")

    arguments: QueueArgs = field(default_factory=QueueArgs)


@dataclass(frozen=True, slots=True)
class BindSpec:
    """
    Binding specification.

    Attributes:
        src: Source exchange or queue.
        dst: Destination exchange or queue.
        routing_key: Routing key.
        kind: Type of binding ("exchange" or "queue").
    """

    src: str
    dst: str
    routing_key: str
    kind: Literal["exchange", "queue"] = "queue"


@dataclass(frozen=True, slots=True)
class ConsumerArgs:
    """
    Consumer arguments.

    Attributes:
        priority: Consumer priority.
        cancel_on_ha_failover: Cancel on HA failover.
        custom: Custom arguments.
    """

    priority: int | None = None
    cancel_on_ha_failover: bool | None = None

    custom: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        args: dict[str, Any] = {}

        if self.priority is not None:
            args["x-priority"] = self.priority

        if self.cancel_on_ha_failover is not None:
            args["x-cancel-on-ha-failover"] = self.cancel_on_ha_failover

        if self.custom is not None:
            args.update(self.custom)

        return args


@dataclass(frozen=True, slots=True)
class ConsumerSpec:
    """
    Consumer specification.

    Attributes:
        queue: Queue name.
        callback: Async callback to process messages.
        prefetch_count: Maximum unacknowledged messages.
        prefetch_size: Maximum unacknowledged bytes.
        auto_ack: Auto acknowledge messages.
        exclusive: Exclusive consumer.
        arguments: Consumer arguments.
    """

    queue: str
    callback: Callable[[aiormq.abc.AbstractChannel, aiormq.abc.DeliveredMessage], Awaitable]
    prefetch_count: int | None = None
    prefetch_size: int | None = None
    auto_ack: bool = True
    exclusive: bool = False
    consumer_tag: str | None = None

    arguments: ConsumerArgs = field(default_factory=ConsumerArgs)


@dataclass(slots=True, frozen=True)
class Consumer:
    """
    Consumer instance.

    Attributes:
        spec: Consumer specification.
        consumer_tag: Consumer tag from broker.
        channel: AMQP channel for this consumer.
    """

    spec: ConsumerSpec
    consumer_tag: str
    channel: aiormq.abc.AbstractChannel


T = TypeVar("T", bound=Hashable)


class UniqueList(MutableSequence[T], Generic[T]):
    def __init__(self, iterable: Iterable[T] | None = None):
        self._data: dict[T, None] = {}
        if iterable is not None:
            for item in iterable:
                self.append(item)

    def __len__(self):
        return len(self._data)

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> list[T]: ...

    def __getitem__(self, index):
        keys = list(self._data)
        if isinstance(index, slice):
            return keys[index]
        return keys[index]

    def __setitem__(self, index, value):
        if isinstance(index, slice):
            raise TypeError("slice assignment is not supported")

        keys = list(self._data)

        keys.pop(index)

        try:
            keys.remove(value)
        except ValueError:
            pass

        keys.insert(index, value)
        self._data = dict.fromkeys(keys)

    def __delitem__(self, index):
        keys = list(self._data)
        del keys[index]
        self._data = dict.fromkeys(keys)

    def insert(self, index, value):
        keys = list(self._data)

        try:
            keys.remove(value)
        except ValueError:
            pass

        keys.insert(index, value)
        self._data = dict.fromkeys(keys)

    def append(self, value):
        self._data[value] = None

    def remove(self, value):
        try:
            del self._data[value]
        except KeyError:
            raise ValueError(f"{value!r} not in list")

    def __contains__(self, value):
        return value in self._data

    def __iter__(self):
        return iter(self._data)

    def copy(self):
        return self.__class__(self)

    def __eq__(self, other):
        if isinstance(other, MutableSequence):
            return list(self) == list(other)
        return NotImplemented

    def __repr__(self):
        return f"{self.__class__.__name__}({list(self._data)})"


@dataclass(frozen=True, slots=True)
class Topology:
    """
    Topology of RabbitMQ entities.

    Attributes:
        exchanges: List of exchange specifications.
        queues: List of queue specifications.
        bindings: List of binding specifications.
        consumers: List of consumer specifications.
    """

    exchanges: UniqueList[BaseExchangeSpec] = field(default_factory=lambda: UniqueList[BaseExchangeSpec]())
    queues: UniqueList[BaseQueueSpec] = field(default_factory=lambda: UniqueList[BaseQueueSpec]())
    bindings: UniqueList[BindSpec] = field(default_factory=lambda: UniqueList[BindSpec]())
    consumers: UniqueList[ConsumerSpec] = field(default_factory=lambda: UniqueList[ConsumerSpec]())


class Ops:
    """
    RabbitMQ operations handler.

    Provides high-level operations for managing exchanges, queues,
    bindings, and consuming messages.

    Attributes:
        conn: Connection to RabbitMQ.
        timeout: Default operation timeout.
    """

    def __init__(
        self,
        conn: ConnectionProtocol,
        timeout: Number | None = None,
    ):
        """
        Initialize Ops.

        Args:
            conn: Connection to RabbitMQ.
            timeout: Default operation timeout.
        """
        self._conn = conn
        self._timeout = timeout
        self._topology = Topology()
        self._consumers: dict[str, Consumer] = {}

        self._conn.set_callback(
            f"on_connection_state_changed[{id(self)}]",
            self._on_connection_state_changed,
        )

    async def _on_connection_state_changed(self, state_from: ConnectionState, state_to: ConnectionState):
        if state_to == ConnectionState.CONNECTED and state_from != ConnectionState.INITIAL:
            await self._restore_topology()

    async def _restore_topology(self):
        for spec in self._topology.exchanges:
            await self.exchange_declare(spec)
        for spec in self._topology.queues:
            await self.queue_declare(spec)
        for spec in self._topology.bindings:
            await self.bind(spec)

        consumers_map = {consumer.spec: consumer for consumer in self._consumers.values()}
        for spec in self._topology.consumers:
            consumer = consumers_map.get(spec)
            if consumer and not consumer.channel.is_closed:
                continue
            await self.consume(spec)

    @property
    def consumers(self) -> list[Consumer]:
        return list(self._consumers.values())

    async def apply_topology(self, topology: Topology, consume: bool | None = None, restore: bool | None = None):
        """
        Apply entire topology declaration.

        Args:
            topology: Topology to apply.
            consume: If `True`, start consuming according to the topology.
            restore: If `True`, restore on reconnect.
        """
        logger.info("apply topology[restore=%s] %s", topology, restore)

        for spec in topology.exchanges:
            await self.exchange_declare(spec, restore=restore)
        for spec in topology.queues:
            await self.queue_declare(spec, restore=restore)
        for spec in topology.bindings:
            await self.bind(spec, restore=restore)
        if consume:
            for spec in topology.consumers:
                if not next(filter(lambda consumer: consumer.spec == spec, self.consumers), None):
                    await self.consume(spec, restore=restore)

    async def check_exists(
        self,
        spec: BaseExchangeSpec | BaseQueueSpec,
        timeout: Number | None = None,
    ) -> bool:
        """
        Check if subj exists.

        Args:
            spec: Specification to check.
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.

        Returns:
            True if the subj exists, False otherwise.
        """
        match spec:
            case BaseExchangeSpec():
                return await self.check_exchange_exists(spec.name, timeout=timeout)
            case BaseQueueSpec():
                return await self.check_queue_exists(spec.name, timeout=timeout)
            case _:
                raise ValueError("invalid spec type")

    async def declare(
        self,
        spec: BaseExchangeSpec | BaseQueueSpec,
        timeout: Number | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        """
        Declare subj.

        Args:
            restore: If `True`, automatically redeclare subj after reconnection.
            force: If `True`, delete and redeclare subj if declaration fails
                due to parameter mismatch.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        match spec:
            case BaseExchangeSpec():
                await self.exchange_declare(spec, timeout=timeout, restore=restore, force=force)
            case BaseQueueSpec():
                await self.queue_declare(spec, timeout=timeout, restore=restore, force=force)
            case _:
                raise ValueError("invalid spec type")

    async def delete(self, spec: Spec, timeout: Number | None = None):
        """
        Delete subj.

        Args:
            timeout: Operation timeout. If `None`, uses the default timeout.

        Raises:
            Exception: If deleting fails.
        """
        match spec:
            case BaseExchangeSpec():
                if spec.kind == "read-only":
                    raise OperationError("can not delete read-only exchange")
                await self.exchange_delete(spec.name, timeout=timeout)
            case BaseQueueSpec():
                if spec.kind == "read-only":
                    raise OperationError("can not delete read-only queue")
                await self.queue_delete(spec.name, timeout=timeout)
            case _:
                raise ValueError("invalid spec type")

    async def check_exchange_exists(self, name: str, timeout: Number | None = None) -> bool:
        """
        Check if exchange exists.

        Args:
            name: Exchange name.
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.

        Returns:
            True if the exchange exists, False otherwise.
        """
        timeout_ = timeout if timeout is not None else self._timeout

        channel = await self._conn.channel(timeout=timeout_)

        try:
            await channel.exchange_declare(name, passive=True, timeout=timeout_)
            return True
        except aiormq.ChannelNotFoundEntity:
            return False

    async def exchange_declare(
        self,
        spec: BaseExchangeSpec,
        timeout: Number | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        """
        Declare the exchange.

        Args:
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.
            restore: If `True`, automatically redeclare the exchange after reconnection.
            force: If `True`, delete and redeclare the exchange if declaration fails
                due to parameter mismatch.

        Raises:
            Exception: If declaring fails.
        """
        if spec.kind == "read-only":
            raise OperationError("can not declare read-only exchange")

        timeout_ = timeout if timeout is not None else self._timeout

        async def op():
            logger.info(_("declare[restore=%s, force=%s] %s"), restore, force, spec)

            channel = await self._conn.channel(timeout=timeout_)
            await channel.exchange_declare(
                spec.name,
                exchange_type=spec.type,
                durable=spec.durable,
                auto_delete=spec.auto_delete,
                arguments=spec.arguments.to_dict(),
                timeout=timeout_,
            )

        if force:

            async def on_error(e):
                channel = await self._conn.channel(timeout=timeout_)
                logger.info(_("delete[on_error] %s"), spec)
                await channel.exchange_delete(spec.name, timeout=timeout_)

            await retry(
                RetryPolicy(delays=[0], exc_filter=(aiormq.ChannelPreconditionFailed,)),
                on_error=on_error,
            )(op)()

        else:
            await op()

        if restore:
            self._topology.exchanges.append(spec)

    async def exchange_delete(self, name: str, timeout: Number | None = None):
        """
        Delete exchange.

        Args:
            name: Exchange name.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        logger.info(_("delete exchange '%s'"), name)

        timeout_ = timeout if timeout is not None else self._timeout

        channel = await self._conn.channel(timeout=timeout_)

        await channel.exchange_delete(name, timeout=timeout_)

        spec = next(filter(lambda spec: spec.name == name, self._topology.exchanges), None)
        if spec:
            self._topology.exchanges.remove(spec)

    async def check_queue_exists(self, name: str, timeout: Number | None = None) -> bool:
        """
        Check if queue exists.

        Args:
            name: Queue name.
            timeout: Operation timeout. If `None`, uses the default timeout.

        Returns:
            True if the queue exists, False otherwise.
        """
        timeout_ = timeout if timeout is not None else self._timeout

        channel = await self._conn.channel(timeout=timeout_)

        try:
            await channel.queue_declare(name, passive=True, timeout=timeout_)
            return True
        except aiormq.ChannelNotFoundEntity:
            return False

    async def queue_declare(
        self,
        spec: BaseQueueSpec,
        timeout: Number | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        """
        Declare queue.

        Args:
            spec: Queue specification.
            timeout: Operation timeout. If `None`, uses the default timeout.
            restore: Restore this binding on connection issue.
            force: Force redeclare queue if it has already been declared with different parameters.

        Raises:
            Exception: If declaring fails.
        """
        if spec.kind == "read-only":
            raise OperationError("can not declare read-only queue")

        timeout_ = timeout if timeout is not None else self._timeout

        async def op():
            logger.info(_("declare[restore=%s, force=%s] %s"), restore, force, spec)

            channel = await self._conn.channel(timeout=timeout_)
            arguments = spec.arguments.to_dict()
            await channel.queue_declare(
                spec.name,
                durable=spec.durable,
                exclusive=spec.exclusive,
                auto_delete=spec.auto_delete,
                arguments=arguments,
                timeout=timeout_,
            )

        if force:

            async def on_error(e):
                channel = await self._conn.channel(timeout=timeout_)
                logger.info(_("delete[on_error] %s"), spec)
                await channel.queue_delete(spec.name, timeout=timeout_)

            await retry(
                RetryPolicy(delays=[0], exc_filter=(aiormq.ChannelPreconditionFailed,)),
                on_error=on_error,
            )(op)()

        else:
            await op()

        if restore:
            self._topology.queues.append(spec)

    async def queue_delete(self, name: str, timeout: Number | None = None):
        """
        Delete queue.

        Args:
            name: Queue name.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        logger.info(_("delete queue '%s'"), name)

        timeout_ = timeout if timeout is not None else self._timeout

        channel = await self._conn.channel(timeout=timeout_)

        await channel.queue_delete(name, timeout=timeout_)

        spec = next(filter(lambda spec: spec.name == name, self._topology.queues), None)
        if spec:
            self._topology.queues.remove(spec)

    async def bind(
        self,
        spec: BindSpec,
        timeout: Number | None = None,
        restore: bool | None = None,
    ):
        """
        Bind item to exchange.

        Args:
            spec: Bind specification.
            timeout: Operation timeout. If `None`, uses the default timeout.
            restore: Restore this binding on connection issue.
        """
        logger.info(
            _("bind %s '%s' to exchange '%s' with routing_key '%s'"),
            spec.kind,
            spec.dst,
            spec.src,
            spec.routing_key,
        )

        timeout_ = timeout if timeout is not None else self._timeout
        channel = await self._conn.channel(timeout=timeout_)

        match spec.kind:
            case "exchange":
                await channel.exchange_bind(
                    spec.dst,
                    spec.src,
                    routing_key=spec.routing_key,
                    timeout=timeout_,
                )
            case "queue":
                await channel.queue_bind(
                    spec.dst,
                    spec.src,
                    routing_key=spec.routing_key,
                    timeout=timeout_,
                )
            case _:
                raise ValueError("invalid spec type")

        if restore:
            self._topology.bindings.append(spec)

    async def unbind(
        self,
        spec: BindSpec,
        timeout: Number | None = None,
    ):
        """
        Unbind item from exchange.

        Args:
            spec: Bind specification.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        logger.info(
            _("unbind %s '%s' from exchange '%s' for routing_key '%s'"),
            spec.kind,
            spec.dst,
            spec.src,
            spec.routing_key,
        )

        timeout_ = timeout if timeout is not None else self._timeout
        channel = await self._conn.channel(timeout=timeout_)

        match spec.kind:
            case "exchange":
                await channel.exchange_unbind(
                    spec.dst,
                    spec.src,
                    routing_key=spec.routing_key,
                    timeout=timeout_,
                )
            case "queue":
                await channel.queue_unbind(
                    spec.dst,
                    spec.src,
                    routing_key=spec.routing_key,
                    timeout=timeout_,
                )
            case _:
                raise ValueError("invalid spec type")

        if spec in self._topology.bindings:
            self._topology.bindings.remove(spec)

    async def publish(
        self,
        exchange: str,
        data: bytes,
        routing_key: str,
        properties: dict | None = None,
        mandatory: bool = False,
        timeout: Number | None = None,
    ):
        """
        Publish data to the exchange.

        Args:
            exchange: Exchange name.
            data: Data to publish.
            routing_key: Routing key for message delivery.
            properties: Optional RabbitMQ message properties.
            mandatory: If `True`, return unroutable message to publisher.
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.
        """
        timeout_ = timeout if timeout is not None else self._timeout

        channel = await self._conn.channel(timeout=timeout_)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                _("exchange[name='%s'] %s channel[%s] publish[routing_key='%s'] %s"),
                exchange,
                self._conn,
                channel,
                routing_key,
                (
                    (
                        data[: config.log_data_truncate_size] + b"<truncated>"
                        if len(data) > config.log_data_truncate_size
                        else data
                    )
                    if not config.log_sanitize
                    else "<hidden>"
                ),
            )

        await channel.basic_publish(
            data,
            exchange=exchange,
            routing_key=routing_key,
            properties=BasicProperties(**(properties or {})),
            mandatory=mandatory,
            timeout=timeout_,
        )

    async def consume(
        self,
        spec: ConsumerSpec,
        timeout: Number | None = None,
        restore: bool | None = None,
    ) -> Consumer:
        """
        Consume queue.

        Args:
            spec: Spec.
            timeout: Operation timeout. If `None`, uses the default timeout.
            restore: Restore consuming on connection issue.

        Returns:
            Consumer: The active consumer instance.
        """
        timeout_ = timeout if timeout is not None else self._timeout

        channel = await self._conn.new_channel(timeout=timeout_)

        await channel.basic_qos(
            prefetch_count=spec.prefetch_count,
            prefetch_size=spec.prefetch_size,
            timeout=timeout_,
        )

        consumer_tag = cast(
            str,
            (
                await channel.basic_consume(
                    spec.queue,
                    partial(spec.callback, channel),
                    no_ack=spec.auto_ack,
                    exclusive=spec.exclusive,
                    arguments=spec.arguments.to_dict(),
                    consumer_tag=spec.consumer_tag,
                    timeout=timeout_,
                )
            ).consumer_tag,
        )

        logger.info(_("consume %s"), spec)

        consumer = Consumer(spec, consumer_tag, channel)

        if restore:
            self._topology.consumers.append(spec)

        self._consumers[consumer_tag] = consumer

        return consumer

    async def stop_consume(
        self,
        consumer_tag: str,
        timeout: Number | None = None,
    ):
        """
        Stop consume.

        Args:
            consumer_tag: Consumer tag.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        if consumer_tag in self._consumers:
            consumer = self._consumers.pop(consumer_tag)
            if consumer.spec in self._topology.consumers:
                self._topology.consumers.remove(consumer.spec)
            if not consumer.channel.is_closed:
                logger.info(_("stop consume %s"), consumer.spec)
                timeout_ = timeout if timeout is not None else self._timeout
                await consumer.channel.basic_cancel(consumer.consumer_tag, timeout=timeout_)


@dataclass(frozen=True, slots=True)
class DefaultExchange:
    """
    Default exchange wrapper.

    Attributes:
        spec: Default exchange specification.
        ops: Ops instance.
    """

    spec: DefaultExchangeSpec = field(init=False, default_factory=DefaultExchangeSpec)
    ops: Ops

    async def publish(
        self,
        data: bytes,
        routing_key: str,
        properties: dict | None = None,
        mandatory: bool = False,
        timeout: Number | None = None,
    ):
        """
        Publish data to the default exchange.

        Args:
            data: Data to publish.
            routing_key: Routing key for message delivery.
            properties: Optional RabbitMQ message properties.
            mandatory: If `True`, return unroutable message to publisher.
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.
        """

        await self.ops.publish(
            self.spec.name,
            data,
            routing_key,
            properties=properties,
            mandatory=mandatory,
            timeout=timeout,
        )


@dataclass(frozen=True, slots=True)
class Exchange:
    """
    Exchange wrapper.

    Attributes:
        spec: Exchange specification.
        ops: Ops instance.
    """

    spec: BaseExchangeSpec
    ops: Ops

    async def check_exists(self, timeout: Number | None = None) -> bool:
        """
        Check if exchange exists.

        Args:
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.

        Returns:
            True if the exchange exists, False otherwise.
        """
        return await self.ops.check_exchange_exists(self.spec.name, timeout=timeout)

    async def declare(
        self,
        timeout: Number | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        """
        Declare exchange.

        Args:
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.
            restore: Restore this exchange on connection issue.
            force: Force redeclare if already declared with different parameters.
        """
        await self.ops.exchange_declare(self.spec, timeout=timeout, restore=restore, force=force)

    async def delete(self, timeout: Number | None = None):
        """
        Delete exchange.

        Args:
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        await self.ops.delete(self.spec, timeout=timeout)

    async def bind(
        self,
        exchange: str,
        routing_key: str,
        timeout: Number | None = None,
        restore: bool | None = None,
    ):
        """
        Bind this exchange to another exchange.

        Args:
            exchange: Exchange name.
            routing_key: Routing key to bind.
            timeout: Operation timeout. If `None`, uses the default timeout.
            restore: Restore this binding on connection issue.
        """
        await self.ops.bind(
            BindSpec(src=exchange, dst=self.spec.name, routing_key=routing_key),
            timeout=timeout,
            restore=restore,
        )

    async def unbind(
        self,
        exchange: str,
        routing_key: str,
        timeout: Number | None = None,
    ):
        """
        Unbind this exchange from another exchange.

        Args:
            exchange: Exchange name.
            routing_key: Routing key to unbind.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        await self.ops.unbind(
            BindSpec(src=exchange, dst=self.spec.name, routing_key=routing_key),
            timeout=timeout,
        )

    async def publish(
        self,
        data: bytes,
        routing_key: str,
        properties: dict | None = None,
        mandatory: bool = False,
        timeout: Number | None = None,
    ):
        """
        Publish data to the exchange.

        Args:
            data: Data to publish.
            routing_key: Routing key for message delivery.
            properties: Optional RabbitMQ message properties.
            mandatory: If `True`, return unroutable message to publisher.
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.
        """

        await self.ops.publish(
            self.spec.name,
            data,
            routing_key,
            properties=properties,
            mandatory=mandatory,
            timeout=timeout,
        )


@dataclass(frozen=True, slots=True)
class Queue:
    """
    Queue wrapper.

    Attributes:
        spec: Queue specification.
        ops: Ops instance.
    """

    spec: BaseQueueSpec
    ops: Ops

    async def check_exists(self, timeout: Number | None = None) -> bool:
        """
        Check if queue exists.

        Args:
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.

        Returns:
            True if the queue exists, False otherwise.
        """
        return await self.ops.check_queue_exists(self.spec.name, timeout=timeout)

    async def declare(
        self,
        timeout: Number | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        """
        Declare queue.

        Args:
            timeout: Operation timeout in seconds. If `None`, uses the default timeout.
            restore: Restore this queue on connection issue.
            force: Force redeclare if already declared with different parameters.
        """
        await self.ops.queue_declare(self.spec, timeout=timeout, restore=restore, force=force)

    async def delete(self, timeout: Number | None = None):
        """
        Delete queue.

        Args:
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        await self.ops.delete(self.spec, timeout=timeout)

    async def bind(
        self,
        exchange: str,
        routing_key: str,
        timeout: Number | None = None,
        restore: bool | None = None,
    ):
        """
        Bind queue to exchange.

        Args:
            exchange: Exchange name.
            routing_key: Routing key to bind.
            timeout: Operation timeout. If `None`, uses the default timeout.
            restore: Restore this binding on connection issue.
        """
        await self.ops.bind(
            BindSpec(src=exchange, dst=self.spec.name, routing_key=routing_key),
            timeout=timeout,
            restore=restore,
        )

    async def unbind(
        self,
        exchange: str,
        routing_key: str,
        timeout: Number | None = None,
    ):
        """
        Unbind queue from exchange.

        Args:
            exchange: Exchange name.
            routing_key: Routing key to unbind.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        await self.ops.unbind(
            BindSpec(src=exchange, dst=self.spec.name, routing_key=routing_key),
            timeout=timeout,
        )

    async def consume(
        self,
        callback: Callable[[aiormq.abc.AbstractChannel, aiormq.abc.DeliveredMessage], Awaitable],
        prefetch_count: int | None = None,
        prefetch_size: int | None = None,
        auto_ack: bool = True,
        exclusive: bool = False,
        consumer_tag: str | None = None,
        arguments: ConsumerArgs | None = None,
        timeout: Number | None = None,
        restore: bool | None = None,
    ) -> Consumer:
        """
        Start consuming messages from queue.

        Args:
            callback: Async callback function to handle messages.
            prefetch_count: Maximum number of unacknowledged messages.
            prefetch_size: Maximum number of unacknowledged bytes.
            auto_ack: If True, automatically acknowledge messages.
            exclusive: If True, create exclusive consumer.
            consumer_tag: Custom consumer tag.
            arguments: Consumer arguments.
            timeout: Operation timeout in seconds.
            restore: If True, restore consumer on reconnect.

        Returns:
            Consumer: Active consumer instance.
        """
        spec = ConsumerSpec(
            queue=self.spec.name,
            callback=callback,
            prefetch_count=prefetch_count,
            prefetch_size=prefetch_size,
            auto_ack=auto_ack,
            exclusive=exclusive,
            consumer_tag=consumer_tag,
            arguments=arguments or ConsumerArgs(),
        )
        return await self.ops.consume(spec, timeout=timeout, restore=restore)

    async def stop_consume(
        self,
        consumer_tag: str,
        timeout: Number | None = None,
    ):
        """
        Stop consume.

        Args:
            consumer_tag: Consumer tag.
            timeout: Operation timeout. If `None`, uses the default timeout.
        """
        await self.ops.stop_consume(consumer_tag, timeout=timeout)
