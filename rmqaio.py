import asyncio
import importlib.metadata
import logging
import os
import ssl
import sys

from asyncio import FIRST_COMPLETED, Event, Future, Lock, Task, create_task, current_task, get_event_loop, sleep, wait
from collections.abc import Hashable
from dataclasses import dataclass, field
from enum import StrEnum
from functools import partial, wraps
from inspect import iscoroutine, iscoroutinefunction
from itertools import chain, repeat
from ssl import SSLContext
from typing import Callable, Coroutine, Iterable
from uuid import uuid4

import aiormq
import aiormq.exceptions
import yarl


__version__ = importlib.metadata.version("rmqaio")


logger = logging.getLogger("rmqaio")

log_frmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s lineno:%(lineno)4d -- %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stderr)
log_hndl.setFormatter(log_frmt)

logger.addHandler(log_hndl)

CONNECT_TIMEOUT = 15
LOG_SANITIZE = True

BasicProperties = aiormq.spec.Basic.Properties


class QueueType(StrEnum):
    """RabbitMQ queue type"""

    CLASSIC = "classic"
    QUORUM = "quorum"


def retry(
    *,
    retry_timeouts: Iterable[int],
    exc_filter: Callable[[Exception], bool],
    msg: str | None = None,
    on_error: Callable | None = None,
):
    """Retry decorator.

    Args:
        retry_timeouts: Retry timeout as list of int, for example: `[1, 2, 3]` or `itertools.repeat(5)`.
        exc_filter: Callable to determine whether or not to retry.
        msg: Message to log on retry.
        on_error: Callable to call on error.
        reraise: Reraise exception or not.
    """

    def decorator(fn):
        @wraps(fn)
        async def wrapper(*args, **kwds):
            timeouts = iter(retry_timeouts)
            attempt = 0
            while True:
                try:
                    return await fn(*args, **kwds)
                except Exception as e:
                    if not exc_filter(e):
                        raise e
                    try:
                        t = next(timeouts)
                        attempt += 1
                        logger.warning(
                            "%s (%s %s) retry(%s) in %s second(s)",
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
                        raise e

        return wrapper

    return decorator


def create_ssl_context(url: str, password: str | None = None, cwd: str | None = None) -> SSLContext | None:
    """Create ssl context from url."""

    if not url.startswith("amqps://"):
        return

    cwd = cwd or os.path.abspath(os.path.dirname(__file__))

    query = yarl.URL(url).query

    capath = query.get("capath")
    if capath and not capath.startswith("/"):
        capath = os.path.join(cwd, capath)

    cafile = query.get("cafile")
    if cafile and not cafile.startswith("/"):
        cafile = os.path.join(cwd, cafile)

    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, capath=capath, cafile=cafile)

    cert = query.get("certfile")
    if cert:
        if not cert.startswith("/"):
            cert = os.path.join(cwd, cert)
        keyfile = query.get("keyfile")
        if keyfile and not keyfile.startswith("/"):
            keyfile = os.path.join(cwd, keyfile)
        context.load_cert_chain(cert, keyfile=keyfile, password=password)

    verify = query.get("no_verify_ssl", "0") == "0"
    if not verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    return context


class LoopIter:
    """Infinity iterator class."""

    __slots__ = ("_data", "_i", "_j", "_iter")

    def __init__(self, data: list):
        self._data = data
        self._i = -1
        self._j = 0
        self._iter = iter(data)

    def __next__(self):
        if self._j == len(self._data):
            self._j = 0
            raise StopIteration
        self._i = (self._i + 1) % len(self._data)
        self._j += 1
        return self._data[self._i]

    def reset(self):
        self._j = 1


class Connection:
    """RabbitMQ connection."""

    __shared: dict = {}

    def __init__(
        self,
        url: str | list[str],
        name: str | None = None,
        retry_timeouts: Iterable[int] | None = None,
        exc_filter: Callable[[Exception], bool] | None = None,
    ):
        if not isinstance(url, (list, tuple, set)):
            self.urls = [url]
        else:
            self.urls = list(url)

        self._urls_iter = LoopIter(self.urls)

        self.url = next(self._urls_iter)

        self.name = name or uuid4().hex[-4:]

        self._ssl_contexts = {url: create_ssl_context(url, cwd=os.path.abspath("~")) for url in self.urls}

        self._open_task: Task | Future = Future()
        self._open_task.set_result(None)

        self._watcher_task: Task | None = None
        self._watcher_task_started: Event = Event()

        self._closed: Future = Future()

        self._key: tuple = (name, get_event_loop(), tuple(sorted(self.urls)))

        if self._key not in self.__shared:
            self.__shared[self._key] = {
                "refs": 0,
                "objs": 0,
                "conn": None,
                "connect_lock": Lock(),
            }

        shared: dict = self.__shared[self._key]
        shared["objs"] += 1
        shared[self] = {
            "on_open": {},
            "on_lost": {},
            "on_close": {},
            "callback_tasks": {"on_open": {}, "on_lost": {}, "on_close": {}},
        }
        self._shared = shared

        self._channel: aiormq.abc.AbstractChannel | None = None

        self._retry_timeouts = retry_timeouts or []

        self._exc_filter = exc_filter or (
            lambda e: isinstance(e, (asyncio.TimeoutError, ConnectionError, aiormq.exceptions.AMQPConnectionError))
        )

    def __del__(self):
        if getattr(self, "_key", None):
            if self._conn and not self.is_closed:
                logger.warning("%s unclosed", self)
            shared = self._shared
            shared["objs"] -= 1
            if self in shared:
                shared.pop(self, None)
            if shared["objs"] == 0:
                self.__shared.pop(self._key, None)

    @property
    def _conn(self) -> aiormq.abc.AbstractConnection:
        return self._shared["conn"]

    @_conn.setter
    def _conn(self, value: aiormq.abc.AbstractConnection | None):
        self._shared["conn"] = value

    @property
    def _refs(self) -> int:
        return self._shared["refs"]

    @_refs.setter
    def _refs(self, value: int):
        self._shared["refs"] = value

    async def _execute_callbacks(self, tp: str, reraise: bool | None = None):
        async def fn(name, callback):
            logger.debug("%s execute callback[tp=%s, name=%s, reraise=%s]", self, tp, name, reraise)

            self._shared[self]["callback_tasks"][tp][name] = current_task()
            try:
                if iscoroutinefunction(callback):
                    await callback()
                else:
                    res = callback()
                    if iscoroutine(res):
                        await res
            except Exception as e:
                logger.exception("%s callback[tp=%s, name=%s, callback=%s] error", self, tp, name, callback)
                if reraise:
                    raise e
            finally:
                self._shared[self]["callback_tasks"][tp].pop(name, None)

        for name, callback in tuple(self._shared[self][tp].items()):
            await create_task(fn(name, callback))

    def set_callback(self, tp: str, name: Hashable, callback: Callable):
        logger.debug("%s set callback[tp=%s, name=%s, callback=%s]", self, tp, name, callback)
        if shared := self._shared.get(self):
            if tp not in shared:
                raise ValueError("invalid callback type")
            shared[tp][name] = callback

    def remove_callback(self, tp: str, name: Hashable, cancel: bool | None = None):
        if shared := self._shared.get(self):
            if tp not in shared:
                raise ValueError("invalid callback type")
            if name in shared[tp]:
                del shared[tp][name]
            if cancel:
                task = shared["callback_tasks"][tp].get(name)
                if task:
                    task.cancel()

    def remove_callbacks(self, cancel: bool | None = None):
        if self in self._shared:
            if cancel:
                for tp in ("on_open", "on_lost", "on_close"):
                    for task in self._shared[self]["callback_tasks"][tp].values():
                        task.cancel()
            self._shared[self] = {
                "on_open": {},
                "on_lost": {},
                "on_close": {},
                "callback_tasks": {"on_open": {}, "on_lost": {}, "on_close": {}},
            }

    def __str__(self):
        return f"{self.__class__.__name__}[{yarl.URL(self.url).host}]#{self.name}"

    def __repr__(self):
        return self.__str__()

    @property
    def is_open(self) -> bool:
        return self._watcher_task is not None and not (self.is_closed or self._conn is None or self._conn.is_closed)

    @property
    def is_closed(self) -> bool:
        return self._closed.done()

    async def _watcher(self):
        try:
            self._watcher_task_started.set()
            await wait([self._conn.closing, self._closed], return_when=FIRST_COMPLETED)
        except Exception as e:
            logger.exception(e)
            logger.warning("%s %s %s", self, e.__class__, e)

        self._watcher_task = None

        if not self._closed.done():
            logger.warning("%s connection lost", self)
            if self._channel:
                await self._channel.close()
            self._refs -= 1
            create_task(self.open(retry_timeouts=iter(chain((0, 3), repeat(5)))))
            await self._execute_callbacks("on_lost")

    async def _connect(
        self,
        retry_timeouts: Iterable[int] | None = None,
        exc_filter: Callable[[Exception], bool] | None = None,
    ):
        if retry_timeouts is None:
            retry_timeouts = self._retry_timeouts
        if exc_filter is None:
            exc_filter = self._exc_filter
        while not self.is_closed:
            connect_timeout = yarl.URL(self.url).query.get("connection_timeout")
            if connect_timeout is not None:
                connect_timeout = int(connect_timeout) / 1000
            else:
                connect_timeout = CONNECT_TIMEOUT
            try:
                logger.info("%s connecting[timeout=%s]...", self, connect_timeout)

                async with asyncio.timeout(connect_timeout):
                    if self._retry_timeouts:
                        self._conn = await retry(
                            retry_timeouts=retry_timeouts,
                            exc_filter=exc_filter,
                        )(aiormq.connect)(
                            self.url,
                            context=self._ssl_contexts[self.url],
                        )
                    else:
                        self._conn = await aiormq.connect(self.url, context=self._ssl_contexts[self.url])
                self._urls_iter.reset()
                break
            except (asyncio.TimeoutError, ConnectionError, aiormq.exceptions.ConnectionClosed) as e:
                try:
                    url = next(self._urls_iter)
                except StopIteration:
                    raise e
                logger.warning("%s %s %s", self, e.__class__, e)
                self.url = url

        logger.info("%s connected", self)

    async def open(
        self,
        retry_timeouts: Iterable[int] | None = None,
        exc_filter: Callable[[Exception], bool] | None = None,
    ):
        """Open connection."""

        if self.is_open:
            return

        if self.is_closed:
            self._closed = Future()

        async with self._shared["connect_lock"]:
            if self._conn is None or self._conn.is_closed:
                self._open_task = create_task(self._connect(retry_timeouts=retry_timeouts, exc_filter=exc_filter))
                await self._open_task

            if self._watcher_task is None:
                self._refs += 1
                self._watcher_task_started.clear()
                self._watcher_task = create_task(self._watcher())
                await self._watcher_task_started.wait()
                try:
                    await self._execute_callbacks("on_open", reraise=True)
                except Exception as e:
                    logger.exception(e)
                    await self.close()
                    raise e

    async def close(self):
        """Close connection"""

        if self.is_closed:
            return

        if not self._open_task.done():
            self._open_task.cancel()
            self._open_task = Future()

        if self._conn:
            await self._execute_callbacks("on_close")

        self._closed.set_result(None)

        self._refs = max(0, self._refs - 1)
        if self._refs == 0:
            if self._conn:
                await self._conn.close()
                self._conn = None
                logger.info("%s close underlying connection", self)

        self.remove_callbacks(cancel=True)

        if self._watcher_task:
            await self._watcher_task
            self._watcher_task = None

        logger.info("%s closed", self)

    @retry(retry_timeouts=[0], exc_filter=lambda e: isinstance(e, aiormq.exceptions.ConnectionClosed))
    async def new_channel(self) -> aiormq.abc.AbstractChannel:
        """Create new channel"""

        await self.open()
        return await self._conn.channel()

    async def channel(self) -> aiormq.abc.AbstractChannel:
        """Get or create channel"""

        if self._channel is None or self._channel.is_closed:
            await self.open()
            if self._channel is None or self._channel.is_closed:
                self._channel = await self.new_channel()
        return self._channel


@dataclass(slots=True, frozen=True)
class SimpleExchange:
    """Simple exchange. Only publish method allowed."""

    name: str = ""
    timeout: int | None = None
    conn: Connection = None  # type: ignore
    conn_factory: Callable = field(default=None, repr=False)  # type: ignore

    def __post_init__(self):
        if all((self.conn, self.conn_factory)):
            raise Exception("conn and conn_factory are incompatible")
        if not any((self.conn, self.conn_factory)):
            raise Exception("conn or conn_factory is requried")
        if self.conn_factory:
            object.__setattr__(self, "conn", self.conn_factory())

    async def close(self):
        logger.debug("%s close", self)
        try:
            if self.conn_factory:
                self.conn.remove_callbacks(cancel=True)
        finally:
            if self.conn_factory:
                await self.conn.close()

    async def publish(
        self,
        data: bytes,
        routing_key: str,
        properties: dict | None = None,
        timeout: int | None = None,
    ):
        """
        Publish data to exchange with routing key
        Args:
            data: data to publish.
            routing_key: routing key.
            properties: RabbitMQ message properties.
            timeout: publish operation timeout.
        """

        channel = await self.conn.channel()

        logger.debug(
            "Exchange[name='%s'] channel[%s] publish[routing_key='%s'] %s",
            self.name,
            channel,
            routing_key,
            data if not LOG_SANITIZE else "<hiden>",
        )

        await channel.basic_publish(
            data,
            exchange=self.name,
            routing_key=routing_key,
            properties=BasicProperties(**(properties or {})),
            timeout=timeout or self.timeout,
        )


@dataclass(slots=True, frozen=True)
class Exchange:
    """RabbitMQ exchange class."""

    name: str = ""
    type: str = "direct"
    durable: bool = False
    auto_delete: bool = False
    timeout: int | None = None
    conn: Connection = None  # type: ignore
    conn_factory: Callable = field(default=None, repr=False)  # type: ignore

    def __post_init__(self):
        if all((self.conn, self.conn_factory)):
            raise Exception("conn and conn_factory are incompatible")
        if not any((self.conn, self.conn_factory)):
            raise Exception("conn or conn_factory is requried")
        if self.conn_factory:
            object.__setattr__(self, "conn", self.conn_factory())

    async def close(self, delete: bool | None = None, timeout: int | None = None):
        if self.conn.is_closed:
            raise Exception("already closed")

        logger.debug("%s close[delete=%s]", self, delete)

        try:
            if self.conn_factory:
                self.conn.remove_callbacks(cancel=True)
            else:
                self.conn.remove_callback("on_open", f"on_open_exchange_{self.name}_declare", cancel=True)
            if delete and self.name != "":
                channel = await self.conn.channel()
                try:
                    await channel.exchange_delete(self.name, timeout=timeout or self.timeout)
                except aiormq.exceptions.AMQPError:
                    pass
        finally:
            if self.conn_factory:
                await self.conn.close()

    async def declare(
        self,
        timeout: int | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        if self.name == "":
            return

        logger.debug("%s declare[restore=%s, force=%s]", self, restore, force)

        async def fn():
            channel = await self.conn.channel()
            await channel.exchange_declare(
                self.name,
                exchange_type=self.type,
                durable=self.durable,
                auto_delete=self.auto_delete,
                timeout=timeout or self.timeout,
            )

        if force:

            async def on_error(e):
                channel = await self.conn.channel()
                await channel.exchange_delete(self.name)

            await retry(
                retry_timeouts=[0],
                exc_filter=lambda e: isinstance(e, aiormq.ChannelPreconditionFailed),
                on_error=on_error,
            )(fn)()

        else:
            await fn()

        if restore:
            self.conn.set_callback(
                "on_open",
                f"on_open_exchange_{self.name}_declare",
                partial(self.declare, timeout=timeout, restore=restore, force=force),
            )

    async def publish(
        self,
        data: bytes,
        routing_key: str,
        properties: dict | None = None,
        timeout: int | None = None,
    ):
        channel = await self.conn.channel()

        logger.debug(
            "Exchange[name='%s'] channel[%s] publish[routing_key='%s'] %s",
            self.name,
            channel,
            routing_key,
            data if not LOG_SANITIZE else "<hiden>",
        )

        await channel.basic_publish(
            data,
            exchange=self.name,
            routing_key=routing_key,
            properties=BasicProperties(**(properties or {})),
            timeout=timeout or self.timeout,
        )


@dataclass(slots=True, frozen=True)
class Consumer:
    """Consumer class."""

    channel: aiormq.abc.AbstractChannel
    consumer_tag: str

    async def close(self):
        logger.debug("%s close", self)
        await self.channel.close()


@dataclass(slots=True, frozen=True)
class Queue:
    """RabbitMQ queue class."""

    name: str
    type: QueueType = QueueType.CLASSIC
    durable: bool = False
    auto_delete: bool = False
    prefetch_count: int | None = 1
    max_priority: int | None = None
    expires: int | None = None
    msg_ttl: int | None = None
    timeout: int | None = None
    conn: Connection = None  # type: ignore
    conn_factory: Callable = field(default=None, repr=False)  # type: ignore
    consumer: Consumer | None = field(default=None, init=False)
    bindings: list[tuple[Exchange, str]] = field(default_factory=list, init=False)

    def __post_init__(self):
        if all((self.conn, self.conn_factory)):
            raise Exception("conn and conn_factory are incompatible")
        if not any((self.conn, self.conn_factory)):
            raise Exception("conn or conn_factory is requried")
        if self.conn_factory:
            object.__setattr__(self, "conn", self.conn_factory())
        self.conn.set_callback(
            "on_lost",
            f"on_lost_queue_{self.name}_cleanup_consumer",
            lambda: object.__setattr__(self, "consumer", None),
        )
        self.conn.set_callback(
            "on_close",
            f"on_close_queue_{self.name}_cleanup_consumer",
            lambda: object.__setattr__(self, "consumer", None),
        )

    async def close(self, delete: bool | None = None, timeout: int | None = None):
        if self.conn.is_closed:
            raise Exception("already closed")

        logger.debug("%s close[delete=%s]", self, delete)

        try:
            await self.stop_consume()
            for exchange, routing_key in self.bindings:
                await self.unbind(exchange, routing_key)
            if self.conn_factory:
                self.conn.remove_callbacks(cancel=True)
            else:
                self.conn.remove_callback("on_open", f"on_open_queue_{self.name}_declare", cancel=True)
            if delete:
                channel = await self.conn.channel()
                try:
                    await channel.queue_delete(self.name, timeout=timeout or self.timeout)
                except aiormq.exceptions.AMQPError as e:
                    logger.warning(e)
        finally:
            if self.conn_factory:
                await self.conn.close()

    async def declare(
        self,
        timeout: int | None = None,
        restore: bool | None = None,
        force: bool | None = None,
    ):
        logger.debug("%s declare[restore=%s, force=%s]", self, restore, force)

        async def fn():
            channel = await self.conn.channel()
            arguments = {
                "x-queue-type": self.type,
            }
            if self.max_priority:
                arguments["x-max-priority"] = self.max_priority
            if self.expires:
                arguments["x-expires"] = int(self.expires) * 1000
            if self.msg_ttl:
                arguments["x-message-ttl"] = int(self.msg_ttl) * 1000
            await channel.queue_declare(
                self.name,
                durable=self.durable,
                auto_delete=self.auto_delete,
                arguments=arguments,
                timeout=timeout or self.timeout,
            )

        if force:

            async def on_error(e):
                channel = await self.conn.channel()
                await channel.queue_delete(self.name)

            await retry(
                retry_timeouts=[0],
                exc_filter=lambda e: isinstance(e, aiormq.ChannelPreconditionFailed),
                on_error=on_error,
            )(fn)()

        else:
            await fn()

        if restore:
            self.conn.set_callback(
                "on_open",
                f"on_open_queue_{self.name}_declare",
                partial(self.declare, timeout=timeout, restore=restore, force=force),
            )

    async def bind(
        self,
        exchange: Exchange,
        routing_key: str,
        timeout: int | None = None,
        restore: bool | None = None,
    ):
        logger.debug(
            "Bind queue '%s' to exchange '%s' with routing_key '%s'",
            self.name,
            exchange.name,
            routing_key,
        )

        channel = await self.conn.channel()
        await channel.queue_bind(
            self.name,
            exchange.name,
            routing_key=routing_key,
            timeout=timeout or self.timeout,
        )

        if (exchange, routing_key) not in self.bindings:
            self.bindings.append((exchange, routing_key))

        if restore:
            self.conn.set_callback(
                "on_open",
                f"on_open_queue_{self.name}_bind_{exchange.name}_{routing_key}",
                partial(self.bind, exchange, routing_key, timeout=timeout, restore=restore),
            )

    async def unbind(self, exchange: Exchange, routing_key: str, timeout: int | None = None):
        logger.debug(
            "Unbind queue '%s' from exchange '%s' for routing_key '%s'",
            self.name,
            exchange.name,
            routing_key,
        )

        if (exchange, routing_key) in self.bindings:
            self.bindings.remove((exchange, routing_key))

            channel = await self.conn.channel()
            await channel.queue_unbind(
                self.name,
                exchange.name,
                routing_key=routing_key,
                timeout=timeout or self.timeout,
            )

            self.conn.remove_callback(
                "on_open",
                f"on_open_queue_{self.name}_bind_{exchange.name}_{routing_key}",
                cancel=True,
            )

    async def consume(
        self,
        callback: Callable[[aiormq.Channel, aiormq.abc.DeliveredMessage], Coroutine],
        prefetch_count: int | None = None,
        timeout: int | None = None,
        retry_timeout: int = 5,
    ):
        if self.consumer is None:
            channel = await self.conn.new_channel()
            await channel.basic_qos(
                prefetch_count=prefetch_count or self.prefetch_count,
                timeout=timeout or self.timeout,
            )

            object.__setattr__(
                self,
                "consumer",
                Consumer(
                    channel=channel,
                    consumer_tag=(
                        await channel.basic_consume(
                            self.name,
                            partial(callback, channel),
                            timeout=timeout or self.timeout,
                        )
                    ).consumer_tag,
                ),
            )

            logger.info("%s consuming", self)

            self.conn.set_callback(
                "on_lost",
                f"on_lost_queue_{self.name}_consume",
                partial(
                    retry(
                        retry_timeouts=repeat(retry_timeout),
                        exc_filter=lambda e: True,
                    )(self.consume),
                    callback,
                    prefetch_count=prefetch_count,
                    timeout=timeout,
                ),
            )

        return self.consumer

    async def stop_consume(self, timeout: int | None = None):
        logger.debug("%s stop consuming", self)

        self.conn.remove_callback("on_lost", f"on_lost_queue_{self.name}_consume", cancel=True)

        if self.consumer and not self.consumer.channel.is_closed:
            await self.consumer.channel.basic_cancel(self.consumer.consumer_tag, timeout=timeout)
            await self.consumer.close()
            object.__setattr__(self, "consumer", None)
