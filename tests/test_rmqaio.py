import asyncio

from itertools import repeat
from unittest import mock
from urllib.parse import quote_plus

import httpx
import pytest

import rmqaio


def test_LoopIter():
    it = rmqaio.rmqaio._LoopIter(["a", "b"])
    for idx in range(3):
        assert next(it) == "a", idx
        assert next(it) == "b", idx
        with pytest.raises(StopIteration):
            next(it)

    it = rmqaio.rmqaio._LoopIter(["a", "b"])
    for idx in range(3):
        assert next(it) == "a", idx
        it.reset()
        assert next(it) == "a", idx
        assert next(it) == "b", idx
        with pytest.raises(StopIteration):
            next(it)

    assert next(it) == "a"
    assert next(it) == "b"
    it.reset()
    assert next(it) == "b"
    assert next(it) == "a"
    with pytest.raises(StopIteration):
        next(it)
    assert next(it) == "b"
    assert next(it) == "a"
    with pytest.raises(StopIteration):
        next(it)
    assert next(it) == "b"
    assert next(it) == "a"
    with pytest.raises(StopIteration):
        next(it)


async def assert_has_connection(api):
    for _ in range(10):
        resp = api.get("/api/connections")
        if len(resp.json()) == 1:
            break
        await asyncio.sleep(1)
    else:
        assert False


async def assert_has_not_connection(api):
    for _ in range(10):
        resp = api.get("/api/connections")
        if len(resp.json()) == 0:
            break
        await asyncio.sleep(1)
    else:
        assert False


async def assert_has_channel(api):
    for _ in range(10):
        resp = api.get("/api/channels")
        if len(resp.json()) == 1:
            break
        await asyncio.sleep(1)
    else:
        assert False


async def assert_has_not_channel(api):
    for _ in range(10):
        resp = api.get("/api/channels")
        if len(resp.json()) == 0:
            break
        await asyncio.sleep(1)
    else:
        assert False


async def assert_has_exchange(api, name):
    for _ in range(10):
        resp = api.get(f"/api/exchanges/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 200:
            break
        await asyncio.sleep(1)
    else:
        assert False


async def assert_has_not_exchange(api, name):
    for _ in range(10):
        resp = api.get(f"/api/exchanges/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 404:
            break
        await asyncio.sleep(1)
    else:
        assert False


async def assert_has_queue(api, name):
    for _ in range(10):
        resp = api.get(f"/api/queues/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 200:
            break
        await asyncio.sleep(1)
    else:
        assert False


async def assert_has_not_queue(api, name):
    for _ in range(10):
        resp = api.get(f"/api/queues/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 404:
            break
        await asyncio.sleep(1)
    else:
        assert False


@pytest.fixture
def mock_connect():
    with mock.patch("aiormq.connect") as m:
        mock_conn = mock.AsyncMock()
        mock_conn.is_closed = False
        mock_conn.closing = asyncio.Future()
        m.return_value = mock_conn
        yield m


class TestConnection:
    @pytest.mark.asyncio
    async def test__init(self):
        conn = rmqaio.Connection("amqp://admin@example.com", name="abc")
        try:
            assert str(conn.url) == "amqp://admin@example.com"
            assert conn.name == "abc"
            assert conn._key is not None
            assert conn._key in conn._Connection__shared
            assert conn._shared["objs"] == 1
            assert conn in conn._shared
            assert conn._shared[conn] == {
                "on_open": {},
                "on_lost": {},
                "on_close": {},
                "callback_tasks": {"on_close": {}, "on_lost": {}, "on_open": {}},
            }
            assert conn._conn is None
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test__init_custom(self):
        conn = rmqaio.Connection(["amqp://admin@rabbitmq1.com", "amqp://admin@rabbitmq2.com"])
        try:
            assert str(conn.url) == "amqp://admin@rabbitmq1.com"
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_callbacks(self):
        on_open_cb_flag = False
        on_close_cb_flag = False

        def on_open_cb():
            nonlocal on_open_cb_flag
            on_open_cb_flag = not on_open_cb_flag

        def on_close_cb():
            nonlocal on_close_cb_flag
            on_close_cb_flag = not on_close_cb_flag

        def cb_with_exception():
            raise Exception

        conn = rmqaio.Connection("amqp://admin@example.com")
        try:
            conn.set_callback("on_open", "test", on_open_cb)
            assert conn._shared[conn]["on_open"]["test"] == on_open_cb
            conn.set_callback("on_close", "test", on_close_cb)
            assert conn._shared[conn]["on_close"]["test"] == on_close_cb

            await conn._execute_callbacks("on_open")
            assert on_open_cb_flag is True
            assert on_close_cb_flag is False

            conn.remove_callback("on_open", "test")
            assert conn._shared[conn]["on_open"] == {}
            assert conn._shared[conn]["on_close"]["test"] == on_close_cb

            await conn._execute_callbacks("on_close")
            assert on_open_cb_flag is True
            assert on_close_cb_flag is True

            conn.remove_callbacks(cancel=True)
            assert conn._shared[conn]["on_open"] == {}
            assert conn._shared[conn]["on_close"] == {}
            assert conn._shared[conn]["callback_tasks"] == {"on_close": {}, "on_lost": {}, "on_open": {}}

            conn.set_callback("on_open", "excpetion", cb_with_exception)
            await conn._execute_callbacks("on_open")
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_connect(self, mock_connect):
        conn = rmqaio.Connection("amqp://admin@example.com")
        try:
            await conn.open()
            mock_connect.assert_awaited_once_with("amqp://admin@example.com", context=None)
            assert conn.is_open is True
            assert conn.is_closed is False
            assert conn._conn is not None
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_channel(self, mock_connect):
        conn = rmqaio.Connection("amqp://admin@example.com")
        try:
            await conn.open()
            await conn.channel()
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_str(self):
        conn = rmqaio.Connection(["amqp://admin@example.com"], name="abc")
        try:
            assert str(conn) == "Connection[example.com]#abc"
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_repr(self):
        conn = rmqaio.Connection(["amqp://admin@example.com"], name="abc")
        try:
            assert repr(conn) == "Connection[example.com]#abc"
        finally:
            await conn.close()


class TestRMQAIO:
    @pytest.mark.asyncio
    async def test_connection(self, rabbitmq):
        api = httpx.Client(base_url=f"http://{rabbitmq['ip']}:15672", auth=("guest", "guest"))

        conn = rmqaio.Connection(
            [
                "amqp://invalid",
                f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            ],
            name="abc",
            retry_timeouts=[1, 3, 5],
        )
        assert conn
        assert f"{conn}" == f"Connection[invalid]#abc"
        assert conn.is_open is False
        assert conn.is_closed is False

        try:
            await conn.open()
            assert conn.is_open is True
            assert conn.is_closed is False

            await assert_has_connection(api)

            channel = await conn.channel()
            assert await conn.channel() == channel
            await assert_has_channel(api)

            await conn.close()
            assert conn.is_open is False
            assert conn.is_closed is True
            await assert_has_not_connection(api)
            await assert_has_not_channel(api)

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_exchange(self, rabbitmq):
        api = httpx.Client(base_url=f"http://{rabbitmq['ip']}:15672", auth=("guest", "guest"))

        exchange = rmqaio.Exchange(
            conn_factory=lambda: rmqaio.Connection(
                [f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}"],
            )
        )
        try:
            await exchange.declare()
        finally:
            await exchange.close(delete=True)

        exchange = rmqaio.Exchange(
            name="test",
            conn_factory=lambda: rmqaio.Connection(
                [
                    "amqp://invalid",
                    f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
                ],
                retry_timeouts=[1, 3, 5],
            ),
        )
        try:
            await exchange.declare()
            await assert_has_exchange(api, "test")
        finally:
            await exchange.close(delete=True)
            await assert_has_not_exchange(api, "test")

    @pytest.mark.asyncio
    async def test_queue(self, rabbitmq):
        api = httpx.Client(base_url=f"http://{rabbitmq['ip']}:15672", auth=("guest", "guest"))

        queue = rmqaio.Queue(
            name="test",
            conn_factory=lambda: rmqaio.Connection(
                f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}?heartbeat=4",
                retry_timeouts=[1, 3, 5],
            ),
        )
        try:
            await queue.declare()
            await assert_has_queue(api, "test")
        finally:
            await queue.close(delete=True)
            await assert_has_not_queue(api, "test")

    @pytest.mark.asyncio
    async def test_lost_connection(self, rabbitmq):
        api = httpx.Client(base_url=f"http://{rabbitmq['ip']}:15672", auth=("guest", "guest"))

        conn = rmqaio.Connection(
            [
                "amqp://invalid1?connection_timeout=1000",
                "amqp://invalid2?connection_timeout=2000",
                f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            ],
            name="abc",
            retry_timeouts=repeat(1),
        )

        try:
            exchange = rmqaio.Exchange(name="test", conn=conn)
            await exchange.declare(restore=True)

            queue = rmqaio.Queue(name="test", conn=conn)
            await queue.declare(restore=True)

            rabbitmq["container"].stop()

            await asyncio.sleep(3)

            rabbitmq["container"].start()

            await asyncio.sleep(5)

            for _ in range(20):
                if conn.is_open:
                    break
                await asyncio.sleep(1)
                continue
            else:
                assert False

        finally:
            await conn.close()
