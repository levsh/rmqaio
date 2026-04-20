import asyncio

from os import path
from unittest.mock import AsyncMock

import aiormq
import pytest

import rmqaio

from rmqaio import ConnectionState
from tests.utils import (
    assert_has_channel,
    assert_has_connection,
    assert_has_exchange,
    assert_has_not_channel,
    assert_has_not_connection,
    assert_has_not_exchange,
    assert_has_not_queue,
    assert_has_queue,
)


CWD = path.dirname(path.dirname(path.abspath(__file__)))


@pytest.mark.integration
class TestRMQaio:
    @pytest.mark.asyncio
    async def test_invalid_connection(self, rabbitmq):
        conn = rmqaio.SharedConnection(
            "amqp://invalid",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1]),
        )
        with pytest.raises(aiormq.exceptions.AMQPConnectionError):
            await conn.open()

    @pytest.mark.asyncio
    async def test_connection(self, rabbitmq, api):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        await conn.open()
        assert conn.is_open is True
        assert conn.is_closed is False
        await assert_has_connection(api)

        channel = await conn.channel()
        await assert_has_channel(api)
        assert await conn.channel() == await conn.channel()

        await conn.close()
        assert conn.is_open is False
        assert conn.is_closed is True
        await assert_has_not_connection(api)
        await assert_has_not_channel(api)

    @pytest.mark.asyncio
    async def test_connection_tls(self, rabbitmq_tls):
        conn = rmqaio.SharedConnection(
            (
                f"amqps://{rabbitmq_tls['ip']}:{rabbitmq_tls['port']}"
                "?auth=plain"
                f"&certfile={CWD}/files/rabbitmq/tls/client/cert.pem"
                f"&keyfile={CWD}/files/rabbitmq/tls/client/key.pem"
                "&no_verify_ssl=1"
            ),
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        await conn.open()
        assert conn.is_open is True
        assert conn.is_closed is False
        await conn.close()
        assert conn.is_open is False
        assert conn.is_closed is True

    @pytest.mark.asyncio
    async def test_connection_tls_invalid_path(self, rabbitmq_tls):
        conn = rmqaio.SharedConnection(
            (
                f"amqps://{rabbitmq_tls['ip']}:{rabbitmq_tls['port']}"
                "?auth=plain"
                f"&certfile=/invalid/cert.pem"
                f"&keyfile=/invalid/key.pem"
                "&no_verify_ssl=1"
            ),
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        try:
            await conn.open()
        except FileNotFoundError:
            pass
        else:
            assert False

        assert conn.is_open is False
        assert conn.is_closed is False
        await conn.close()

    @pytest.mark.asyncio
    async def test_lost_connection(self, rabbitmq):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        try:
            await conn.open()

            for _ in range(2):
                await asyncio.sleep(0.5)

                rabbitmq["container"].stop()

                await asyncio.sleep(1)

                rabbitmq["container"].start()

                await asyncio.sleep(1)

                for _ in range(20):
                    if conn.is_open:
                        break
                    await asyncio.sleep(1)
                    continue
                else:
                    assert False
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_stuck_connection(self, rabbitmq):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}?heartbeat=2",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        try:
            await conn.open()

            await asyncio.sleep(0.5)

            rabbitmq["container"].pause()

            await asyncio.sleep(15)

            assert conn.is_open is False

            rabbitmq["container"].unpause()

            await asyncio.sleep(1)

            for _ in range(20):
                if conn.is_open:
                    break
                await asyncio.sleep(1)
                continue
            else:
                assert False

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_refresh_connection(self, rabbitmq):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        try:
            await conn.open()
            assert conn.is_open

            await conn.refresh()
            await asyncio.sleep(1)
            assert conn.is_open
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_close_while_connecting(self, rabbitmq):
        conn = rmqaio.Connection(
            "amqp://localhost:7777",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        asyncio.create_task(conn.open())
        await asyncio.sleep(1)
        await conn.close()
        assert conn._state == ConnectionState.CLOSED
        assert conn._conn is None
        assert conn._channel is None
        assert conn._loop_task is None
        assert conn._connected_event.is_set() is False
        assert conn._closed_event.is_set() is True

    @pytest.mark.asyncio
    async def test_default_exchange(self, rabbitmq):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        try:
            rmqaio.DefaultExchange(rmqaio.Ops(conn))
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_exchange(self, rabbitmq, api):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        exchange = rmqaio.Exchange(
            rmqaio.ExchangeSpec(name="test"),
            rmqaio.Ops(conn),
        )
        try:
            await exchange.declare()
            await assert_has_exchange(api, "test")
            assert await exchange.check_exists() is True
            await exchange.delete()
            await assert_has_not_exchange(api, "test")
            assert await exchange.check_exists() is False
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_queue(self, rabbitmq, api):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        queue = rmqaio.Queue(
            rmqaio.QueueSpec(name="test"),
            rmqaio.Ops(conn),
        )
        try:
            await queue.declare()
            await assert_has_queue(api, "test")
            assert await queue.check_exists() is True
            await queue.delete()
            await assert_has_not_queue(api, "test")
            assert await queue.check_exists() is False
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_publish(self, rabbitmq):
        rmqaio.config.log_sanitize = False

        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        exchange = rmqaio.Exchange(
            rmqaio.ExchangeSpec(name="test"),
            rmqaio.Ops(conn),
        )
        queue = rmqaio.Queue(
            rmqaio.QueueSpec(name="test"),
            rmqaio.Ops(conn),
        )

        flag = AsyncMock()

        async def cb(ch, msg):
            if msg.body == b"hello!":
                await flag()

        try:
            await exchange.declare()
            await queue.declare()
            await queue.bind("test", routing_key="key")
            consumer = await queue.consume(cb)
            await exchange.publish(b"hello!", routing_key="key")
            await asyncio.sleep(1)
            flag.assert_awaited_once()
            await queue.stop_consume(consumer.consumer_tag)
            await exchange.publish(b"hello!", routing_key="key")
            await asyncio.sleep(1)
            flag.assert_awaited_once()
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_restore_topology(self, rabbitmq):
        conn = rmqaio.SharedConnection(
            f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
            open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
        )
        ops = rmqaio.Ops(conn)
        exchange = rmqaio.Exchange(
            rmqaio.ExchangeSpec(name="test"),
            ops,
        )
        queue = rmqaio.Queue(
            rmqaio.QueueSpec(name="test"),
            ops,
        )

        flag = AsyncMock()

        async def cb(ch, msg):
            if msg.body == b"hello!":
                await flag()

        try:
            await exchange.declare(restore=True)
            await queue.declare(restore=True)
            await queue.bind("test", routing_key="key", restore=True)
            consumer = await queue.consume(cb, restore=True)
            await exchange.publish(b"hello!", routing_key="key")
            await asyncio.sleep(1)
            flag.assert_awaited_once()
            await conn.refresh()
            await asyncio.sleep(1)
            await exchange.publish(b"hello!", routing_key="key")
            await asyncio.sleep(1)
            assert flag.await_count == 2
        finally:
            await conn.close()


class TestExamples:
    @pytest.mark.asyncio
    async def test_basic_usage(self, rabbitmq):
        import asyncio

        from rmqaio import Connection, Exchange, ExchangeSpec, Ops, Queue, QueueSpec, RetryPolicy

        async def main():
            conn = Connection(
                f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}",
                open_retry_policy=rmqaio.RetryPolicy(delays=[1, 3, 5, 5, 5, 5]),
            )

            ops = Ops(conn, timeout=30)

            exchange_spec = ExchangeSpec(
                name="my-exchange",
                type="direct",
                durable=True,
            )
            exchange = Exchange(exchange_spec, ops)
            await exchange.declare(restore=True)

            queue_spec = QueueSpec(name="my-queue", durable=True)
            queue = Queue(queue_spec, ops)
            await queue.declare(restore=True)

            await queue.bind(exchange="my-exchange", routing_key="my-key", restore=True)

            async def callback(channel, msg):
                print(f"Received message: {msg.body.decode()}")
                await channel.basic_ack(msg.delivery.tag)

            await queue.consume(callback, auto_ack=False)

            await exchange.publish(data=b"Hello, World!", routing_key="my-key")

            await asyncio.sleep(1)

            await conn.close()

        await main()
