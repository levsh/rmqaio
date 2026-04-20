from unittest.mock import AsyncMock, MagicMock

import pytest

from rmqaio import DefaultExchange, Exchange, ExchangeSpec, Queue, QueueSpec


@pytest.fixture
def mock_ops():
    ops = AsyncMock()
    ops.publish = AsyncMock()
    ops.check_exchange_exists = AsyncMock()
    ops.exchange_declare = AsyncMock()
    ops.delete = AsyncMock()
    ops.bind = AsyncMock()
    ops.unbind = AsyncMock()
    ops.check_queue_exists = AsyncMock()
    ops.queue_declare = AsyncMock()
    ops.queue_delete = AsyncMock()
    ops.consume = AsyncMock()
    return ops


class TestDefaultExchange:
    @pytest.mark.asyncio
    async def test_spec_is_default(self, mock_ops):
        default_exchange = DefaultExchange(ops=mock_ops)
        assert default_exchange.spec.name == ""
        assert default_exchange.spec.kind == "default"
        assert default_exchange.spec.type == "direct"

    @pytest.mark.asyncio
    async def test_publish(self, mock_ops):
        default_exchange = DefaultExchange(ops=mock_ops)
        await default_exchange.publish(b"test_data", "test.key")
        mock_ops.publish.assert_called_once_with(
            "",  # default exchange name
            b"test_data",
            "test.key",
            properties=None,
            mandatory=False,
            timeout=None,
        )

    @pytest.mark.asyncio
    async def test_publish_with_properties(self, mock_ops):
        default_exchange = DefaultExchange(ops=mock_ops)
        properties = {"content_type": "application/json"}
        await default_exchange.publish(b"test_data", "test.key", properties=properties, mandatory=True, timeout=30)
        mock_ops.publish.assert_called_once_with(
            "",
            b"test_data",
            "test.key",
            properties=properties,
            mandatory=True,
            timeout=30,
        )


class TestExchange:
    def test_init(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        assert exchange.spec is spec
        assert exchange.ops is mock_ops

    @pytest.mark.asyncio
    async def test_check_exists(self, mock_ops):
        mock_ops.check_exchange_exists = AsyncMock(return_value=True)
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        result = await exchange.check_exists(timeout=30)
        mock_ops.check_exchange_exists.assert_called_once_with("test_exchange", timeout=30)
        assert result is True

    @pytest.mark.asyncio
    async def test_declare(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        await exchange.declare(restore=True, timeout=30)
        mock_ops.exchange_declare.assert_called_once_with(spec, restore=True, timeout=30, force=None)

    @pytest.mark.asyncio
    async def test_declare_with_force(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        await exchange.declare(force=True)
        mock_ops.exchange_declare.assert_called_once_with(spec, restore=None, timeout=None, force=True)

    @pytest.mark.asyncio
    async def test_delete(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        await exchange.delete(timeout=30)
        mock_ops.delete.assert_called_once_with(spec, timeout=30)

    @pytest.mark.asyncio
    async def test_bind(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        await exchange.bind("parent_exchange", "routing.key", restore=True, timeout=30)
        mock_ops.bind.assert_called_once()
        call_args = mock_ops.bind.call_args[0][0]
        assert call_args.src == "parent_exchange"
        assert call_args.dst == "test_exchange"
        assert call_args.routing_key == "routing.key"

    @pytest.mark.asyncio
    async def test_unbind(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        await exchange.unbind("parent_exchange", "routing.key", timeout=30)
        mock_ops.unbind.assert_called_once()
        call_args = mock_ops.unbind.call_args[0][0]
        assert call_args.src == "parent_exchange"
        assert call_args.dst == "test_exchange"
        assert call_args.routing_key == "routing.key"

    @pytest.mark.asyncio
    async def test_publish(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        await exchange.publish(b"test_data", "test.key")
        mock_ops.publish.assert_called_once_with(
            "test_exchange",
            b"test_data",
            "test.key",
            properties=None,
            mandatory=False,
            timeout=None,
        )

    @pytest.mark.asyncio
    async def test_publish_with_properties(self, mock_ops):
        spec = ExchangeSpec(name="test_exchange", type="fanout", kind="normal")
        exchange = Exchange(spec=spec, ops=mock_ops)
        properties = {"content_type": "application/json"}
        await exchange.publish(b"test_data", "test.key", properties=properties, mandatory=True, timeout=30)
        mock_ops.publish.assert_called_once_with(
            "test_exchange",
            b"test_data",
            "test.key",
            properties=properties,
            mandatory=True,
            timeout=30,
        )


class TestQueue:
    def test_init(self, mock_ops):
        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)
        assert queue.spec is spec
        assert queue.ops is mock_ops

    @pytest.mark.asyncio
    async def test_check_exists(self, mock_ops):
        mock_ops.check_queue_exists = AsyncMock(return_value=True)
        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)
        result = await queue.check_exists(timeout=30)
        mock_ops.check_queue_exists.assert_called_once_with("test_queue", timeout=30)
        assert result is True

    @pytest.mark.asyncio
    async def test_declare(self, mock_ops):
        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)
        await queue.declare(restore=True, timeout=30)
        mock_ops.queue_declare.assert_called_once_with(spec, restore=True, timeout=30, force=None)

    @pytest.mark.asyncio
    async def test_declare_with_force(self, mock_ops):
        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)
        await queue.declare(force=True)
        mock_ops.queue_declare.assert_called_once_with(spec, restore=None, timeout=None, force=True)

    @pytest.mark.asyncio
    async def test_delete(self, mock_ops):
        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)
        await queue.delete(timeout=30)
        mock_ops.delete.assert_called_once_with(spec, timeout=30)

    @pytest.mark.asyncio
    async def test_bind(self, mock_ops):
        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)
        await queue.bind("test_exchange", "routing.key", restore=True, timeout=30)
        mock_ops.bind.assert_called_once()
        call_args = mock_ops.bind.call_args[0][0]
        assert call_args.src == "test_exchange"
        assert call_args.dst == "test_queue"
        assert call_args.routing_key == "routing.key"

    @pytest.mark.asyncio
    async def test_unbind(self, mock_ops):
        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)
        await queue.unbind("test_exchange", "routing.key", timeout=30)
        mock_ops.unbind.assert_called_once()
        call_args = mock_ops.unbind.call_args[0][0]
        assert call_args.src == "test_exchange"
        assert call_args.dst == "test_queue"
        assert call_args.routing_key == "routing.key"

    @pytest.mark.asyncio
    async def test_consume(self, mock_ops):
        from rmqaio.rmqaio import Consumer

        mock_ops.consume = AsyncMock(return_value=Consumer(MagicMock(), "tag", MagicMock()))

        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)

        async def callback(channel, message):
            pass

        await queue.consume(callback, prefetch_count=10, auto_ack=False)
        mock_ops.consume.assert_called_once()
        call_args = mock_ops.consume.call_args[0][0]
        assert call_args.queue == "test_queue"
        assert call_args.callback is callback

    @pytest.mark.asyncio
    async def test_consume_with_restore(self, mock_ops):
        from rmqaio.rmqaio import Consumer

        mock_ops.consume = AsyncMock(return_value=Consumer(MagicMock(), "tag", MagicMock()))

        spec = QueueSpec(name="test_queue", kind="normal")
        queue = Queue(spec=spec, ops=mock_ops)

        async def callback(channel, message):
            pass

        await queue.consume(callback, restore=True)
        mock_ops.consume.assert_called_once()
        call_kwargs = mock_ops.consume.call_args[1]
        assert call_kwargs["restore"] is True
