from unittest.mock import AsyncMock, MagicMock

import aiormq
import pytest

from rmqaio import BindSpec, ConsumerSpec, ExchangeSpec, Ops, QueueSpec, Topology
from rmqaio.rmqaio import OperationError


@pytest.fixture
def ops(mock_conn):
    return Ops(mock_conn, timeout=30)


class TestOpsInit:
    def test_init_default(self, mock_conn):
        ops = Ops(mock_conn)
        assert ops._conn is mock_conn
        assert ops._timeout is None
        assert ops._topology is not None
        assert ops._consumers == {}

    def test_init_with_timeout(self, mock_conn):
        ops = Ops(mock_conn, timeout=60)
        assert ops._timeout == 60

    def test_set_callback(self, mock_conn):
        Ops(mock_conn)
        mock_conn.set_callback.assert_called_once()
        callback_name = mock_conn.set_callback.call_args[0][0]
        assert "on_state_changed" in callback_name


class TestOpsCheckExists:
    @pytest.mark.asyncio
    async def test_check_exists_exchange(self, ops, mock_channel):
        mock_channel.exchange_declare = AsyncMock()
        result = await ops.check_exists(ExchangeSpec(kind="normal", name="test", type="direct"))
        assert result is True
        mock_channel.exchange_declare.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_exists_exchange_not_found(self, ops, mock_channel):
        mock_channel.exchange_declare = AsyncMock(side_effect=aiormq.ChannelNotFoundEntity("not found"))
        result = await ops.check_exists(ExchangeSpec(kind="normal", name="test", type="direct"))
        assert result is False

    @pytest.mark.asyncio
    async def test_check_exists_queue(self, ops, mock_channel):
        mock_channel.queue_declare = AsyncMock()
        result = await ops.check_exists(QueueSpec(kind="normal", name="test"))
        assert result is True
        mock_channel.queue_declare.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_exists_queue_not_found(self, ops, mock_channel):
        mock_channel.queue_declare = AsyncMock(side_effect=aiormq.ChannelNotFoundEntity("not found"))
        result = await ops.check_exists(QueueSpec(kind="normal", name="test"))
        assert result is False

    @pytest.mark.asyncio
    async def test_check_exists_invalid_spec(self, ops):
        with pytest.raises(ValueError, match="invalid spec type"):
            await ops.check_exists("invalid")


class TestOpsDeclare:
    @pytest.mark.asyncio
    async def test_declare_exchange(self, ops, mock_channel):
        spec = ExchangeSpec(name="test_exchange")
        await ops.declare(spec)
        mock_channel.exchange_declare.assert_called_once()

    @pytest.mark.asyncio
    async def test_declare_exchange_with_restore(self, ops):
        spec = ExchangeSpec(name="test_exchange")
        await ops.declare(spec, restore=True)
        assert spec in ops._topology.exchanges

    @pytest.mark.asyncio
    async def test_declare_queue(self, ops, mock_channel):
        spec = QueueSpec(name="test_queue")
        await ops.declare(spec)
        mock_channel.queue_declare.assert_called_once()

    @pytest.mark.asyncio
    async def test_declare_queue_with_restore(self, ops):
        spec = QueueSpec(name="test_queue")
        await ops.declare(spec, restore=True)
        assert spec in ops._topology.queues

    @pytest.mark.asyncio
    async def test_declare_invalid_spec(self, ops):
        with pytest.raises(ValueError, match="invalid spec type"):
            await ops.declare("invalid")


class TestOpsDelete:
    @pytest.mark.asyncio
    async def test_delete_exchange(self, ops, mock_channel):
        spec = ExchangeSpec(name="test_exchange")
        ops._topology.exchanges.append(spec)
        await ops.delete(spec)
        mock_channel.exchange_delete.assert_called_once()
        assert spec not in ops._topology.exchanges

    @pytest.mark.asyncio
    async def test_delete_queue(self, ops, mock_channel):
        spec = QueueSpec(name="test_queue")
        ops._topology.queues.append(spec)
        await ops.delete(spec)
        mock_channel.queue_delete.assert_called_once()
        assert spec not in ops._topology.queues

    @pytest.mark.asyncio
    async def test_delete_read_only_exchange(self, ops):
        spec = ExchangeSpec(kind="read-only", name="test_exchange", type="direct")
        with pytest.raises(OperationError, match="can not delete read-only exchange"):
            await ops.delete(spec)

    @pytest.mark.asyncio
    async def test_delete_read_only_queue(self, ops):
        spec = QueueSpec(kind="read-only", name="test_queue")
        with pytest.raises(OperationError, match="can not delete read-only queue"):
            await ops.delete(spec)


class TestOpsExchangeDeclare:
    @pytest.mark.asyncio
    async def test_exchange_declare(self, ops, mock_channel):
        spec = ExchangeSpec(name="test_exchange")
        await ops.exchange_declare(spec)
        mock_channel.exchange_declare.assert_called_once()

    @pytest.mark.asyncio
    async def test_exchange_declare_with_restore(self, ops):
        spec = ExchangeSpec(name="test_exchange")
        await ops.exchange_declare(spec, restore=True)
        assert spec in ops._topology.exchanges

    @pytest.mark.asyncio
    async def test_exchange_declare_ro(self, ops):
        spec = ExchangeSpec(kind="read-only", name="test_exchange", type="direct")
        with pytest.raises(OperationError, match="can not declare read-only exchange"):
            await ops.exchange_declare(spec)


class TestOpsExchangeDelete:
    @pytest.mark.asyncio
    async def test_exchange_delete(self, ops, mock_channel):
        spec = ExchangeSpec(name="test_exchange")
        ops._topology.exchanges.append(spec)
        await ops.exchange_delete(spec.name)
        mock_channel.exchange_delete.assert_called_once()
        assert spec not in ops._topology.exchanges


class TestOpsCheckQueueExists:
    @pytest.mark.asyncio
    async def test_check_queue_exists(self, ops):
        result = await ops.check_queue_exists("test_queue")
        assert result is True

    @pytest.mark.asyncio
    async def test_check_queue_exists_not_found(self, ops, mock_channel):
        mock_channel.queue_declare = AsyncMock(side_effect=aiormq.ChannelNotFoundEntity("not found"))
        result = await ops.check_queue_exists("test_queue")
        assert result is False


class TestOpsQueueDeclare:
    @pytest.mark.asyncio
    async def test_queue_declare(self, ops, mock_channel):
        spec = QueueSpec(name="test_queue")
        await ops.queue_declare(spec)
        mock_channel.queue_declare.assert_called_once()

    @pytest.mark.asyncio
    async def test_queue_declare_with_restore(self, ops):
        spec = QueueSpec(name="test_queue")
        await ops.queue_declare(spec, restore=True)
        assert spec in ops._topology.queues

    @pytest.mark.asyncio
    async def test_queue_declare_ro(self, ops):
        spec = QueueSpec(kind="read-only", name="test_queue")
        with pytest.raises(OperationError, match="can not declare read-only queue"):
            await ops.queue_declare(spec)


class TestOpsQueueDelete:
    @pytest.mark.asyncio
    async def test_queue_delete(self, ops, mock_channel):
        spec = QueueSpec(name="test_queue")
        ops._topology.queues.append(spec)
        await ops.queue_delete(spec.name)
        mock_channel.queue_delete.assert_called_once()
        assert spec not in ops._topology.queues


class TestOpsBind:
    @pytest.mark.asyncio
    async def test_bind_queue(self, ops, mock_channel):
        spec = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        await ops.bind(spec)
        mock_channel.queue_bind.assert_called_once()

    @pytest.mark.asyncio
    async def test_bind_queue_with_restore(self, ops):
        spec = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        await ops.bind(spec, restore=True)
        assert spec in ops._topology.bindings

    @pytest.mark.asyncio
    async def test_bind_exchange(self, ops, mock_channel):
        spec = BindSpec(
            src="parent_exchange",
            dst="test_exchange",
            routing_key="test.key",
            kind="exchange",
        )
        await ops.bind(spec)
        mock_channel.exchange_bind.assert_called_once()

    @pytest.mark.asyncio
    async def test_bind_invalid_kind(self, ops):
        spec = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
            kind="invalid",  # type: ignore
        )
        with pytest.raises(ValueError, match="invalid spec type"):
            await ops.bind(spec)


class TestOpsUnbind:
    @pytest.mark.asyncio
    async def test_unbind_queue(self, ops, mock_channel):
        spec = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        ops._topology.bindings.append(spec)
        await ops.unbind(spec)
        mock_channel.queue_unbind.assert_called_once()
        assert spec not in ops._topology.bindings

    @pytest.mark.asyncio
    async def test_unbind_exchange(self, ops, mock_channel):
        spec = BindSpec(
            kind="exchange",
            src="parent_exchange",
            dst="test_exchange",
            routing_key="test.key",
        )
        ops._topology.bindings.append(spec)
        await ops.unbind(spec)
        mock_channel.exchange_unbind.assert_called_once()

    @pytest.mark.asyncio
    async def test_unbind_invalid_kind(self, ops):
        spec = BindSpec(
            kind="invalid",
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        with pytest.raises(ValueError, match="invalid spec type"):
            await ops.unbind(spec)


class TestOpsPublish:
    @pytest.mark.asyncio
    async def test_publish(self, ops, mock_channel):
        await ops.publish("test_exchange", b"test_data", "test.key")
        mock_channel.basic_publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_with_properties(self, ops, mock_channel):
        properties = {"content_type": "application/json"}
        await ops.publish("test_exchange", b"test_data", "test.key", properties=properties)
        mock_channel.basic_publish.assert_called_once()


class TestOpsConsume:
    @pytest.mark.asyncio
    async def test_consume(self, ops, mock_channel):
        async def callback(channel, message):
            pass

        mock_channel.basic_consume = AsyncMock(return_value=MagicMock(consumer_tag="test_tag"))

        spec = ConsumerSpec(queue="test_queue", callback=callback)
        consumer = await ops.consume(spec)

        mock_channel.basic_qos.assert_called_once()
        mock_channel.basic_consume.assert_called_once()
        assert consumer is not None
        assert "test_tag" in ops._consumers

    @pytest.mark.asyncio
    async def test_consume_with_restore(self, ops, mock_channel):
        async def callback(channel, message):
            pass

        mock_channel.basic_consume = AsyncMock(return_value=MagicMock(consumer_tag="test_tag"))

        spec = ConsumerSpec(queue="test_queue", callback=callback)
        await ops.consume(spec, restore=True)

        assert spec in ops._topology.consumers


class TestOpsStopConsume:
    @pytest.mark.asyncio
    async def test_stop_consume(self, ops, mock_channel):
        async def callback(channel, message):
            pass

        mock_channel.basic_consume = AsyncMock(return_value=MagicMock(consumer_tag="test_tag"))
        mock_channel.is_closed = False

        spec = ConsumerSpec(queue="test_queue", callback=callback)
        await ops.consume(spec, restore=True)

        await ops.stop_consume("test_tag")

        mock_channel.basic_cancel.assert_called_once()
        assert "test_tag" not in ops._consumers
        assert spec not in ops._topology.consumers


class TestOpsApplyTopology:
    @pytest.mark.asyncio
    async def test_apply_topology(self, ops, mock_channel):
        exchange_spec = ExchangeSpec(name="test_exchange")
        queue_spec = QueueSpec(name="test_queue")
        binding_spec = BindSpec(src="test_exchange", dst="test_queue", routing_key="test.key")

        topology = Topology()
        topology.exchanges.append(exchange_spec)
        topology.queues.append(queue_spec)
        topology.bindings.append(binding_spec)

        await ops.apply_topology(topology)
        mock_channel.exchange_declare.assert_called_once()
        mock_channel.queue_declare.assert_called_once()
        mock_channel.queue_bind.assert_called_once()

    @pytest.mark.asyncio
    async def test_apply_topology_with_restore(self, ops, mock_channel):
        exchange_spec = ExchangeSpec(name="test_exchange")
        queue_spec = QueueSpec(name="test_queue")
        binding_spec = BindSpec(src="test_exchange", dst="test_queue", routing_key="test.key")

        topology = Topology()
        topology.exchanges.append(exchange_spec)
        topology.queues.append(queue_spec)
        topology.bindings.append(binding_spec)

        await ops.apply_topology(topology, restore=True)
        assert exchange_spec in ops._topology.exchanges
        assert queue_spec in ops._topology.queues
        assert binding_spec in ops._topology.bindings

    @pytest.mark.asyncio
    async def test_apply_topology_with_consume(self, ops, mock_channel):
        async def callback(channel, message):
            pass

        mock_channel.basic_consume = AsyncMock(return_value=MagicMock(consumer_tag="test_tag"))
        mock_channel.basic_qos = AsyncMock()

        exchange_spec = ExchangeSpec(name="test_exchange")
        queue_spec = QueueSpec(name="test_queue")
        binding_spec = BindSpec(src="test_exchange", dst="test_queue", routing_key="test.key")
        consumer_spec = ConsumerSpec(queue="test_queue", callback=callback)

        topology = Topology()
        topology.exchanges.append(exchange_spec)
        topology.queues.append(queue_spec)
        topology.bindings.append(binding_spec)
        topology.consumers.append(consumer_spec)

        await ops.apply_topology(topology, consume=True)
        mock_channel.basic_consume.assert_called_once()
