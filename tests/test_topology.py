import pytest

from rmqaio import BindSpec, ConsumerSpec, ExchangeSpec, QueueSpec, Topology


class TestTopology:
    def test_empty(self):
        topology = Topology()
        assert len(topology.exchanges) == 0
        assert len(topology.queues) == 0
        assert len(topology.bindings) == 0
        assert len(topology.consumers) == 0

    def test_with_exchanges(self):
        topology = Topology()
        spec = ExchangeSpec(name="test_exchange")
        topology.exchanges.append(spec)
        assert len(topology.exchanges) == 1
        assert topology.exchanges[0] == spec

    def test_with_queues(self):
        topology = Topology()
        spec = QueueSpec(name="test_queue")
        topology.queues.append(spec)
        assert len(topology.queues) == 1
        assert topology.queues[0] == spec

    def test_with_bindings(self):
        topology = Topology()
        spec = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        topology.bindings.append(spec)
        assert len(topology.bindings) == 1
        assert topology.bindings[0] == spec

    @pytest.mark.asyncio
    async def test_with_consumers(self):
        topology = Topology()

        async def callback(channel, message):
            pass

        spec = ConsumerSpec(queue="test_queue", callback=callback)
        topology.consumers.append(spec)
        assert len(topology.consumers) == 1
        assert topology.consumers[0] == spec

    def test_all(self):
        topology = Topology()

        exchange_spec = ExchangeSpec(name="test_exchange")
        queue_spec = QueueSpec(name="test_queue")
        binding_spec = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )

        topology.exchanges.append(exchange_spec)
        topology.queues.append(queue_spec)
        topology.bindings.append(binding_spec)

        assert len(topology.exchanges) == 1
        assert len(topology.queues) == 1
        assert len(topology.bindings) == 1
        assert len(topology.consumers) == 0

    def test_multiple_exchanges(self):
        topology = Topology()
        spec1 = ExchangeSpec(name="exchange1")
        spec2 = ExchangeSpec(name="exchange2")
        topology.exchanges.append(spec1)
        topology.exchanges.append(spec2)
        assert len(topology.exchanges) == 2

    def test_multiple_queues(self):
        topology = Topology()
        spec1 = QueueSpec(name="queue1")
        spec2 = QueueSpec(name="queue2")
        topology.queues.append(spec1)
        topology.queues.append(spec2)
        assert len(topology.queues) == 2
