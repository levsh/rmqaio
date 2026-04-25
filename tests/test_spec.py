import pytest

from rmqaio import (
    BaseExchangeArgs,
    BaseQueueArgs,
    BindSpec,
    ConsumerArgs,
    ConsumerSpec,
    DefaultExchangeSpec,
    DelayedExchangeArgs,
    DelayedExchangeSpec,
    ExchangeSpec,
    QueueArgs,
    QueueSpec,
)


class TestDefaultExchangeSpec:
    def test_default_values(self):
        spec = DefaultExchangeSpec()
        assert spec.kind == "default"
        assert spec.name == ""
        assert spec.type == "direct"


class TestExchangeSpec:
    def test_init(self):
        spec = ExchangeSpec(name="test_exchange", type="fanout")
        assert spec.name == "test_exchange"
        assert spec.type == "fanout"
        assert spec.kind == "normal"
        assert spec.durable is True
        assert spec.auto_delete is False
        assert spec.internal is False

    def test_hash(self):
        spec1 = ExchangeSpec(name="test_exchange")
        spec2 = ExchangeSpec(name="test_exchange")
        assert hash(spec1) == hash(spec2)

    def test_hash_different_names(self):
        spec1 = ExchangeSpec(name="exchange1", type="fanout")
        spec2 = ExchangeSpec(name="exchange2", type="fanout")
        assert hash(spec1) != hash(spec2)

    def test_post_init_validation(self):
        with pytest.raises(ValueError, match="use DefaultExchangeSpec"):
            ExchangeSpec(name="")


class TestDelayedExchangeSpec:
    def test_default_values(self):
        spec = DelayedExchangeSpec(name="test_delayed")
        assert spec.name == "test_delayed"
        assert spec.type == "x-delayed-message"
        assert spec.kind == "normal"
        assert spec.durable is True

    def test_arguments_default(self):
        spec = DelayedExchangeSpec(name="test_delayed")
        args = spec.arguments
        assert args.delayed_type == "direct"


class TestQueueSpec:
    def test_init(self):
        spec = QueueSpec(name="test_queue")
        assert spec.name == "test_queue"
        assert spec.kind == "normal"
        assert spec.durable is True
        assert spec.exclusive is False
        assert spec.auto_delete is False

    def test_default_queue_type(self):
        spec = QueueSpec(name="test_queue")
        assert spec.arguments.queue_type == "classic"

    def test_queue_type_classic(self):
        args = QueueArgs(queue_type="classic")
        assert args.queue_type == "classic"

    def test_hash(self):
        spec1 = QueueSpec(name="test_queue")
        spec2 = QueueSpec(name="test_queue")
        assert hash(spec1) == hash(spec2)

    def test_hash_different_names(self):
        spec1 = QueueSpec(name="queue1")
        spec2 = QueueSpec(name="queue2")
        assert hash(spec1) != hash(spec2)


class TestBindSpec:
    def test_init(self):
        spec = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        assert spec.src == "test_exchange"
        assert spec.dst == "test_queue"
        assert spec.routing_key == "test.key"
        assert spec.kind == "queue"

    def test_kind_exchange(self):
        spec = BindSpec(
            src="test_exchange",
            dst="test_exchange",
            routing_key="test.key",
            kind="exchange",
        )
        assert spec.kind == "exchange"

    def test_hash(self):
        spec1 = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        spec2 = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="test.key",
        )
        assert hash(spec1) == hash(spec2)

    def test_hash_different(self):
        spec1 = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="key1",
        )
        spec2 = BindSpec(
            src="test_exchange",
            dst="test_queue",
            routing_key="key2",
        )
        assert hash(spec1) != hash(spec2)


class TestConsumerArgs:
    def test_empty(self):
        args = ConsumerArgs()
        assert args.to_dict() == {}

    def test_priority(self):
        args = ConsumerArgs(priority=10)
        assert args.to_dict()["x-priority"] == 10

    def test_cancel_on_ha_failover(self):
        args = ConsumerArgs(cancel_on_ha_failover=True)
        assert args.to_dict()["x-cancel-on-ha-failover"] is True


class TestConsumerSpec:
    def test_init(self):
        async def callback(channel, message):
            pass

        spec = ConsumerSpec(
            queue="test_queue",
            callback=callback,
        )
        assert spec.queue == "test_queue"
        assert spec.callback is callback
        assert spec.prefetch_count is None
        assert spec.prefetch_size is None
        assert spec.auto_ack is True
        assert spec.exclusive is False
        assert spec.consumer_tag is None

    def test_with_args(self):
        async def callback(channel, message):
            pass

        spec = ConsumerSpec(
            queue="test_queue",
            callback=callback,
            prefetch_count=10,
            prefetch_size=100,
            auto_ack=False,
            exclusive=True,
            consumer_tag="test_tag",
        )
        assert spec.prefetch_count == 10
        assert spec.prefetch_size == 100
        assert spec.auto_ack is False
        assert spec.exclusive is True
        assert spec.consumer_tag == "test_tag"


class TestBaseExchangeArgs:
    def test_empty(self):
        args = BaseExchangeArgs()
        assert args.to_dict() == {}

    def test_alternate_exchange(self):
        args = BaseExchangeArgs(alternate_exchange="my-exchange")
        assert args.to_dict() == {"alternate-exchange": "my-exchange"}

    def test_internal(self):
        args = BaseExchangeArgs(internal=True)
        assert args.to_dict() == {"internal": True}

    def test_custom(self):
        args = BaseExchangeArgs(custom={"x-custom-key": "value"})
        result = args.to_dict()
        assert result["x-custom-key"] == "value"

    def test_all_args(self):
        args = BaseExchangeArgs(
            alternate_exchange="dlx",
            internal=True,
            custom={"x-custom": "value"},
        )
        result = args.to_dict()
        assert result["alternate-exchange"] == "dlx"
        assert result["internal"] is True
        assert result["x-custom"] == "value"


class TestDelayedExchangeArgs:
    def test_empty(self):
        args = DelayedExchangeArgs()
        result = args.to_dict()
        assert result["x-delayed-type"] == "direct"

    def test_with_alternate_exchange(self):
        args = DelayedExchangeArgs(alternate_exchange="my-exchange")
        result = args.to_dict()
        assert result["alternate-exchange"] == "my-exchange"
        assert result["x-delayed-type"] == "direct"


class TestBaseQueueArgs:
    def test_empty(self):
        args = BaseQueueArgs()
        result = args.to_dict()
        assert result == {}

    def test_queue_type(self):
        args = BaseQueueArgs(queue_type="classic")
        result = args.to_dict()
        assert result["x-queue-type"] == "classic"

    def test_dead_letter_exchange(self):
        args = BaseQueueArgs(dead_letter_exchange="dlx")
        result = args.to_dict()
        assert result["x-dead-letter-exchange"] == "dlx"

    def test_dead_letter_routing_key(self):
        args = BaseQueueArgs(dead_letter_routing_key="dlx.key")
        result = args.to_dict()
        assert result["x-dead-letter-routing-key"] == "dlx.key"

    def test_message_ttl(self):
        args = BaseQueueArgs(message_ttl=60000)
        result = args.to_dict()
        assert result["x-message-ttl"] == 60000

    def test_max_length(self):
        args = BaseQueueArgs(max_length=1000)
        result = args.to_dict()
        assert result["x-max-length"] == 1000

    def test_overflow(self):
        args = BaseQueueArgs(overflow="drop-head")
        result = args.to_dict()
        assert result["x-overflow"] == "drop-head"

    def test_single_active_consumer(self):
        args = BaseQueueArgs(single_active_consumer=True)
        result = args.to_dict()
        assert result["x-single-active-consumer"] is True

    def test_custom(self):
        args = BaseQueueArgs(custom={"x-custom": "value"})
        result = args.to_dict()
        assert result["x-custom"] == "value"

    def test_all_args(self):
        args = BaseQueueArgs(
            queue_type="classic",
            dead_letter_exchange="dlx",
            dead_letter_routing_key="dlx.key",
            message_ttl=60000,
            max_length=1000,
            overflow="drop-head",
            single_active_consumer=True,
            custom={"x-custom": "value"},
        )
        result = args.to_dict()
        assert result["x-queue-type"] == "classic"
        assert result["x-dead-letter-exchange"] == "dlx"
        assert result["x-dead-letter-routing-key"] == "dlx.key"
        assert result["x-message-ttl"] == 60000
        assert result["x-max-length"] == 1000
        assert result["x-overflow"] == "drop-head"
        assert result["x-single-active-consumer"] is True
        assert result["x-custom"] == "value"
