# RMQaio

A Python library for RabbitMQ built around [aiormq](https://github.com/mosquito/aiormq)

## Installation

```bash
pip install rmqaio
```

## Features

The library provides a simple and intuitive interface for working with RabbitMQ:

### Connection Management

- **`Connection`** - Connection to RabbitMQ with automatic reconnection.
- **`SharedConnection`** - Shares a single underlying connection across multiple instances with identical parameters.
    Uses reference counting - the underlying connection closes only when all references are released.

### Spec-Based API

The library uses a specification-driven approach:

- **`ExchangeSpec`** / **`QueueSpec`** / **`BindSpec`** - Define exchanges, queues, and bindings.
- **`ConsumerSpec`** - Define message consumers.
- **`Ops`** - Handler for all RabbitMQ operations.

### Operation Handling

- **`Ops`** - Central handler for declare, bind, publish, consume operations.
- **`Topology`** - Tracks exchanges, queues, bindings, and consumers for auto-restore.

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `RMQAIO_LOG_SANITIZE` | Replace user data in logs with `<hidden>` | True |
| `RMQAIO_LOG_DATA_TRUNCATE_SIZE` | Maximum size of data to log before truncation | 10000 |

## Basic Usage Example

```python
import asyncio

from rmqaio import Connection, Exchange, ExchangeSpec, Ops, Queue, QueueSpec, RetryPolicy

async def main():
    conn = Connection(f"amqp://{rabbitmq['ip']}:{rabbitmq['port']}")

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

asyncio.run(main())
```

## Connection

### Creating a Connection

```python
from rmqaio import Connection

conn = Connection("amqp://localhost")
await conn.open()
```

### channel() vs new_channel()

- **`channel()`** - Returns cached channel or creates new one.
- **`new_channel()`** - Always creates a new channel.

```python
ch1 = await conn.channel()
ch2 = await conn.channel()
assert ch1 is ch2

ch1 = await conn.new_channel()
ch2 = await conn.new_channel()
assert ch1 is not ch2
```

### SharedConnection

Shares connection across multiple instances:

```python
from rmqaio import SharedConnection, Ops

conn1 = SharedConnection("amqp://localhost")

conn2 = SharedConnection("amqp://localhost")

# conn1 and conn2 share the same underlying connection
```

## Retry Policy

The `RetryPolicy` class controls connection failure handling:

```python
from rmqaio import RetryPolicy, Repeat

policy = RetryPolicy(
    delays=Repeat(5),
    exc_filter=lambda e: isinstance(e, (ConnectionError, asyncio.TimeoutError))
)
```

### Parameters

- **`delays`** - Delays for retry attempts.
- **`exc_filter`** - Exception types or callable to filter retriable exceptions.

## Spec Classes

### DefaultExchangeSpec

The default exchange (empty string name):

```python
from rmqaio import DefaultExchangeSpec

spec = DefaultExchangeSpec()
```

### ExchangeSpec

```python
from rmqaio import ExchangeSpec

spec = ExchangeSpec(
    name="my-exchange",
    type='topic',
    durable=True,
    auto_delete=False,
    arguments=ExchangeArgs(alternate_exchange="dlx")
)
```

### DelayedExchangeSpec

```python
from rmqaio import DelayedExchangeSpec, DelayedExchangeArgs

spec = DelayedExchangeSpec(
    name="delayed-exchange",
    arguments=DelayedExchangeArgs(delayed_type="topic")
)
```

### QueueSpec

```python
from rmqaio import QueueSpec, QueueType, QueueArgs

spec = QueueSpec(
    name="my-queue",
    durable=True,
    arguments=QueueArgs(
        queue_type="quorum",
        dead_letter_exchange="dlx",
        message_ttl=60000,
        max_length=1000,
    )
)
```

### BindSpec

```python
from rmqaio import BindSpec

# Bind queue to exchange
bind_spec = BindSpec(
    kind="queue",
    src="my-exchange",
    dst="my-queue",
    routing_key="my-key",
)

# Bind exchange to exchange
bind_spec = BindSpec(
    kind="exchange",
    src="parent-exchange",
    dst="my-exchange",
    routing_key="my.key",
)
```

### ConsumerSpec

```python
from rmqaio import ConsumerSpec, ConsumerArgs

async def callback(channel, msg):
    await channel.basic_ack(msg.delivery.tag)

spec = ConsumerSpec(
    queue="my-queue",
    callback=callback,
    prefetch_count=10,
    auto_ack=False,
    exclusive=False,
    consumer_tag="my-consumer",
    arguments=ConsumerArgs(priority=5),
)
```

## Exchange Operations

```python
from rmqaio import Connection, Exchange, ExchangeSpec, Ops

conn = Connection("amqp://localhost")
ops = Ops(conn)
spec = ExchangeSpec(name="orders", type='topic', durable=True)
exchange = Exchange(spec, ops)

await exchange.declare()

await exchange.publish(
    data=b"Hello",
    routing_key="order.created",
    properties={"delivery_mode": 2},
)

await exchange.delete()
```

## Queue Operations

```python
from rmqaio import Connection, Ops, Queue, QueueSpec

conn = Connection("amqp://localhost")
ops = Ops(conn)
spec = QueueSpec(name="notifications", durable=True)
queue = Queue(spec, ops)

await queue.declare()

await queue.bind('exchange', routing_key="order.*")

await queue.consume(callback, auto_ack=False)

await queue.stop_consume(consumer_tag)

await queue.delete()
```

## Ops Handler

The `Ops` class is the central handler for all RabbitMQ operations:

```python
from rmqaio import Ops

ops = Ops(conn, timeout=30)

await ops.declare(exchange_spec)
await ops.bind(bind_spec)
await ops.publish("exchange", b"data", "routing.key")
await ops.consume(consumer_spec)
```

### Methods

- `check_exists(spec)` - Check if exchange or queue exists
- `declare(spec)` - Declare exchange or queue
- `delete(spec)` - Delete exchange or queue
- `bind(spec)` - Bind queue/exchange to exchange
- `unbind(spec)` - Unbind from exchange
- `publish(exchange, data, routing_key)` - Publish message
- `consume(spec)` - Start consuming
- `stop_consume(consumer_tag)` - Stop consuming
- `ensure_topology()` - Restore all topology on reconnect

## Topology and restore

The `restore=True` parameter enables automatic restoration after reconnect:

```python
await ops.declare(..., restore=True)
await ops.bind(..., restore=True)
await ops.consume(..., restore=True)
```
On connection loss, the `Ops` handler automatically redeclares all resources.

## force Parameter

Force recreation when declaration parameters differ:

```python
await ops.declare(..., force=True)
```

## Callbacks

Subscribe to connection state changes:

```python
async def on_state_change(state_from, state_to):
    print(f"Connection: {state_from} -> {state_to}")

conn.set_callback("state_handler", on_state_change)
await conn.remove_callback("state_handler")
```

### Connection States

- `INITIAL` - Connection created
- `CONNECTING` - Connecting to broker
- `CONNECTED` - Connected and operational
- `REFRESHING` - Refreshing connection
- `CLOSING` - Closing connection
- `CLOSED` - Connection closed

## Message Acknowledgment

### Auto Ack (default)

```python
async def callback(channel, msg):
    print(f"Received: {msg.body.decode()}")

await queue.consume(callback)  # auto_ack=True by default
```

### Manual Ack

```python
async def callback(channel, msg):
    try:
        await channel.basic_ack(msg.delivery.tag)
    except Exception:
        await channel.basic_nack(msg.delivery.tag, requeue=True)

await queue.consume(callback, auto_ack=False)
```

## Graceful Shutdown

```python
await conn.close()
```

Or use context manager:

```python
async with Connection("amqp://localhost") as conn:
    ops = Ops(conn)
    # work with ops
```
