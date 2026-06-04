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

Shares a single underlying connection across multiple instances. The connection is established by the first instance that calls `open()` and closed when all instances have called `close()`:

```python
from rmqaio import SharedConnection

conn1 = SharedConnection("amqp://localhost")
conn2 = SharedConnection("amqp://localhost")

await conn1.open()
await conn2.open()

# conn1 and conn2 share the same underlying connection

await conn1.close()
await conn2.close()  # underlying connection closes only after last close
```

### Graceful Shutdown

```python
await conn.close()
```

Or use context manager:

```python
async with Connection("amqp://localhost") as conn:
    pass  # work with connection
```

## Connection Callbacks

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

- **`delays`** - Delays between retry attempts (in seconds). Default: `Repeat(5)` (infinite retries every 5 seconds).
- **`exc_filter`** - Exception types or callable to filter retriable exceptions. Default: `(asyncio.TimeoutError, ConnectionError, aiormq.exceptions.AMQPConnectionError)`.

### Repeat

Represents a constant delay between retries:

- `Repeat(5)` - retry indefinitely every 5 seconds.

## Spec Classes

### DefaultExchangeSpec

The default exchange (empty string name):

```python
from rmqaio import DefaultExchangeSpec

spec = DefaultExchangeSpec()
```

### DefaultExchange

Shortcut wrapper for publishing to the default exchange:

```python
from rmqaio import DefaultExchange, Ops

ops = Ops(conn)
exchange = DefaultExchange(ops)

await exchange.publish(data=b"Hello", routing_key="my.queue")
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
        queue_type="quorum",  # default: "classic"
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

### Consumer

Returned by `consume()` methods:

| Field | Type | Description |
|-------|------|-------------|
| `spec` | `ConsumerSpec` | Consumer specification used to create this consumer |
| `consumer_tag` | `str` | Consumer tag assigned by the broker |
| `channel` | `AbstractChannel` | AMQP channel for this consumer |

## Exchange Operations

```python
from rmqaio import Connection, Exchange, ExchangeSpec, Ops

conn = Connection("amqp://localhost")
ops = Ops(conn)
spec = ExchangeSpec(name="orders", type='topic', durable=True)
exchange = Exchange(spec, ops)

await exchange.declare(restore=True, force=False)

await exchange.publish(
    data=b"Hello",
    routing_key="order.created",
    properties={"delivery_mode": 2},
    mandatory=False,
)

await exchange.check_exists()  # True if declared

await exchange.bind(exchange="other-exchange", routing_key="orders.#", restore=True)

await exchange.unbind(exchange="other-exchange", routing_key="orders.#")

await exchange.delete()
```

## Queue Operations

```python
from rmqaio import Connection, Ops, Queue, QueueSpec

conn = Connection("amqp://localhost")
ops = Ops(conn)
spec = QueueSpec(name="notifications", durable=True)
queue = Queue(spec, ops)

await queue.declare(restore=True)

await queue.check_exists()  # True if declared

await queue.bind(exchange, routing_key="order.*", restore=True)

await queue.unbind(exchange, routing_key="order.*")

consumer = await queue.consume(callback, auto_ack=False, prefetch_count=10)

queue.consumers  # list of active Consumer objects

await queue.stop_consume(consumer_tag)  # stop specific consumer
await queue.stop_consume()  # stop all consumers

await queue.delete()
```

## Ops Handler

The `Ops` class is the central handler for all RabbitMQ operations:

```python
from rmqaio import Ops

ops = Ops(conn, timeout=30)

await ops.declare(exchange_spec, restore=True)
await ops.bind(bind_spec, restore=True)
await ops.publish("exchange", b"data", "routing.key", properties={"delivery_mode": 2})
consumer = await ops.consume(consumer_spec, restore=True)
```

### Methods

**General:**

- `check_exists(spec, timeout)` - Check if exchange or queue exists
- `declare(spec, timeout, restore, force)` - Declare exchange or queue
- `delete(spec, timeout)` - Delete exchange or queue
- `bind(spec, timeout, restore)` - Bind queue/exchange to exchange
- `unbind(spec, timeout)` - Unbind from exchange
- `publish(exchange, data, routing_key, properties, mandatory, timeout)` - Publish message
- `consume(spec, timeout, restore)` - Start consuming (returns `Consumer`)
- `stop_consume(consumer_tag=None, timeout)` - Stop consuming. If `consumer_tag` is `None`, stops all consumers.
- `apply_topology(topology, consume, restore, force)` - Apply entire topology declaration

**Exchange-specific:**

- `check_exchange_exists(name, timeout)` - Check if exchange exists by name
- `exchange_declare(spec, timeout, restore, force)` - Declare an exchange
- `exchange_delete(name, timeout)` - Delete an exchange by name

**Queue-specific:**

- `check_queue_exists(name, timeout)` - Check if queue exists by name
- `get_queue(name, timeout)` - Get queue declare info from broker
- `queue_declare(spec, timeout, restore, force)` - Declare a queue
- `queue_delete(name, timeout)` - Delete a queue by name

## Topology and restore

The `restore=True` parameter enables automatic restoration after reconnect:

```python
await ops.declare(..., restore=True)
await ops.bind(..., restore=True)
await ops.consume(..., restore=True)
```

On connection loss, the `Ops` handler automatically redeclares all resources.

### apply_topology

Apply an entire `Topology` at once:

```python
from rmqaio import Topology

topology = Topology(
    exchanges=[ExchangeSpec(name="my-exchange", ...)],
    queues=[QueueSpec(name="my-queue", ...)],
    bindings=[BindSpec(...)],
    consumers=[ConsumerSpec(...)],
)

await ops.apply_topology(topology, consume=True, restore=True, force=False)
```

## force Parameter

Force recreation when declaration parameters differ:

```python
await ops.declare(..., force=True)
```

## Exceptions

- **`RmqAioError`** - Base exception for all library errors.
- **`ConnectionInvalidStateError`** - Raised when an operation is attempted in an invalid connection state (e.g., reopening a closed connection).
- **`OperationError`** - Raised when an operation is not allowed (e.g., deleting a read-only exchange or queue).

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

## Type Aliases

| Alias | Value |
|-------|-------|
| `Number` | `int \| float` |
| `ExchangeType` | `Literal["direct", "fanout", "topic", "headers"]` |
| `DelayedExchangeType` | `Literal["direct", "fanout", "topic", "headers"]` |
| `QueueType` | `Literal["classic", "quorum", "stream"]` |
| `QueueMode` | `Literal["default", "lazy"]` |
| `OverflowPolicy` | `Literal["drop-head", "reject-publish", "reject-publish-dlx"]` |
