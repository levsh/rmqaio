# RMQaio

Async RabbitMQ library with spec-based API, topology restore, and automatic reconnection.
Built on [aiormq](https://github.com/mosquito/aiormq).

## Installation

```bash
pip install rmqaio
```

## Quick Start

```python
import asyncio
from rmqaio import Connection, Exchange, ExchangeSpec, Ops, Queue, QueueSpec

async def main():
    conn = Connection("amqp://localhost")
    ops = Ops(conn, timeout=30)

    exchange = Exchange(ExchangeSpec(name="events", type="topic", durable=True), ops)
    await exchange.declare(restore=True)

    queue = Queue(QueueSpec(name="my-queue", durable=True), ops)
    await queue.declare(restore=True)
    await queue.bind("events", routing_key="#", restore=True)

    async def callback(channel, msg):
        print(f"Got: {msg.body.decode()}")
        await channel.basic_ack(msg.delivery.tag)

    await queue.consume(callback, auto_ack=False)

    await exchange.publish(b"hello", routing_key="user.created")

    await asyncio.sleep(1)
    await conn.close()

asyncio.run(main())
```

## Connection

### Creating a connection

```python
conn = Connection("amqp://localhost")
await conn.open()
```

Connection URL supports query parameters: `amqp://host?connection_timeout=30000` (ms, converted to seconds).

SSL and TLS:

```python
# Via ssl_context parameter (full control)
from ssl import SSLContext

conn = Connection("amqp://host:5671", ssl_context=SSLContext())

# Via amqps:// scheme (automatic TLS, no custom certs)
conn = Connection("amqps://host:5671")

# Via query parameters (certs + verification)
conn = Connection("amqps://host:5671?cafile=/etc/ca.pem&certfile=/etc/client.pem&keyfile=/etc/client.key")
conn = Connection("amqps://host:5671?no_verify_ssl=1")
```

The `amqps://` scheme enables TLS. Parameters like `cafile`, `certfile`, `keyfile`, `no_verify_ssl` are passed through the URL. Use `ssl_context` when you need advanced certificate handling (e.g. in-memory certs, custom verification).

### Cached vs new channels

```python
ch1 = await conn.channel()
ch2 = await conn.channel()
assert ch1 is ch2  # same cached channel

ch1 = await conn.new_channel()
ch2 = await conn.new_channel()
assert ch1 is not ch2  # always fresh
```

Use `channel()` for general operations, `new_channel()` for consumers.

### SharedConnection

Shares one underlying `Connection` across instances with identical parameters. Reference-counted — closes only after all callers call `close()`.

```python
from rmqaio import SharedConnection

conn1 = SharedConnection("amqp://localhost")
conn2 = SharedConnection("amqp://localhost")

await conn1.open()
await conn2.open()
# ... work ...
await conn1.close()
await conn2.close()  # underlying connection closes now
```

### Graceful shutdown

```python
await conn.close()
```

Or with context manager:

```python
async with Connection("amqp://localhost") as conn:
    ops = Ops(conn)
```

## Reconnection & reliability

When the broker goes away, RMQaio automatically reconnects, redeclares exchanges and queues, rebinds, and restarts consumers.

### Connection states

- `INITIAL` — created, not opened
- `CONNECTING` — first connection attempt
- `RECONNECTING` — reconnecting after loss
- `CONNECTED` — operational
- `REFRESHING` — manual refresh in progress
- `CLOSING` / `CLOSED` — shut down

### Retry policy

Control first-connect and reconnect attempts separately:

```python
from rmqaio import Connection, RetryPolicy, Repeat

conn = Connection(
    "amqp://localhost",
    open_retry_policy=RetryPolicy(delays=[1, 2, 5]),       # 3 attempts
    reopen_retry_policy=RetryPolicy(delays=Repeat(5)),     # infinite, every 5s
)
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `delays` | `Repeat(5)` | Delays between retries. `Repeat(n)` = infinite, finite list = limited attempts |
| `exc_filter` | `TimeoutError`, `ConnectionError`, `AMQPConnectionError` | Exceptions to retry on, or a callable |

On non-retryable exceptions (e.g. auth failure), the connection stays closed and the error propagates.

### Callbacks

Subscribe to state transitions:

```python
async def on_state_change(state_from, state_to):
    print(f"{state_from} -> {state_to}")

conn.set_callback("my-handler", on_state_change)
await conn.remove_callback("my-handler")
```

Useful for metrics, logging, or circuit breakers.

### Topology restore

When `restore=True` is passed during declaration, the resource is tracked internally and redeclared after reconnect:

```python
await exchange.declare(restore=True)
await queue.declare(restore=True)
await queue.bind("events", "#", restore=True)
await queue.consume(callback, restore=True)
```

## Declaring resources

### Specs

Requests are built with typed dataclasses:

| Spec | Purpose | Key fields |
|------|---------|------------|
| `ExchangeSpec` | Declare an exchange | `name`, `type`, `durable`, `arguments` |
| `DelayedExchangeSpec` | Delayed message exchange (plugin) | `name`, `arguments.delayed_type` |
| `DefaultExchangeSpec` | Built-in default exchange | `none` (name is always `""`) |
| `QueueSpec` | Declare a queue | `name`, `durable`, `arguments` |
| `BindSpec` | Bind queue or exchange | `src`, `dst`, `routing_key`, `kind` |
| `ConsumerSpec` | Start a consumer | `queue`, `callback`, `prefetch_count`, `auto_ack` |

```python
from rmqaio import ExchangeSpec, QueueSpec, BindSpec, ExchangeArgs, QueueArgs

exchange = ExchangeSpec(
    name="orders",
    type="topic",
    durable=True,
    arguments=ExchangeArgs(alternate_exchange="dlx"),
)

queue = QueueSpec(
    name="order-events",
    durable=True,
    arguments=QueueArgs(
        queue_type="quorum",
        dead_letter_exchange="dlx",
        message_ttl=60000,
    ),
)

binding = BindSpec(src="orders", dst="order-events", routing_key="order.*")
```

### Exchange and Queue wrappers

Convenience objects that pair a spec with an `Ops` instance:

```python
ex = Exchange(ExchangeSpec(name="orders", type="topic", durable=True), ops)
await ex.declare(restore=True)
await ex.publish(b"data", routing_key="order.created")
await ex.bind("other-exchange", routing_key="orders.#")
await ex.delete()

q = Queue(QueueSpec(name="tasks", durable=True), ops)
await q.declare(restore=True)
await q.bind("orders", routing_key="task.*")
await q.consume(callback, prefetch_count=10, auto_ack=False)
await q.delete()
```

### force parameter

RabbitMQ raises `ChannelPreconditionFailed` when you declare a resource that already exists with different parameters. Pass `force=True` to delete and recreate on mismatch:

```python
await ex.declare(force=True)
```

## Publishing

```python
await ops.publish("events", b'{"user": "alice"}', routing_key="user.created")

# With properties
await ops.publish(
    "events",
    b"data",
    routing_key="key",
    properties={"delivery_mode": 2, "content_type": "application/json"},
    mandatory=True,
)
```

## Consuming

```python
async def callback(channel, msg):
    print(f"Received: {msg.body.decode()}")
    await channel.basic_ack(msg.delivery.tag)

consumer = await queue.consume(callback, prefetch_count=10, auto_ack=False)

# Stop specific consumer
await queue.stop_consume(consumer.consumer_tag)

# Stop all
await queue.stop_consume()
```

### Auto-ack vs manual

| Mode | Usage | Guarantee |
|------|-------|-----------|
| `auto_ack=True` (default) | At-most-once delivery. Callback runs, message is acked automatically. | Fast, loss possible on crash |
| `auto_ack=False` | At-least-once delivery. You call `basic_ack` / `basic_nack`. | Safe, requires explicit ack |

```python
# Auto-ack (lossy, fast)
await queue.consume(callback)

# Manual ack (safe)
async def safe_callback(channel, msg):
    try:
        await channel.basic_ack(msg.delivery.tag)
    except Exception:
        await channel.basic_nack(msg.delivery.tag, requeue=True)

await queue.consume(safe_callback, auto_ack=False)
```

## Ops & Topology

### Direct ops usage

`Ops` is the core handler. All Exchange/Queue wrapper methods delegate to it:

```python
from rmqaio import Ops

ops = Ops(conn, timeout=30)

await ops.declare(exchange_spec, restore=True)
await ops.declare(queue_spec, restore=True, force=True)
await ops.bind(bind_spec, restore=True)
await ops.publish("events", b"data", routing_key="key")
await ops.consume(consumer_spec, restore=True)
await ops.stop_consume(consumer_tag)
```

### apply_topology

Declare an entire topology at once:

```python
from rmqaio import Topology, ExchangeSpec, QueueSpec, BindSpec

topology = Topology(
    exchanges=[ExchangeSpec(name="events", type="topic")],
    queues=[QueueSpec(name="my-queue")],
    bindings=[BindSpec(src="events", dst="my-queue", routing_key="#")],
)

await ops.apply_topology(topology, consume=True, restore=True)
```

## Exceptions

- **`RmqAioError`** — base class for all library errors
- **`ConnectionInvalidStateError`** — operation attempted in an invalid state (e.g. reopening a closed connection)
- **`OperationError`** — operation not allowed (e.g. declaring or deleting a read-only exchange or queue)

---

Full API reference: [rmqaio reference](api_reference.md)
