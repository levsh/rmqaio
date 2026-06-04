# RMQaio

Async RabbitMQ library with spec-based API, topology restore, and automatic reconnection.

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
    await conn.open()

    ops = Ops(conn)

    exchange = Exchange(ExchangeSpec(name="events", type="topic", durable=True), ops)
    await exchange.declare(restore=True)

    queue = Queue(QueueSpec(name="my-queue", durable=True), ops)
    await queue.declare(restore=True)
    await queue.bind("events", routing_key="#", restore=True)

    await conn.close()

asyncio.run(main())
```

[Documentation](https://levsh.github.io/rmqaio)
