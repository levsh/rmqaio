## Описание

RMQaio - библиотека для удобной работы с RabbitMQ.


## Установка

```bash
pip install rmqaio
```


## Состав библиотеки

Библиотека включает в себя всего несколько классов:

- класс `Connection` для создания умного подключения с переподлючением и возможностью пересоздания
    топологии созданных объектов(обменников, очередей, привязок и тд) в случае обрыва соединения.

    Для каждой комбинации `EventLoop`, имя подключения и URL(список URL)
    создается всего одно сетевое подключение к RabbitMQ.

- класс обменника `SimpleExchange`.
    Используется как обменник по умолчанию либо же как обменник, созданный в другом приложении.

- класс обменника `Exchange`.
    Позволяет создавать нужный обменник и удалять его по необходимости.

- класс очереди `Queue`.
    Позволяет создавать нужную очередь, привязывать её к обменнику и слушать сообщения, попадающие в эту очередь.


## Пример использования

```python
from rmqaio import Connection, Exchange, Queue

conn = Connection("amqp://localhost", name="ABC")

exchange = Exchange(name="Test", conn=conn)
await ex.declare(restore=True)

queue = Queue(name="Test", conn=conn)
await queue.declare(restore=True)
await queue.bind(exchange, routing_key="abc", restore=True)

async def callback(channel, msg):
    print("Получено сообщение:", msg)

await queue.consume(callback)

data = "Привет!"
await exchange.publish(data.encode(), routing_key="abc")

await queue.close(delete=True)
await exchange.close(delete=True)
await conn.close()
```
