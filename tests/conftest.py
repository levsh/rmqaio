import asyncio
import locale
import logging
import sys

from os import path
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from docker.errors import DockerException

import rmqaio

from tests import utils


CWD = path.dirname(path.abspath(__file__))

rmqaio.config.log_sanitize = False

logger = logging.getLogger("rmqaio")
log_frmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s lineno:%(lineno)4d -- %(message)s")
log_hndl = logging.StreamHandler(stream=sys.stdout)
log_hndl.setFormatter(log_frmt)

logger.addHandler(log_hndl)
logger.setLevel(logging.DEBUG)


@pytest.fixture(autouse=True)
def set_en_locale(monkeypatch):
    monkeypatch.setenv("LANG", "en_US.UTF-8")
    monkeypatch.setenv("LANGUAGE", "en_US.UTF-8")
    locale.setlocale(locale.LC_ALL, "en_US.UTF-8")


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests that require external services like Docker")


@pytest.fixture(scope="class")
def container_executor():
    try:
        yield utils.ContainerExecutor()
    except DockerException:
        pytest.skip("Docker daemon is not available")


@pytest.fixture(scope="function")
def rabbitmq(container_executor):
    with container_executor.run_wait_up(
        "rabbitmq:3-management",
        ports={"5672": "5672", "15672": "15672"},
        healthcheck={
            "test": ["CMD", "rabbitmq-diagnostics", "-q", "ping"],
            "interval": 10**9,
            "timeout": 10**9,
            "retries": 60,
        },
    ) as container:
        ip = utils.get_ip(container)
        port = 5672
        yield {"container": container, "ip": ip, "port": port}


@pytest.fixture(scope="function")
def rabbitmq_tls(container_executor):
    f = path.join(CWD, "files/rabbitmq")
    with container_executor.run_wait_up(
        "rabbitmq:3-management",
        volumes={
            f: {"bind": "/etc/rabbitmq/configs", "mode": "ro"},
        },
        ports={"5671": "5671", "15671": "15671"},
        environment={
            "RABBITMQ_CONFIG_FILE": "/etc/rabbitmq/configs/rabbitmq.conf",
        },
        healthcheck={
            "test": ["CMD", "rabbitmq-diagnostics", "-q", "ping"],
            "interval": 10**9,
            "timeout": 10**9,
            "retries": 60,
        },
    ) as container:
        ip = utils.get_ip(container)
        port = 5671
        yield {"container": container, "ip": ip, "port": port}


@pytest.fixture(scope="function")
def api(rabbitmq):
    return httpx.Client(base_url=f"http://{rabbitmq['ip']}:15672", auth=("guest", "guest"))


@pytest.fixture
def mock_aiormq():
    event_loop = asyncio.get_event_loop()
    conn = MagicMock()
    conn.connect = AsyncMock()
    conn.closing = event_loop.create_future()
    conn.close = AsyncMock()

    def make_mock_channel():
        channel = AsyncMock()
        channel.is_closed = False
        return channel

    conn.channel = AsyncMock(side_effect=lambda *args, **kwargs: make_mock_channel())

    with mock.patch("aiormq.connect") as m:
        m.return_value = conn
        yield m


@pytest.fixture
def mock_channel():
    channel = AsyncMock()
    channel.is_closed = False
    channel.exchange_declare = AsyncMock()
    channel.exchange_delete = AsyncMock()
    channel.queue_declare = AsyncMock()
    channel.queue_delete = AsyncMock()
    channel.queue_bind = AsyncMock()
    channel.queue_unbind = AsyncMock()
    channel.exchange_bind = AsyncMock()
    channel.exchange_unbind = AsyncMock()
    channel.basic_publish = AsyncMock()
    channel.basic_qos = AsyncMock()
    channel.basic_consume = AsyncMock()
    channel.basic_cancel = AsyncMock()
    channel.new_channel = AsyncMock()
    return channel


@pytest.fixture
def mock_conn(mock_channel):
    conn = MagicMock()
    conn.set_callback = MagicMock()
    conn.channel = AsyncMock(return_value=mock_channel)
    conn.new_channel = AsyncMock(return_value=mock_channel)
    return conn
