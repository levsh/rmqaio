import logging

from os import path

import pytest

import rmqaio

from rmqaio import logger
from tests import utils


logger.setLevel(logging.DEBUG)
rmqaio.LOG_SANITIZE = False


CWD = path.dirname(path.abspath(__file__))


@pytest.fixture(scope="class")
def container_executor():
    yield utils.ContainerExecutor()


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
        ports={"5671": "5671", "15672": "15672"},
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
