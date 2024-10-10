import logging
import platform
import time

import httpx
import pytest

import rmqaio

from rmqaio import logger
from tests import utils


logger.setLevel(logging.INFO)
rmqaio.LOG_SANITIZE = False


@pytest.fixture(scope="function")
def container_executor():
    _container_executor = utils.ContainerExecutor()
    try:
        yield _container_executor
    finally:
        for container in _container_executor.containers:
            container.stop()
            container.remove(v=True)


@pytest.fixture(scope="function")
def rabbitmq(container_executor):
    container = container_executor.run_wait_up(
        "rabbitmq:3-management",
        ports={"5672": "5672", "15672": "15672"},
    )
    if platform.system() == "Darwin":
        ip, port = "127.0.0.1", 5672
    else:
        ip, port = container.attrs["NetworkSettings"]["IPAddress"], 5672

    try:
        utils.wait_socket_available((ip, port), 20)
    except Exception:
        print("\n")
        print(container.logs().decode())
        raise

    api = httpx.Client(base_url=f"http://{ip}:15672", auth=("guest", "guest"))

    for _ in range(20):
        try:
            resp = api.get(f"/api/vhosts")
            if resp.status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(1)
    else:
        raise Exception

    yield {"container": container, "ip": ip, "port": port}
