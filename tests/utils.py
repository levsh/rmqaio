import asyncio
import platform
import time

from contextlib import contextmanager
from urllib.parse import quote_plus

import docker


def get_ip(container, internal: bool = False):
    if internal or platform.system() != "Darwin":
        if "IPAddress" in container.attrs["NetworkSettings"]:
            ip = container.attrs["NetworkSettings"]["IPAddress"]
        else:
            ip = container.attrs["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
    else:
        ip = "127.0.0.1"
    return ip


class ContainerExecutor:
    def __init__(self):
        self.client = docker.from_env()
        self.kwds = {"detach": True}

    @contextmanager
    def create(self, image, **kwds):
        container_kwds = self.kwds.copy()
        container_kwds.update(kwds)
        container = self.client.containers.create(image, **container_kwds)
        try:
            yield container
        except Exception as e:
            print(container.logs().decode())
            raise e
        finally:
            container.stop()
            container.remove(v=True)

    @contextmanager
    def run(self, image, **kwds):
        with self.create(image, **kwds) as container:
            container.start()
            yield container

    @contextmanager
    def run_wait_up(self, image, **kwds):
        with self.run(image, **kwds) as container:
            tend = time.monotonic() + 30
            while (container.status != "running" or container.health != "healthy") and time.monotonic() < tend:
                time.sleep(0.5)
                container.reload()
            container.reload()
            if container.status != "running" or container.health != "healthy":
                print()
                print(container.logs().decode())
                print(container.status)
                print(container.health)
                print()
                raise Exception(f"container '{container.name}' error")
            yield container

    @contextmanager
    def run_wait_exit(self, image, **kwds):
        with self.run(image, **kwds) as container:
            container.reload()
            container.wait()
            yield container


RETRIES = 10


def retry(fn):

    async def wrapper(*args, **kwds):
        attempts = 5
        for attempt in range(1, attempts + 1):
            try:
                return await fn(*args, **kwds)
            except Exception as e:
                if attempt >= attempts:
                    raise e
                await asyncio.sleep(1)

    return wrapper


@retry
async def assert_has_connection(api):
    for _ in range(RETRIES):
        resp = api.get("/api/connections")
        if len(resp.json()) == 1:
            break
        await asyncio.sleep(1)
    else:
        assert False


@retry
async def assert_has_not_connection(api):
    for _ in range(RETRIES):
        resp = api.get("/api/connections")
        if len(resp.json()) == 0:
            break
        await asyncio.sleep(1)
    else:
        assert False


@retry
async def assert_has_channel(api):
    for _ in range(RETRIES):
        resp = api.get("/api/channels")
        if len(resp.json()) == 1:
            break
        await asyncio.sleep(1)
    else:
        assert False


@retry
async def assert_has_not_channel(api):
    for _ in range(RETRIES):
        resp = api.get("/api/channels")
        if len(resp.json()) == 0:
            break
        await asyncio.sleep(1)
    else:
        assert False


@retry
async def assert_has_exchange(api, name):
    for _ in range(RETRIES):
        resp = api.get(f"/api/exchanges/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 200:
            break
        await asyncio.sleep(1)
    else:
        assert False


@retry
async def assert_has_not_exchange(api, name):
    for _ in range(RETRIES):
        resp = api.get(f"/api/exchanges/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 404:
            break
        await asyncio.sleep(1)
    else:
        assert False


@retry
async def assert_has_queue(api, name):
    for _ in range(RETRIES):
        resp = api.get(f"/api/queues/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 200:
            break
        await asyncio.sleep(1)
    else:
        assert False


@retry
async def assert_has_not_queue(api, name):
    for _ in range(RETRIES):
        resp = api.get(f"/api/queues/{quote_plus('/')}/{quote_plus(name)}")
        if resp.status_code == 404:
            break
        await asyncio.sleep(1)
    else:
        assert False
