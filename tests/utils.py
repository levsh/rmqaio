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

    def dump_logs(self, container):
        try:
            container.reload()
            return container.logs().decode()
        except Exception as e:
            return f"failed to read logs: {e}"

    @contextmanager
    def create(self, image, **kwds):
        container_kwds = self.kwds.copy()
        container_kwds.update(kwds)
        container = self.client.containers.create(image, **container_kwds)
        try:
            yield container
        except Exception:
            print("=== CONTAINER LOGS ===")
            print(self.dump_logs(container))
            raise
        finally:
            try:
                container.stop(timeout=10)
            except Exception:
                pass
            try:
                container.remove(force=True, v=True)
            except Exception:
                pass

    @contextmanager
    def run(self, image, **kwds):
        with self.create(image, **kwds) as container:
            container.start()
            yield container

    def wait_healthy(self, container, timeout=60):
        start = time.monotonic()

        while time.monotonic() - start < timeout:
            container.reload()

            status = container.status
            health = container.attrs.get("State", {}).get("Health", {}).get("Status")

            if status == "running" and (health in (None, "healthy")):
                return container

            if status == "exited":
                print("=== CONTAINER LOGS (EXITED EARLY) ===")
                print(self.dump_logs(container))
                break

            time.sleep(1)

        raise RuntimeError(self.dump_logs(container))

    @contextmanager
    def run_wait_up(self, image, **kwds):
        with self.run(image, **kwds) as container:
            container = self.wait_healthy(container)
            yield container

    @contextmanager
    def run_wait_exit(self, image, **kwds):
        with self.run(image, **kwds) as container:
            try:
                yield container
            except Exception:
                print("=== CONTAINER LOGS ===")
                print(self.dump_logs(container))
                raise
            finally:
                container.wait()


RETRIES = 10


def retry(fn):

    async def wrapper(*args, **kwds):
        attempts = 5
        for attempt in range(1, attempts + 1):
            try:
                return await fn(*args, **kwds)
            except Exception:
                if attempt >= attempts:
                    raise
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
