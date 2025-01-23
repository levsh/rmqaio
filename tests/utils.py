import platform
import time

from contextlib import contextmanager

import docker


def get_ip(container):
    if platform.system() == "Darwin":
        ip = "127.0.0.1"
    else:
        ip = container.attrs["NetworkSettings"]["IPAddress"]
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
