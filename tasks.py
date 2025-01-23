import os

from invoke import task


PYTHON_TAG = os.environ.get("PYTHON_TAG", "3.11-alpine")


@task
def ensure_images(c):
    """Build docker images"""

    images = [
        "rabbitmq:3-management",
    ]

    for image in images:
        c.run(f"docker pull {image}")


@task
def run_linters(c):
    """Run linters"""

    cmd = "poetry run ruff check rmqaio"
    c.run(cmd)


@task
def run_tests(c):
    """Run tests"""

    cmd = (
        "poetry run coverage run --data-file=artifacts/.coverage --source rmqaio "
        "-m pytest -v --maxfail=1 tests/ && "
        "poetry run coverage json --data-file=artifacts/.coverage -o artifacts/coverage.json && "
        "poetry run coverage report --data-file=artifacts/.coverage -m"
    )
    c.run(cmd)
