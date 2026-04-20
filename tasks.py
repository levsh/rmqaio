import os

from invoke import task


PYTHON_TAG = os.environ.get("PYTHON_TAG", "3.11-alpine")


@task
def ensure_images(c):
    """Prepare docker images"""

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


@task
def build_docs(c):
    """Build documentation."""

    cmd = "poetry run mkdocs build -f docs/mkdocs.yml"
    c.run(cmd)


@task
def run_docs(c):
    """Run local HTTP server with documentation on http://localhost:8000."""

    cmd = "poetry run python -m http.server -d docs/site"
    print("Serving on http://localhost:8000...")
    c.run(cmd)
