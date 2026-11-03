"""Arranca uvicorn leyendo PORT del entorno (distroless no tiene shell)."""

import os
import runpy
import sys


def main() -> None:
    sys.argv = [
        "uvicorn",
        "main:app",
        "--host",
        "0.0.0.0",
        "--port",
        os.environ.get("PORT", "8000"),
    ]
    runpy.run_module("uvicorn", run_name="__main__")


if __name__ == "__main__":
    main()
