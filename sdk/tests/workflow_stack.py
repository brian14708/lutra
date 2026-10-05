"""Disposable real services for the workflow integration proof."""

from __future__ import annotations

import getpass
import os
import socket
import subprocess  # ruff: ignore[suspicious-subprocess-import]
import time
from typing import TYPE_CHECKING, BinaryIO

import httpx

if TYPE_CHECKING:
    from pathlib import Path
    from uuid import UUID


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Stack:
    def __init__(self, directory: Path, binary: Path, logs: Path) -> None:
        self.directory, self.binary = directory, binary
        self.logs = logs
        logs.mkdir(parents=True, exist_ok=True)
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.files: list[BinaryIO] = []
        postgres, storage, ingress, admin, node, api = [free_port() for _ in range(6)]
        self.env = {
            **os.environ,
            "DATABASE_URL": f"postgres://{getpass.getuser()}@127.0.0.1:{postgres}/postgres?sslmode=disable",
            "LUTRA_ADDR": f"127.0.0.1:{api}",
            "LUTRA_URL": f"http://127.0.0.1:{api}/api",
            "LUTRA_RESTATE_INGRESS": f"http://127.0.0.1:{ingress}",
            "LUTRA_RESTATE_ADMIN": f"http://127.0.0.1:{admin}",
            "LUTRA_RESTATE_CALLBACK": f"http://127.0.0.1:{api}/durable",
            "LUTRA_WORKER_CONCURRENCY": "1",
            "RESTATE_BASE_DIR": str(directory / "restate"),
            "RESTATE_NODE_NAME": "workflow-proof",
            "RESTATE_CLUSTER_NAME": "workflow-proof",
            "RESTATE_BIND_ADDRESS": f"127.0.0.1:{node}",
            "RESTATE_ADVERTISED_ADDRESS": f"http://127.0.0.1:{node}",
            "RESTATE_ADMIN__BIND_ADDRESS": f"127.0.0.1:{admin}",
            "RESTATE_INGRESS__BIND_ADDRESS": f"127.0.0.1:{ingress}",
            "AWS_ENDPOINT_URL_S3": f"http://127.0.0.1:{storage}",
            "AWS_ACCESS_KEY_ID": "lutra",
            "AWS_SECRET_ACCESS_KEY": "lutra-secret",
            "AWS_S3_BUCKET": "lutra",
            "AWS_REGION": "us-east-1",
            "AWS_S3_SECURE": "false",
            "RUSTFS_ACCESS_KEY": "lutra",
            "RUSTFS_SECRET_KEY": "lutra-secret",
            "MC_CONFIG_DIR": str(directory / "mc"),
        }
        self.commands = {
            "postgres": (
                "postgres",
                "-D",
                str(directory / "postgres"),
                "-k",
                str(directory),
                "-p",
                str(postgres),
            ),
            "rustfs": (
                "rustfs",
                "server",
                str(directory / "objects"),
                "--address",
                f"127.0.0.1:{storage}",
            ),
            "restate": ("restate-server", "--no-logo", "--listen-mode=tcp"),
            "server": (str(binary),),
        }

    def run(self, *command: str) -> None:
        subprocess.run(command, env=self.env, check=True, capture_output=True)  # ruff: ignore[subprocess-without-shell-equals-true]

    def sql(self, statement: str, run_id: UUID) -> str:
        result = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true]
            ("psql", "-d", self.env["DATABASE_URL"], "-At", "-v", f"run_id={run_id}"),  # ruff: ignore[start-process-with-partial-path] Executables come from the Nix shell.
            input=statement.encode(),
            env=self.env,
            check=True,
            capture_output=True,
        )
        return result.stdout.decode().strip()

    def start(self, name: str) -> None:
        output = (self.logs / f"{name}.log").open("ab")
        self.files.append(output)
        self.processes[name] = subprocess.Popen(  # ruff: ignore[subprocess-without-shell-equals-true]
            self.commands[name], cwd=self.directory, env=self.env, stdout=output, stderr=output
        )

    def wait(self, url: str) -> None:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            for name, process in self.processes.items():
                if process.poll() is not None:
                    message = f"{name} exited; inspect {self.logs}"
                    raise RuntimeError(message)
            try:
                if httpx.get(url).status_code < 500:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.1)
        message = f"service did not become ready: {url}"
        raise TimeoutError(message)

    def launch(self) -> None:
        self.run("initdb", "--auth=trust", "--no-locale", "-D", str(self.directory / "postgres"))
        (self.directory / "objects").mkdir()
        self.start("postgres")
        self.start("rustfs")
        self.start("restate")
        self.wait(self.env["AWS_ENDPOINT_URL_S3"] + "/health/ready")
        self.wait(self.env["LUTRA_RESTATE_ADMIN"] + "/health")
        self.run(
            "mc",
            "alias",
            "set",
            "proof",
            self.env["AWS_ENDPOINT_URL_S3"],
            "lutra",
            "lutra-secret",
            "--path",
            "on",
        )
        self.run("mc", "mb", "--ignore-existing", "proof/lutra")
        self.start("server")
        self.wait(self.env["LUTRA_URL"] + "/")

    def restart(self, *, graceful: bool = False) -> None:
        for name in ("server", "restate"):
            if graceful and name == "server":
                self.processes[name].terminate()
            else:
                self.processes[name].kill()
            self.processes[name].wait(timeout=15)
            self.processes.pop(name)
        self.start("restate")
        self.wait(self.env["LUTRA_RESTATE_ADMIN"] + "/health")
        self.start("server")
        self.wait(self.env["LUTRA_URL"] + "/")

    def close(self) -> None:
        for process in reversed(self.processes.values()):
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for output in self.files:
            output.close()
