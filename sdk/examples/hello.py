"""Submit one local task to Lutra."""

import os

import lutra

env = lutra.TaskEnvironment("hello")


@env.task
def hello(name: str) -> str:
    return f"hello, {name}"


if __name__ == "__main__":
    lutra.init(
        endpoint=os.environ.get("LUTRA_ENDPOINT", "http://localhost:8080/api"),
        api_key=os.environ.get("LUTRA_API_KEY", "test"),
    )
    handle = lutra.run(hello, "world")
    print(handle.result())
