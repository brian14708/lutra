"""Submit a fan-out/fan-in DAG to Lutra."""

import os

import lutra

env = lutra.TaskEnvironment("dag")


@env.task
def double(value: int) -> int:
    return value * 2


@env.task
def total(values: list[int]) -> int:
    return sum(lutra.map(double, values))


if __name__ == "__main__":
    lutra.init(
        endpoint=os.environ.get("LUTRA_ENDPOINT", "http://localhost:8080/api"),
        api_key=os.environ.get("LUTRA_API_KEY", "test"),
    )
    handle = lutra.run(total, [1, 2, 3])
    print(handle.result())
