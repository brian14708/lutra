# /// script
# requires-python = ">=3.11"
# dependencies = ["lutra[tui]", "openai-codex==0.160.0", "claude-agent-sdk==0.2.163"]
#
# [tool.uv.sources]
# lutra = { path = "..", editable = true }
# ///
"""Ask Codex for a mock weather report and Claude for a recommendation."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing_extensions import TypedDict
from urllib.parse import urlsplit, urlunsplit

import lutra
from lutra.agent import Agent, AgentSettings
from lutra.contrib.claude import Claude
from lutra.contrib.codex import Codex

environment = lutra.TaskEnvironment(name="weather-agent")
CONFIG = (
    lutra.ConfigBinding("openai_api_key", "openai/api_key", sensitive=True),
    lutra.ConfigBinding("openai_base_url", "openai/base_url", sensitive=False),
    lutra.ConfigBinding("openai_model", "openai/model", sensitive=False),
)


class WeatherResult(TypedDict):
    report: lutra.BlobRef
    recommendation: lutra.BlobRef


async def forecast() -> dict[str, object]:  # ruff: ignore[unused-async] Agent tools use async functions.
    """Return a fictional forecast for Tokyo.

    Returns:
        Mock weather data.

    """
    return {
        "city": "Tokyo",
        "mock": True,
        "temperature_c": {"low": 18, "high": 24},
        "morning": "sunny",
        "afternoon": "rain likely",
        "evening": "cloudy",
    }


def settings() -> AgentSettings:
    config = lutra.current_context().config
    values: list[str] = []
    for name in ("openai_model", "openai_base_url", "openai_api_key"):
        value = config.get(name)
        if not isinstance(value, str) or not value:
            message = "config.missing"
            raise lutra.ConfigError(message)
        values.append(value)
    url = urlsplit(values[1])
    base_url = urlunsplit(url._replace(path=url.path.rstrip("/").removesuffix("/v1")))
    return AgentSettings(model=values[0], base_url=base_url, api_key=values[2])


@environment.task(config=CONFIG)
async def weather_report() -> lutra.BlobRef:
    """Have Codex summarize a forecast in plain language.

    Returns:
        Uploaded report.

    Raises:
        ValueError: If Codex does not create the named report file.

    """
    async with Agent(adapter=Codex(settings=settings()), tools=[forecast]).session() as session:
        path = "weather-report.txt"
        await session.run(
            "Call forecast. Plan a day in Tokyo for a family with a young child: a two-hour "
            "outdoor walk, an indoor museum visit, and dinner outside if conditions permit. "
            "Compare morning, afternoon, and evening; choose the best outdoor period, explain "
            "temperature and rain tradeoffs, and propose a rainy-weather backup. Distinguish "
            "forecast facts from assumptions and mention that the data is fictional. "
            f"Write the report to {session.workspace / path}."
        )
        artifacts = await session.artifacts(path)
        if path not in artifacts:
            message = "Codex did not create weather-report.txt"
            raise ValueError(message)
        return artifacts[path]


@environment.task(config=CONFIG)
async def recommendation(report: lutra.BlobRef) -> lutra.BlobRef:
    """Have Claude recommend what to wear or bring based on the weather.

    Returns:
        Uploaded recommendation.

    Raises:
        ValueError: If Claude does not create exactly one recommendation file.

    """
    adapter = Claude(settings=settings())
    async with Agent(adapter=adapter).session() as session:
        await lutra.current_context().blobs.download_file(
            report, session.workspace / "weather-report.txt"
        )
        await session.run(
            "Read weather-report.txt. Write one simple recommendation about what to wear or bring "
            f"to {session.workspace / 'recommendation.txt'}."
        )
        artifacts = await session.artifacts("recommendation.txt")
        if "recommendation.txt" not in artifacts:
            message = "Claude did not create recommendation.txt"
            raise ValueError(message)
        return artifacts["recommendation.txt"]


@environment.workflow
async def weather() -> WeatherResult:
    """Load mock weather, write a report with Codex, and get advice from Claude.

    Returns:
        Artifact references for the weather report and recommendation.

    """
    report = await lutra.run(weather_report(), key="codex-report")
    advice = await lutra.run(recommendation(report), key="claude-recommendation")
    return {"report": report, "recommendation": advice}


async def main() -> None:
    overrides: dict[str, object] = {
        setting: os.environ[name]
        for name, setting in {
            "OPENAI_API_KEY": "openai/api_key",
            "OPENAI_BASE_URL": "openai/base_url",
            "OPENAI_MODEL": "openai/model",
        }.items()
        if name in os.environ
    }
    client = lutra.Client(os.environ.get("LUTRA_URL", "http://127.0.0.1:8080/api"))
    result = await client.run(weather(), config_overrides=overrides, display="live")
    output = Path(".data/weather")
    await asyncio.to_thread(output.mkdir, parents=True, exist_ok=True)
    await client.blobs.download_file(result["report"], output / "weather-report.txt")
    await client.blobs.download_file(result["recommendation"], output / "recommendation.txt")
    print((output / "weather-report.txt").read_text())
    print((output / "recommendation.txt").read_text())
    print(f"Files: {output}")


if __name__ == "__main__":
    asyncio.run(main())
