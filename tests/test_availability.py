"""Offline regressions for HTTP requests racing with valid meter readings."""
import asyncio
import ast
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from test_websocket import SOURCE, bridge, client


POWER = "0100100700ff"
PLAINTEXT = "1-0:16.7.0(42*W)\r\n!"
# Synthetic one-reading SML transport frame, including its valid CRC.
SML = bytes.fromhex(
    "1b1b1b1b0101010177070100100700ff0101621b520053002a011b1b1b1b1a009016"
)
IMPRESSIONS = {"$type": "imp_data", "kw": 0.042, "kwh": 1}


class UpdateFailed(Exception):
    pass


@pytest.fixture
def clock(client, monkeypatch):
    current = SimpleNamespace(now=90.0)
    monkeypatch.setattr(client, "time", SimpleNamespace(
        time=lambda: current.now, monotonic=lambda: current.now,
    ))
    return current


@pytest.fixture
def update_method(client, clock):
    # Execute the actual coordinator method, stubbing only its HA surroundings.
    tree = ast.parse((SOURCE / "__init__.py").read_text())
    coordinator = next(n for n in tree.body if isinstance(n, ast.ClassDef)
                       and n.name == "TibberLocalDataUpdateCoordinator")
    method = next(n for n in coordinator.body if isinstance(n, ast.AsyncFunctionDef)
                  and n.name == "_async_update_data")
    namespace = {
        "time": client.time, "_LOGGER": Mock(), "UpdateFailed": UpdateFailed,
        "ClientConnectionError": aiohttp.ClientConnectionError,
        "OBIS_DATA_KEY": client.OBIS_DATA_KEY, "METRICS_KEY": client.METRICS_KEY,
    }
    exec(compile(ast.Module(body=[method], type_ignores=[]), "__init__.py", "exec"), namespace)
    return namespace["_async_update_data"]


class PendingHTTP:
    def __init__(self, error=None, payload=PLAINTEXT, stage="headers"):
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.error = error
        self.payload = payload
        self.stage = stage
        self.status = 200

    async def pause(self):
        self.entered.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error

    async def __aenter__(self):
        if self.stage == "headers":
            await self.pause()
        return self

    async def __aexit__(self, *args):
        pass

    def raise_for_status(self):
        pass

    async def body(self, **kwargs):
        if self.stage == "body":
            await self.pause()
        return self.payload

    text = read = json = body


def make_bridge(client, response, mode=99):
    b = bridge(client, SimpleNamespace(get=Mock(return_value=response)), mode=mode)
    b.url_data = b.url_data_2026_09
    b.node_device_id = "meter01"
    b._ws_LAST_UPDATE = 90  # A previous socket reading avoids the startup grace.
    b.updated_tibber_metrics_if_needed = AsyncMock()
    b._ws_notify_for_new_data = Mock()
    return b


def coordinator(b, classic=False, polling=False):
    return SimpleNamespace(
        bridge=b, _use_classic=classic, _use_websocket_in_config=not polling,
        _integration_update_interval_in_seconds=3,
    )


async def publish(b, value=52, meter="meter01"):
    await b.handle_buffer(
        f'<device:{meter} topic:"plaintext">1-0:16.7.0({value}*W)\r\n!'.encode()
    )


def value(b):
    return next(entry.get_value() for key, entry in b._obis_values.items() if str(key) == POWER)


@pytest.mark.parametrize("stage", ["headers", "body"])
@pytest.mark.parametrize("error", [asyncio.TimeoutError, aiohttp.ClientConnectionError])
def test_failed_old_http_preserves_fresh_websocket_reading(client, clock, update_method, stage, error):
    async def scenario():
        response = PendingHTTP(error=error(), stage=stage)
        b = make_bridge(client, response)
        await publish(b, 42)
        clock.now = 100
        task = asyncio.create_task(update_method(coordinator(b)))
        await asyncio.wait_for(response.entered.wait(), 1)
        clock.now = 100.95
        await publish(b)
        clock.now = 101
        response.release.set()
        result = await task
        assert result[client.OBIS_DATA_KEY] is b._obis_values
        assert value(b) == 52
    asyncio.run(scenario())


@pytest.mark.parametrize("traffic", ["none", "invalid", "wrong_meter", "new_but_stale", "prior_but_fresh"])
def test_failed_http_still_rejects_stale_or_unvalidated_data(client, clock, update_method, traffic):
    async def scenario():
        response = PendingHTTP(error=asyncio.TimeoutError())
        b = make_bridge(client, response)
        if traffic == "prior_but_fresh":
            clock.now = 99.5
        await publish(b, 42)
        previous_stamp = b._last_meter_data_update
        clock.now = 100
        task = asyncio.create_task(update_method(coordinator(b, classic=traffic == "prior_but_fresh")))
        await asyncio.wait_for(response.entered.wait(), 1)
        clock.now = 101
        if traffic == "invalid":
            await b.handle_buffer(b'<device:meter01 topic:"plaintext">invalid')
        elif traffic == "wrong_meter":
            await publish(b, meter="meter02")
        elif traffic == "new_but_stale":
            await publish(b)
        if traffic != "new_but_stale":
            assert b._last_meter_data_update == previous_stamp
        clock.now = 105 if traffic == "new_but_stale" else 101
        response.release.set()
        with pytest.raises(UpdateFailed) as raised:
            await task
        assert isinstance(raised.value.__cause__, asyncio.TimeoutError)
    asyncio.run(scenario())


@pytest.mark.parametrize("traffic", ["fresh", "polling", "invalid_notification"])
def test_fallback_uses_valid_meter_freshness_and_respects_polling(client, clock, update_method, traffic):
    async def scenario():
        b = bridge(client, mode=99)
        clock.now = 90 if traffic == "invalid_notification" else 99.5
        await b.mode_99_read_plaintext(PLAINTEXT, b.MAX_READ_RETRIES, False)
        b._ws_LAST_NEW_DATA_NOTIFY = 100 if traffic == "invalid_notification" else 0
        b.update = AsyncMock()
        clock.now = 101
        result = await update_method(coordinator(b, polling=traffic == "polling"))
        assert result[client.OBIS_DATA_KEY] is b._obis_values
        if traffic == "fresh":
            b.update.assert_not_awaited()
        else:
            b.update.assert_awaited_once()
    asyncio.run(scenario())


@pytest.mark.parametrize("classic,polling", [(True, False), (True, True), (False, True)])
def test_classic_and_polling_transport_failures_remain_failures(client, clock, update_method, classic, polling):
    async def scenario():
        response = PendingHTTP(error=aiohttp.ClientConnectionError())
        response.release.set()
        b = make_bridge(client, response)
        await publish(b, 42)
        clock.now = 100
        with pytest.raises(UpdateFailed):
            await update_method(coordinator(b, classic, polling))
    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["headers", "body"])
def test_cancellation_is_not_hidden_by_fresh_data(client, clock, update_method, stage):
    async def scenario():
        response = PendingHTTP(stage=stage)
        b = make_bridge(client, response)
        clock.now = 100
        task = asyncio.create_task(update_method(coordinator(b)))
        await asyncio.wait_for(response.entered.wait(), 1)
        clock.now = 101
        await publish(b)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["headers", "body"])
@pytest.mark.parametrize("stale", [False, True])
def test_old_success_cannot_replace_newer_reading_or_hide_staleness(client, clock, update_method, stage, stale):
    async def scenario():
        response = PendingHTTP(payload=PLAINTEXT, stage=stage)
        b = make_bridge(client, response)
        clock.now = 100
        task = asyncio.create_task(update_method(coordinator(b)))
        await asyncio.wait_for(response.entered.wait(), 1)
        clock.now = 101
        await publish(b)
        clock.now = 105 if stale else 101.05
        response.release.set()
        if stale:
            with pytest.raises(UpdateFailed):
                await task
        else:
            result = await task
            assert result[client.OBIS_DATA_KEY] is b._obis_values
        assert value(b) == 52
        assert b._last_meter_data_update == 101
    asyncio.run(scenario())


@pytest.mark.parametrize("mode,payload,invalid", [
    (3, SML, b"incomplete SML"),
    (99, PLAINTEXT, "invalid"),
    (10, IMPRESSIONS, {"$type": "unrelated"}),
])
def test_only_valid_nonempty_parser_results_refresh_meter_timestamp(client, clock, mode, payload, invalid):
    async def scenario():
        b = bridge(client, mode=mode)
        parse = {3: b.mode_03_read_sml, 99: b.mode_99_read_plaintext,
                 10: b.mode_10_read_json_impressions_ambient}[mode]
        assert b._last_meter_data_update == 0
        clock.now = 100
        await parse(payload, b.MAX_READ_RETRIES, False)
        assert value(b) == 42
        assert b._last_meter_data_update == 100
        previous_values = b._obis_values
        clock.now = 110
        await parse(invalid, b.MAX_READ_RETRIES, False)
        assert b._obis_values is previous_values
        assert b._last_meter_data_update == 100
    asyncio.run(scenario())
