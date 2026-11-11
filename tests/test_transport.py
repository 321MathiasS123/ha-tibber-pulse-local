"""Raw stream cleanup when connection setup, reads, or shutdown fail."""
import asyncio
import ast
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import aiohttp

from test_websocket import SOURCE, bridge, client, wire_frame


def stream_writer():
    return SimpleNamespace(write=Mock(), drain=AsyncMock(), close=Mock(),
                           wait_closed=AsyncMock())


@pytest.mark.parametrize("stage", ["connect", "write", "drain", "read", "eof"])
@pytest.mark.parametrize("close_error", [False, True])
def test_raw_stream_cleans_up_after_setup_and_read_failures(client, monkeypatch, stage, close_error):
    async def scenario():
        reader = asyncio.StreamReader()
        writer = stream_writer()
        if close_error:
            writer.wait_closed.side_effect = ConnectionResetError("peer reset during close")
        if stage == "connect":
            connect = AsyncMock(side_effect=ConnectionRefusedError("connect failed"))
        else:
            connect = AsyncMock(return_value=(reader, writer))
        if stage == "write":
            writer.write.side_effect = BrokenPipeError("write failed")
        elif stage == "drain":
            writer.drain.side_effect = ConnectionResetError("drain failed")
        elif stage == "read":
            reader.set_exception(ConnectionResetError("peer reset while reading"))
        else:
            reader.feed_eof()
        monkeypatch.setattr(client.asyncio, "open_connection", connect)
        b = bridge(client)
        await b.ws_connect_2026_09()
        assert not b.ws_connected
        assert b.ws_reader is None
        assert b.ws_writer is None
        assert writer.close.call_count == (stage != "connect")
        assert writer.wait_closed.await_count == (stage != "connect")
    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["connect", "drain", "read", "metrics_headers", "metrics_body"])
def test_raw_cancellation_closes_socket_and_propagates(client, monkeypatch, stage):
    async def scenario():
        entered = asyncio.Event()

        async def pending(*args, **kwargs):
            entered.set()
            await asyncio.Event().wait()

        reader = asyncio.StreamReader()
        writer = stream_writer()
        connect = pending if stage == "connect" else AsyncMock(return_value=(reader, writer))
        if stage == "drain":
            writer.drain = pending
        elif stage == "read":
            reader = SimpleNamespace(readexactly=pending)
            connect = AsyncMock(return_value=(reader, writer))

        class MetricsResponse:
            async def __aenter__(self):
                if stage == "metrics_headers":
                    await pending()
                return self

            async def __aexit__(self, *args):
                pass

            def raise_for_status(self):
                pass

            json = pending

        session = SimpleNamespace(get=Mock(return_value=MetricsResponse()))
        b = bridge(client, session, mode=99)
        b.node_device_id = "meter01"
        b.url_metrics = b.url_metrics_2026_09
        if stage.startswith("metrics_"):
            reader.feed_data(wire_frame(
                b'<device:meter01 topic:"plaintext">1-0:16.7.0(42*W)\r\n!'
            ))
        monkeypatch.setattr(client.asyncio, "open_connection", connect)
        task = asyncio.create_task(b.ws_connect_2026_09())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not b.ws_connected
        assert b.ws_reader is None
        assert b.ws_writer is None
        assert writer.close.call_count == (stage != "connect")
        assert writer.wait_closed.await_count == (stage != "connect")
    asyncio.run(scenario())


def test_old_connection_cleanup_preserves_replacement_connection(client, monkeypatch):
    async def scenario():
        reader = asyncio.StreamReader()
        reader.feed_eof()
        writer = stream_writer()
        closing = asyncio.Event()
        release = asyncio.Event()

        async def wait_closed():
            closing.set()
            await release.wait()

        writer.wait_closed = wait_closed
        monkeypatch.setattr(client.asyncio, "open_connection", AsyncMock(return_value=(reader, writer)))
        b = bridge(client)
        task = asyncio.create_task(b.ws_connect_2026_09())
        await closing.wait()
        replacement_reader, replacement_writer = object(), stream_writer()
        b.ws_reader, b.ws_writer = replacement_reader, replacement_writer
        b.ws_connected = True
        release.set()
        await task
        assert b.ws_connected
        assert b.ws_reader is replacement_reader
        assert b.ws_writer is replacement_writer
        replacement_writer.close.assert_not_called()
    asyncio.run(scenario())


@pytest.fixture
def watchdog():
    tree = ast.parse((SOURCE / "__init__.py").read_text())
    coordinator = next(node for node in tree.body if isinstance(node, ast.ClassDef)
                       and node.name == "TibberLocalDataUpdateCoordinator")
    method_names = {"_check_for_ws_task_and_cancel_if_running", "_async_watchdog_check"}
    methods = [node for node in coordinator.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
               and node.name in method_names]
    interval = next(node for node in tree.body if isinstance(node, ast.AnnAssign)
                    and isinstance(node.target, ast.Name) and node.target.id == "WEBSOCKET_WATCHDOG_INTERVAL")
    namespace = {"_LOGGER": Mock(), "async_call_later": Mock(), "Final": object, "timedelta": timedelta}
    exec(compile(ast.Module(body=[interval, *methods], type_ignores=[]), "__init__.py", "exec"), namespace)
    assert namespace["WEBSOCKET_WATCHDOG_INTERVAL"] == timedelta(seconds=10)
    current = SimpleNamespace(
        _a_task=None, hass=object(), stop_watchdog=Mock(), call_later_update_device_registry=Mock(),
        bridge=SimpleNamespace(ws_supported=True, ws_connected=False,
                               ws_check_last_update=Mock(return_value=True), ws_connect=AsyncMock()),
    )
    current._check_for_ws_task_and_cancel_if_running = lambda: namespace["_check_for_ws_task_and_cancel_if_running"](current)
    current._async_watchdog_check = lambda: namespace["_async_watchdog_check"](current)
    current._config_entry = SimpleNamespace(async_create_background_task=Mock(
        side_effect=lambda hass, coroutine, name: asyncio.create_task(coroutine)))
    return current


def test_watchdog_leaves_pending_probe_running_across_ticks(watchdog):
    async def scenario():
        entered = asyncio.Event()

        async def connect():
            entered.set()
            await asyncio.Event().wait()

        watchdog.bridge.ws_connect.side_effect = connect
        await watchdog._async_watchdog_check()
        original = watchdog._a_task
        await entered.wait()
        await watchdog._async_watchdog_check()
        await watchdog._async_watchdog_check()
        assert watchdog._a_task is original
        assert not original.done()
        assert original.cancelling() == 0
        watchdog._config_entry.async_create_background_task.assert_called_once()
        original.cancel()
        with pytest.raises(asyncio.CancelledError):
            await original
    asyncio.run(scenario())


@pytest.mark.parametrize("has_finished_task", [False, True])
def test_watchdog_reconnects_disconnected_stream(watchdog, has_finished_task):
    async def scenario():
        if has_finished_task:
            watchdog._a_task = asyncio.create_task(asyncio.sleep(0))
            await watchdog._a_task
        previous = watchdog._a_task
        await watchdog._async_watchdog_check()
        assert watchdog._a_task is not previous
        watchdog._config_entry.async_create_background_task.assert_called_once()
        await watchdog._a_task
        watchdog.bridge.ws_connect.assert_awaited_once()
    asyncio.run(scenario())


def test_watchdog_waits_for_stale_stream_cleanup_before_reconnecting(watchdog):
    async def scenario():
        entered, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def old_connection():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                watchdog.bridge.ws_connected = False
                closing.set()
                await release.wait()

        old_task = asyncio.create_task(old_connection())
        watchdog._a_task = old_task
        watchdog.bridge.ws_connected = True
        watchdog.bridge.ws_check_last_update.return_value = False
        await entered.wait()
        await watchdog._async_watchdog_check()
        assert watchdog._a_task is old_task
        await closing.wait()
        await watchdog._async_watchdog_check()
        assert watchdog._a_task is old_task
        watchdog._config_entry.async_create_background_task.assert_not_called()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await old_task
        await watchdog._async_watchdog_check()
        watchdog._config_entry.async_create_background_task.assert_called_once()
        await watchdog._a_task
    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["handshake", "receive"])
def test_stalled_websocket_probe_times_out_and_selects_raw_fallback(client, monkeypatch, stage):
    async def scenario():
        real_timeout = asyncio.timeout
        deadlines = []

        def short_timeout(seconds):
            deadlines.append(seconds)
            return real_timeout(0.01)

        class Probe:
            async def __aenter__(self):
                if stage == "handshake":
                    await asyncio.Event().wait()
                return SimpleNamespace(receive=AsyncMock(side_effect=self.pending), close=AsyncMock())

            async def __aexit__(self, *args):
                pass

            async def pending(self, **kwargs):
                await asyncio.Event().wait()

        monkeypatch.setattr(client.asyncio, "timeout", short_timeout)
        b = bridge(client, SimpleNamespace(ws_connect=Mock(return_value=Probe())))
        b.url_data = b.url_data_2026_09
        b._use_classic = False
        b.ws_connect_2026_09 = AsyncMock()
        await b.ws_connect()
        assert deadlines == [12]
        b.ws_connect_2026_09.assert_awaited_once()
    asyncio.run(scenario())


def test_raw_connection_establishment_has_bounded_timeout(client, monkeypatch):
    async def scenario():
        real_wait_for = asyncio.wait_for
        deadlines = []
        cancelled = asyncio.Event()

        async def short_wait_for(awaitable, timeout):
            deadlines.append(timeout)
            return await real_wait_for(awaitable, 0.01)

        async def connect(*args):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        monkeypatch.setattr(client.asyncio, "wait_for", short_wait_for)
        monkeypatch.setattr(client.asyncio, "open_connection", connect)
        b = bridge(client)
        await b.ws_connect_2026_09()
        assert deadlines == [10]
        assert cancelled.is_set()
        assert not b.ws_connected
        assert b.ws_reader is None
        assert b.ws_writer is None
    asyncio.run(scenario())


def test_cancellation_during_probe_close_does_not_start_another_connection(client):
    async def scenario():
        closing = asyncio.Event()

        async def close():
            closing.set()
            await asyncio.Event().wait()

        ws = SimpleNamespace(receive=AsyncMock(return_value=aiohttp.WSMessage(
            aiohttp.WSMsgType.CLOSED, None, "")), close=close)
        context = AsyncMock()
        context.__aenter__.return_value = ws
        b = bridge(client, SimpleNamespace(ws_connect=Mock(return_value=context)))
        b.url_data = b.url_data_2026_09
        b._use_classic = False
        b.ws_connect_2026_09 = AsyncMock()
        task = asyncio.create_task(b.ws_connect())
        await asyncio.wait_for(closing.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        b.ws_connect_2026_09.assert_not_awaited()
    asyncio.run(scenario())
