"""Push ownership, real meter freshness, and nonblocking diagnostics."""
import asyncio
import ast
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from test_websocket import SOURCE, bridge, client


@pytest.fixture
def push_setup(client, monkeypatch):
    clock = SimpleNamespace(now=100.0)
    monkeypatch.setattr(client, "time", SimpleNamespace(time=lambda: clock.now, monotonic=lambda: clock.now))
    timers, intervals, unload, events = [], [], [], []

    def callback(function):
        function._hass_callback = True
        return function

    def later(hass, delay, action):
        timer = SimpleNamespace(delay=delay, action=action, cancelled=False)
        timer.cancel = Mock(side_effect=lambda: setattr(timer, "cancelled", True))
        timers.append(timer)
        return timer.cancel

    def listen_once(event, action):
        row = SimpleNamespace(event=event, action=action, active=True)
        row.cancel = Mock(side_effect=lambda: setattr(row, "active", False))
        events.append(row)
        return row.cancel

    class BaseCoordinator:
        def __init__(self, hass, logger, name, update_interval=None):
            self.hass, self.update_interval = hass, update_interval
            self.data, self.last_update_success = None, True
            self.publications, self.errors, self.refreshes = [], [], 0

        def async_set_updated_data(self, data):
            self.data, self.last_update_success = data, True
            self.publications.append(data)

        def async_set_update_error(self, error):
            self.last_update_success = False
            self.errors.append(error)

        async def async_refresh(self):
            self.refreshes += 1
            self.async_set_updated_data(await self._async_update_data())

        async def async_request_refresh(self):
            self.refreshes += 1
            self.async_set_updated_data(await self._async_update_data())

    selected = {
        "__init__", "init_on_load", "start_watchdog", "stop_watchdog", "_cancel_meter_timeout",
        "_schedule_meter_timeout", "_meter_data_expired", "async_request_refresh", "async_refresh",
        "_check_for_ws_task_and_cancel_if_running", "_async_watchdog_check", "_async_update_data",
    }
    tree = ast.parse((SOURCE / "__init__.py").read_text())
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef)
               and node.name == "TibberLocalDataUpdateCoordinator")
    cls.body = [node for node in cls.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name in selected]
    session = SimpleNamespace(get=Mock())
    namespace = {
        "DataUpdateCoordinator": BaseCoordinator, "HomeAssistant": object,
        "time": client.time, "timedelta": timedelta, "callback": callback, "_LOGGER": Mock(),
        "DOMAIN": "tibber_local", "CONF_HOST": "host", "CONF_PASSWORD": "password",
        "CONF_NODE_NUMBER": "node_num", "CONF_DEVICE_ID": "device_id", "CONF_MODE": "mode",
        "CONF_IGNORE_READING_ERRORS": "ignore_reading_errors", "CONF_USE_POLLING": "use_polling",
        "CONF_SCAN_INTERVAL": "scan_interval", "DEFAULT_USE_POLLING": False,
        "DEFAULT_SCAN_INTERVAL": 3, "MODE_3_SML_1_04": 3,
        "EVENT_HOMEASSISTANT_STOP": "stop", "EVENT_HOMEASSISTANT_STARTED": "started",
        "WEBSOCKET_WATCHDOG_INTERVAL": timedelta(seconds=10),
        "OBIS_DATA_KEY": client.OBIS_DATA_KEY, "METRICS_KEY": client.METRICS_KEY,
        "UpdateFailed": type("UpdateFailed", (Exception,), {}),
        "TibberLocalBridge": client.TibberLocalBridge, "tibber_client": client,
        "async_get_clientsession": lambda hass: session, "async_call_later": later,
        "async_track_time_interval": lambda hass, action, interval: intervals.append(interval) or Mock(),
    }
    exec(compile(ast.Module(body=[cls], type_ignores=[]), "__init__.py", "exec"), namespace)
    entry = SimpleNamespace(
        data={"host": "bridge.test", "password": "test-password", "scan_interval": 3, "device_id": "meter01"},
        title="Tibber", version=2, minor_version=2, entry_id="entry",
        async_on_unload=unload.append,
        async_create_background_task=Mock(side_effect=lambda hass, coroutine, name: asyncio.create_task(coroutine)),
    )
    hass = SimpleNamespace(bus=SimpleNamespace(async_listen_once=listen_once), data={},
                           config_entries=SimpleNamespace(async_update_entry=Mock()))

    def make(polling=False):
        entry.data["use_polling"] = polling
        coordinator = namespace["TibberLocalDataUpdateCoordinator"](hass, entry)
        coordinator.call_later_update_device_registry = Mock()
        coordinator.bridge.node_device_id = "meter01"
        return coordinator

    return SimpleNamespace(make=make, clock=clock, timers=timers, intervals=intervals,
                           unload=unload, events=events, namespace=namespace, entry=entry, hass=hass)


async def meter_frame(coordinator, watts=42):
    b = coordinator.bridge
    await b.handle_buffer(f'<device:meter01 topic:"plaintext">1-0:16.7.0({watts}*W)\r\n!'.encode())
    if b._ws_debounced_update_task is not None:
        await b._ws_debounced_update_task


def prepare_plaintext(coordinator):
    coordinator.bridge._com_mode = 99
    coordinator.bridge._LAST_METRICS_UPDATE = 100


@pytest.mark.parametrize("pending_setup", [False, True])
def test_supported_push_manual_refresh_never_fetches_or_republishes_cache(push_setup, pending_setup):
    async def scenario():
        c = push_setup.make()
        prepare_plaintext(c)
        await meter_frame(c)
        c.bridge.update = AsyncMock()
        if pending_setup:
            c._a_task = asyncio.create_task(asyncio.Event().wait())
        assert c.update_interval is None
        assert len(c.publications) == 1
        push_setup.clock.now = 103
        await c.async_request_refresh()
        await c.async_refresh()
        assert c.refreshes == 0
        assert len(c.publications) == 1
        c.bridge.update.assert_not_awaited()
        if pending_setup:
            c._a_task.cancel()
            await asyncio.gather(c._a_task, return_exceptions=True)
    asyncio.run(scenario())


def test_stale_timer_uses_accepted_meter_age_and_retains_pending_probe(push_setup):
    async def scenario():
        c = push_setup.make()
        prepare_plaintext(c)
        await meter_frame(c)
        first = push_setup.timers[-1]
        assert first.delay == 6
        assert first.action._hass_callback
        push_setup.clock.now = 103
        await c.bridge.handle_buffer(b'<device:meter01 topic:"plaintext">invalid')
        await c.bridge.handle_buffer(b'<device:other topic:"plaintext">1-0:16.7.0(52*W)\r\n!')
        assert push_setup.timers[-1] is first
        await meter_frame(c, 52)
        assert first.cancelled
        current = push_setup.timers[-1]
        first.action(None)  # An already queued old deadline must recheck actual age.
        assert c.errors == []
        assert current.cancelled
        c._a_task = asyncio.create_task(asyncio.Event().wait())
        push_setup.clock.now = 109
        current.action(None)
        assert not c.last_update_success
        assert len(c.errors) == 1
        assert len(c.publications) == 2
        assert not c._a_task.done() and c._a_task.cancelling() == 0
        c._a_task.cancel()
        await asyncio.gather(c._a_task, return_exceptions=True)
    asyncio.run(scenario())


def test_genuine_identical_reading_recovers_after_stale_deadline(push_setup):
    async def scenario():
        c = push_setup.make()
        prepare_plaintext(c)
        await meter_frame(c, 42)
        push_setup.clock.now = 106
        push_setup.timers[-1].action(None)
        assert not c.last_update_success
        assert len(c.publications) == 1
        push_setup.clock.now = 107
        await meter_frame(c, 42)
        assert c.last_update_success
        assert c.bridge._last_meter_data_update == 107
        assert len(c.publications) == 2
        assert push_setup.timers[-1].delay == 6
        await c.async_request_refresh()
        assert len(c.publications) == 2
    asyncio.run(scenario())


def test_delayed_push_task_cannot_publish_a_now_stale_reading(push_setup, client, monkeypatch):
    async def scenario():
        c = push_setup.make()
        prepare_plaintext(c)
        c.bridge._ws_LAST_NEW_DATA_NOTIFY = 100
        real_sleep = asyncio.sleep

        async def delayed_sleep(seconds):
            push_setup.clock.now = 107
            await real_sleep(0)

        monkeypatch.setattr(client.asyncio, "sleep", delayed_sleep)
        await meter_frame(c)
        assert c.publications == []
        assert not c.last_update_success
        assert len(c.errors) == 1
    asyncio.run(scenario())


@pytest.mark.parametrize("shutdown", [False, True])
def test_stale_timer_is_cancelled_on_setup_failure_unload_and_shutdown(push_setup, shutdown):
    async def scenario():
        c = push_setup.make()
        prepare_plaintext(c)
        await meter_frame(c)
        timer = push_setup.timers[-1]
        if shutdown:
            stop = next(row for row in push_setup.events if row.event == "stop")
            assert stop.action._hass_callback
            stop.action(object())
        else:
            for callback in push_setup.unload:
                callback()
        assert c._stopped and timer.cancelled
        push_setup.clock.now = 120
        timer.action(None)
        assert c.errors == []
        before = len(c.publications)
        await meter_frame(c, 55)
        assert len(c.publications) == before
        assert c.bridge._ws_metrics_task is None
    asyncio.run(scenario())


@pytest.mark.parametrize("polling", [False, True])
def test_http_polling_works_for_explicit_mode_and_unsupported_websocket(push_setup, polling):
    async def scenario():
        c = push_setup.make(polling=polling)
        c._use_classic = False
        c.bridge.update = AsyncMock()
        if not polling:
            c.bridge.ws_supported = False
            await c.start_watchdog()
            assert c._watchdog is None
            assert not push_setup.intervals
        else:
            await c.async_request_refresh()
        assert c.update_interval == timedelta(seconds=3)
        c.bridge.update.assert_awaited_once()
        assert c.refreshes == 1
        await c.async_refresh()
        assert c.bridge.update.await_count == 2
    asyncio.run(scenario())


def test_404_probe_does_not_start_raw_stream(client):
    async def scenario():
        context = AsyncMock()
        context.__aenter__.side_effect = aiohttp.WSServerHandshakeError(Mock(), (), status=404)
        b = bridge(client, SimpleNamespace(ws_connect=Mock(return_value=context)))
        b._use_classic, b.url_data = False, b.url_data_2026_09
        b.ws_connect_2026_09 = AsyncMock()
        await b.ws_connect()
        assert not b.ws_supported
        b.ws_connect_2026_09.assert_not_awaited()
    asyncio.run(scenario())


@pytest.mark.parametrize("body", [False, True])
def test_metrics_are_single_owned_nonblocking_and_cancelled_on_unload(push_setup, body):
    async def scenario():
        entered = asyncio.Event()

        class Response:
            async def __aenter__(self):
                if not body:
                    entered.set()
                    await asyncio.Event().wait()
                return self

            async def __aexit__(self, *args):
                pass

            def raise_for_status(self):
                pass

            async def json(self):
                entered.set()
                await asyncio.Event().wait()

        c = push_setup.make()
        prepare_plaintext(c)
        b = c.bridge
        b._LAST_METRICS_UPDATE = -1800
        b.url_metrics = b.url_metrics_2026_09
        b.web_session.get = Mock(return_value=Response())
        await meter_frame(c)
        await asyncio.wait_for(entered.wait(), 1)
        metrics = b._ws_metrics_task
        assert len(c.publications) == 1 and not metrics.done()
        push_setup.entry.async_create_background_task.assert_called_once()
        assert push_setup.entry.async_create_background_task.call_args.args[-1] == "tibber_metrics"
        assert b.web_session.get.call_args.kwargs["timeout"] == 10.0
        push_setup.clock.now = 103
        await meter_frame(c, 55)
        assert len(c.publications) == 2
        assert b._ws_metrics_task is metrics
        b.web_session.get.assert_called_once()
        c.stop_watchdog()
        await b.ws_close_and_prepare_to_terminate()
        assert metrics.cancelled() and b._ws_metrics_task is None
        assert not b._metrics_update_is_running
        assert len(c.publications) == 2
    asyncio.run(scenario())


@pytest.mark.parametrize("classic", [False, True])
def test_new_connection_has_transport_grace_without_making_meter_fresh(push_setup, client, monkeypatch, classic):
    async def scenario():
        entered = asyncio.Event()

        async def wait_for_frame(*args):
            entered.set()
            await asyncio.Event().wait()

        c = push_setup.make()
        c.bridge._last_meter_data_update = 50
        c.bridge._ws_LAST_UPDATE = 0

        class Socket:
            close = AsyncMock()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def __aiter__(self):
                return self

            __anext__ = wait_for_frame

        if classic:
            c.bridge.web_session.ws_connect = Mock(return_value=Socket())
            connect = c.bridge.ws_connect_classic
        else:
            reader = SimpleNamespace(readexactly=wait_for_frame)
            writer = SimpleNamespace(write=Mock(), drain=AsyncMock(), close=Mock(), wait_closed=AsyncMock())
            monkeypatch.setattr(client.asyncio, "open_connection", AsyncMock(return_value=(reader, writer)))
            connect = c.bridge.ws_connect_2026_09
        task = asyncio.create_task(connect())
        await asyncio.wait_for(entered.wait(), 1)
        assert c.bridge._ws_LAST_UPDATE == 100
        assert c.bridge._last_meter_data_update == 50
        push_setup.clock.now = 109
        assert c.bridge.ws_check_last_update()
        c._schedule_meter_timeout()
        push_setup.timers[-1].action(None)
        assert not c.last_update_success
        assert not task.done() and task.cancelling() == 0
        task.cancel()
        if classic:
            await task
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
    asyncio.run(scenario())


def test_unload_removes_deferred_startup_listener(push_setup):
    async def scenario():
        namespace = dict(push_setup.namespace)
        c = push_setup.make()
        c.init_on_load = AsyncMock(return_value=True)
        c.start_watchdog = AsyncMock()
        entry, hass = push_setup.entry, push_setup.hass
        entry.as_dict = lambda: {}
        entry.add_update_listener = Mock(return_value=Mock())
        hass.state = "starting"
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        namespace.update(ConfigEntry=object, CoreState=SimpleNamespace(running="running"),
                         mask_map=lambda value: value, PLATFORMS=["sensor"], entry_update_listener=Mock(),
                         TibberLocalDataUpdateCoordinator=lambda *args: c)
        tree = ast.parse((SOURCE / "__init__.py").read_text())
        setup = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                     and node.name == "async_setup_entry")
        exec(compile(ast.Module(body=[setup], type_ignores=[]), "__init__.py", "exec"), namespace)
        assert await namespace["async_setup_entry"](hass, entry)
        started = next(row for row in push_setup.events if row.event == "started")
        assert started.active
        for callback in push_setup.unload:
            callback()
        assert not started.active
        c.start_watchdog.assert_not_awaited()
    asyncio.run(scenario())


def test_metrics_completion_waits_for_next_real_meter_frame_to_publish(push_setup):
    async def scenario():
        release = asyncio.Event()
        c = push_setup.make()
        prepare_plaintext(c)
        b = c.bridge

        async def update_metrics():
            await release.wait()
            b._metrics_data = {"voltage": 3}

        b._LAST_METRICS_UPDATE = -1800
        b.updated_tibber_metrics_if_needed = update_metrics
        await meter_frame(c)
        release.set()
        await b._ws_metrics_task
        assert len(c.publications) == 1
        assert c.data[push_setup.namespace["METRICS_KEY"]] == {}
        push_setup.clock.now = 103
        await meter_frame(c, 55)
        assert len(c.publications) == 2
        assert c.data[push_setup.namespace["METRICS_KEY"]] == {"voltage": 3}
        await b._ws_cancel_metrics_update()
    asyncio.run(scenario())
