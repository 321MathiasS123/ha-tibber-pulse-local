"""Offline regressions for Tibber WebSocket recovery and frame parsing.

Only HA import glue is stubbed. The client, asyncio stream reader, aiohttp message
types, and SmlLib plaintext conversion are real. No production bridge is contacted.
"""
import asyncio
import ast
import base64
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "custom_components/tibber_local"


@pytest.fixture
def client(monkeypatch):
    package = ModuleType("review_tibber")
    package.__path__ = [str(SOURCE)]
    constants = ModuleType("review_tibber.const")
    names = {
        "ENUM_MODES", "MODE_UNKNOWN", "MODE_0_AutoScanMode",
        "MODE_1_IEC_62056_21", "MODE_2_Logarex", "MODE_3_SML_1_04",
        "MODE_10_ImpressionsAmbient", "MODE_11_ImpressionsIR", "MODE_99_PLAINTEXT",
        "ENUM_IMPLEMENTATIONS", "OBIS_DATA_KEY", "METRICS_KEY",
    }
    tree = ast.parse((SOURCE / "const.py").read_text())
    assignments = [n for n in tree.body if isinstance(n, ast.AnnAssign)
                   and isinstance(n.target, ast.Name) and n.target.id in names]
    constants.Final = object
    exec(compile(ast.Module(body=assignments, type_ignores=[]), "const.py", "exec"), constants.__dict__)
    event = ModuleType("homeassistant.helpers.event")
    event.async_call_later = Mock()
    coordinator = ModuleType("homeassistant.helpers.update_coordinator")
    coordinator.DataUpdateCoordinator = object
    for name, module in {
        "review_tibber": package,
        "review_tibber.const": constants,
        "homeassistant": ModuleType("homeassistant"),
        "homeassistant.helpers": ModuleType("homeassistant.helpers"),
        "homeassistant.helpers.event": event,
        "homeassistant.helpers.update_coordinator": coordinator,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)
    spec = importlib.util.spec_from_file_location("review_tibber.tibber_client", SOURCE / "tibber_client.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def bridge(client, session=None, mode=3):
    return client.TibberLocalBridge("bridge.test", "test-password", session or Mock(), com_mode=mode)


class WebSocketContext:
    def __init__(self, response=None, error=None):
        self.ws = SimpleNamespace(receive=AsyncMock(return_value=response, side_effect=error), close=AsyncMock())

    async def __aenter__(self):
        return self.ws

    async def __aexit__(self, *args):
        await self.ws.close()


@pytest.mark.parametrize("kind,expected", [
    (aiohttp.WSMsgType.BINARY, True),
    (aiohttp.WSMsgType.TEXT, True),
    (aiohttp.WSMsgType.CLOSED, False),
    (aiohttp.WSMsgType.ERROR, False),
])
def test_probe_only_accepts_actual_data(client, kind, expected):
    context = WebSocketContext(aiohttp.WSMessage(kind, b"payload", ""))
    session = SimpleNamespace(ws_connect=Mock(return_value=context))
    assert asyncio.run(bridge(client, session).ws_check_implementation()) is expected
    assert context.ws.close.await_count >= 1


def test_probe_timeout_selects_fallback(client):
    context = WebSocketContext(error=asyncio.TimeoutError())
    session = SimpleNamespace(ws_connect=Mock(return_value=context))
    assert asyncio.run(bridge(client, session).ws_check_implementation()) is False


def test_cancellation_during_probe_does_not_start_another_connection(client):
    async def scenario():
        entered = asyncio.Event()

        async def receive(**kwargs):
            entered.set()
            await asyncio.Event().wait()

        context = WebSocketContext()
        context.ws.receive = receive
        b = bridge(client, SimpleNamespace(ws_connect=Mock(return_value=context)))
        b.url_data = "http://bridge.test/node_data.json?node_id=1"
        b._use_classic = False
        b.ws_connect_2026_09 = AsyncMock()
        task = asyncio.create_task(b.ws_connect())
        await entered.wait()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        b.ws_connect_2026_09.assert_not_awaited()
    asyncio.run(scenario())


@pytest.mark.parametrize("classic,probe,route", [(True, False, "classic"), (False, True, "classic"), (False, False, "raw")])
def test_connection_selection(client, classic, probe, route):
    b = bridge(client)
    b.url_data = "http://bridge.test/data.json?node_id=1"
    b._use_classic = classic
    b.ws_check_implementation = AsyncMock(return_value=probe)
    b.ws_connect_classic = AsyncMock()
    b.ws_connect_2026_09 = AsyncMock()
    asyncio.run(b.ws_connect())
    assert b.ws_connect_classic.await_count == (route == "classic")
    assert b.ws_connect_2026_09.await_count == (route == "raw")
    assert b.ws_check_implementation.await_count == (not classic)


def wire_frame(payload, opcode=2, fin=True, masked=False):
    size = len(payload)
    prefix = bytes([(0x80 if fin else 0) | opcode])
    if size < 126:
        length = bytes([size | (0x80 if masked else 0)])
    elif size < 65536:
        length = bytes([126 | (0x80 if masked else 0)]) + size.to_bytes(2, "big")
    else:
        length = bytes([127 | (0x80 if masked else 0)]) + size.to_bytes(8, "big")
    key = b"test" if masked else b""
    encoded = bytes(v ^ key[i % 4] for i, v in enumerate(payload)) if masked else payload
    return prefix + length + key + encoded


def run_raw(client, monkeypatch, wire, b=None, chunks=7):
    async def scenario():
        reader = asyncio.StreamReader()
        writer = SimpleNamespace(write=Mock(), drain=AsyncMock(), close=Mock(), wait_closed=AsyncMock())
        monkeypatch.setattr(client.asyncio, "open_connection", AsyncMock(return_value=(reader, writer)))
        current = b or bridge(client)
        if b is None:
            current.handle_buffer = AsyncMock()
        task = asyncio.create_task(current.ws_connect_2026_09())
        for offset in range(0, len(wire), chunks):
            reader.feed_data(wire[offset:offset + chunks])
            await asyncio.sleep(0)
        reader.feed_eof()
        await asyncio.wait_for(task, 1)
        assert not current.ws_connected
        assert current.ws_writer is None
        assert current.ws_reader is None
        writer.close.assert_called_once()
        return current, writer
    return asyncio.run(scenario())


@pytest.mark.parametrize("size", [0, 1, 125, 126, 255, 65535, 65536])
@pytest.mark.parametrize("masked", [False, True])
def test_raw_lengths_masks_and_tcp_splits(client, monkeypatch, size, masked):
    payload = bytes(i % 256 for i in range(size))
    b, _ = run_raw(client, monkeypatch, wire_frame(payload, masked=masked), chunks=97)
    b.handle_buffer.assert_awaited_once_with(payload)


def test_fragmented_message_with_interleaved_control_frame(client, monkeypatch):
    wire = wire_frame(b"abc", fin=False) + wire_frame(b"hi", opcode=9) + wire_frame(b"def", opcode=0)
    b, writer = run_raw(client, monkeypatch, wire)
    b.handle_buffer.assert_awaited_once_with(b"abcdef")
    assert writer.write.call_count == 2  # initial request and pong


def test_incomplete_frame_is_not_published(client, monkeypatch):
    b, _ = run_raw(client, monkeypatch, wire_frame(b"complete")[:-2])
    b.handle_buffer.assert_not_awaited()


def test_close_frame_stops_before_subsequent_data(client, monkeypatch):
    b, _ = run_raw(client, monkeypatch, wire_frame(b"", opcode=8) + wire_frame(b"late"))
    b.handle_buffer.assert_not_awaited()


def test_raw_plaintext_updates_meter_values(client, monkeypatch):
    b = bridge(client, mode=99)
    b.node_device_id = "meter01"
    b.updated_tibber_metrics_if_needed = AsyncMock()
    b._ws_notify_for_new_data = Mock()
    message = b'<device:meter01 topic:"plaintext">1-0:1.8.0(1234.56*kWh)\r\n1-0:16.7.0(-42*W)\r\n!'
    run_raw(client, monkeypatch, wire_frame(message[:30], fin=False) + wire_frame(message[30:], opcode=0), b)
    values = {str(k): v.get_value() for k, v in b._obis_values.items()}
    assert values == {"0100010800ff": 1234560, "0100100700ff": -42}
    b._ws_notify_for_new_data.assert_called_once()


def test_other_meter_data_does_not_publish(client, monkeypatch):
    b = bridge(client, mode=99)
    b.node_device_id = "meter01"
    b._ws_notify_for_new_data = Mock()
    message = b'<device:meter02 topic:"plaintext">1-0:16.7.0(42*W)\r\n!'
    run_raw(client, monkeypatch, wire_frame(message), b)
    assert b._obis_values == {}
    b._ws_notify_for_new_data.assert_not_called()


@pytest.mark.parametrize("code,expected", [
    ("1-0:81.7.2255(242deg)", "0100510702ff"),
    ("1-0:81.7.26255(242deg)", "010051071aff"),
    ("1-0:81.7.2*255(242deg)", "0100510702ff"),
    ("1-0:1.8.0(1*kWh)", "0100010800ff"),
    ("1-0:81.7.999(242deg)", None),
])
def test_logarex_and_standard_obis_codes(client, code, expected):
    parts = client.TibberLocalBridge.PLAIN_TEXT_LINE.split(code)
    assert client.TibberLocalBridge.obis_hex_from_parts(parts, False) == expected


def test_authentication_header_retains_existing_ascii_credentials(client):
    b = bridge(client)
    assert b.REQ_HEADERS_BASIC_AUTH == {"Authorization": "Basic " + base64.b64encode(b"admin:test-password").decode()}


def test_release_metadata_and_source_syntax():
    manifest = json.loads((SOURCE / "manifest.json").read_text())
    assert manifest["version"] == "2026.10.2.1"
    assert manifest["domain"] == "tibber_local"
    assert manifest["requirements"] == ["smllib>=1.7"]
    for path in SOURCE.rglob("*.py"):
        compile(path.read_bytes(), str(path), "exec")
    for path in SOURCE.rglob("*.json"):
        json.loads(path.read_bytes())
