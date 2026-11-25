"""Connection diagnostics must not change meter freshness or block push delivery."""
import asyncio
import ast
from datetime import timedelta
from numbers import Number
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from test_websocket import SOURCE, client
from test_push import meter_frame, prepare_plaintext, push_setup


class NodesResponse:
    def __init__(self, payload=None, error=None):
        self.payload, self.error = payload, error

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *args):
        pass

    def raise_for_status(self):
        pass

    async def json(self):
        return self.payload


def test_diagnostics_continue_during_meter_silence_without_restoring_availability(push_setup):
    async def scenario():
        c = push_setup.make()
        prepare_plaintext(c)
        await meter_frame(c)
        accepted = c.bridge._last_meter_data_update
        meter_snapshot = c.data
        push_setup.clock.now = 107
        push_setup.timers[-1].action(None)
        assert not c.last_update_success
        c.bridge.web_session.get = Mock(return_value=NodesResponse([
            {"eui": "other", "pdr": 100},
            {"eui": "meter01", "pdr": 61, "lqi": 87, "available": False},
        ]))
        diagnostics = c.nodes_coordinator
        assert diagnostics.update_interval == timedelta(seconds=120)
        assert diagnostics.config_entry is push_setup.entry
        await diagnostics.async_refresh()
        assert diagnostics.last_update_success
        assert diagnostics.data["nodes"]["pdr"] == 61
        assert diagnostics.data["nodes"]["available"] is False
        assert not c.last_update_success and c.data is meter_snapshot
        assert c.bridge._last_meter_data_update == accepted
        assert len(c.publications) == 1
        assert c.bridge.web_session.get.call_args.args[0].endswith("/nodes.json")
        assert c.bridge.web_session.get.call_args.kwargs["timeout"] == 10
    asyncio.run(scenario())


@pytest.mark.parametrize("response", [
    NodesResponse({"bad": "shape"}), NodesResponse([]),
    NodesResponse([{"eui": "other", "available": True}]),
    NodesResponse(error=asyncio.TimeoutError()),
])
def test_failed_diagnostics_do_not_fail_or_republish_meter_data(push_setup, response):
    async def scenario():
        c = push_setup.make()
        prepare_plaintext(c)
        await meter_frame(c)
        c.nodes_coordinator.async_set_updated_data({"nodes": {"pdr": 70}})
        c.bridge.web_session.get = Mock(return_value=response)
        await c.nodes_coordinator.async_refresh()
        assert not c.nodes_coordinator.last_update_success
        assert c.last_update_success and len(c.publications) == 1
        assert c.bridge._last_meter_data_update == 100
    asyncio.run(scenario())


@pytest.mark.parametrize("body", [False, True])
def test_slow_nodes_request_does_not_block_meter_and_cancellation_propagates(push_setup, body):
    async def scenario():
        entered = asyncio.Event()

        class Pending(NodesResponse):
            async def __aenter__(self):
                if not body:
                    entered.set()
                    await asyncio.Event().wait()
                return self

            async def json(self):
                entered.set()
                await asyncio.Event().wait()

        c = push_setup.make()
        prepare_plaintext(c)
        c.bridge.web_session.get = Mock(return_value=Pending())
        task = asyncio.create_task(c.nodes_coordinator.async_refresh())
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.wait_for(meter_frame(c, 42), 1)
        push_setup.clock.now = 103
        await asyncio.wait_for(meter_frame(c, 55), 1)
        assert len(c.publications) == 2 and c.last_update_success
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert c.bridge._nodes_data == {}
        assert len(c.publications) == 2
    asyncio.run(scenario())


def extract_method(filename, cls_name, method_name, namespace=None):
    tree = ast.parse((SOURCE / filename).read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls_name)
    method = next(n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                  and n.name == method_name)
    method.decorator_list = []
    namespace = namespace or {}
    exec(compile(ast.Module(body=[method], type_ignores=[]), filename, "exec"), namespace)
    return namespace[method_name]


@pytest.mark.parametrize("value,expected", [
    (True, True), (False, False), (None, None), (0, False), (1, True),
    ("true", True), ("false", False), ("connected", True),
    ("disconnected", False), ("unexpected", None),
])
def test_connection_sensor_preserves_disconnected_and_unknown(value, expected):
    is_on = extract_method("binary_sensor.py", "TibberLocalBinarySensor", "is_on")
    entity = SimpleNamespace(coordinator=SimpleNamespace(data={"nodes": {}}),
                             _tibber_coordinator=SimpleNamespace(get_sensor_value=lambda tag: value),
                             entity_description=SimpleNamespace(tag=None))
    assert is_on(entity) is expected


def test_node_values_read_independent_snapshot_even_if_meter_has_no_data():
    getter = extract_method("__init__.py", "TibberLocalDataUpdateCoordinator", "_get_node_value_internal",
                            {"NODES_KEY": "nodes"})
    c = SimpleNamespace(data=None, nodes_coordinator=SimpleNamespace(data={"nodes": {"pdr": 0}}))
    assert getter(c, "pdr") == 0
    assert getter(c, "missing") is None


def test_entity_routing_keeps_diagnostics_available_during_meter_outage():
    class CoordinatorEntity:
        def __init__(self, coordinator):
            self.coordinator = coordinator

    tree = ast.parse((SOURCE / "__init__.py").read_text())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "TibberLocalEntity")
    ns = {"CustomFriendlyNameEntity": CoordinatorEntity, "TibberLocalDataUpdateCoordinator": object,
          "EntityDescription": object, "EntityCategory": SimpleNamespace(DIAGNOSTIC="diagnostic"),
          "ObisCode": Mock(), "NODES_KEY": "nodes", "OBIS_DATA_KEY": "obis_data",
          "METRICS_KEY": "metrics", "DOMAIN": "tibber_local"}
    exec(compile(ast.Module(body=[cls], type_ignores=[]), "__init__.py", "exec"), ns)
    nodes = SimpleNamespace(data={"nodes": {"pdr": 50}}, last_update_success=True)
    meter = SimpleNamespace(data={"obis_data": {"power": 42}}, last_update_success=False,
                            nodes_coordinator=nodes, use_conf_entry_title_in_ids=False,
                            device_id="meter01", get_device_info=lambda: {"manufacturer": "Tibber"})
    make = lambda section, key: ns["TibberLocalEntity"](meter, SimpleNamespace(
        entity_category="diagnostic", key=key, tag=SimpleNamespace(section=section)))
    quality, power = make("nodes", "pdr"), make("obis_data", "0100100700ff")
    assert quality.coordinator is nodes and power.coordinator is meter
    assert quality.available and not power.available
    assert power.unique_id == "tibber_local_uid_meter01_0100100700ff"
    assert quality.unique_id == "tibber_local_uid_meter01_pdr"
    assert quality.device_info == power.device_info
    nodes.last_update_success, meter.last_update_success = False, True
    assert not quality.available and power.available


@pytest.mark.parametrize("volts,expected", [(2.0, 0), (2.4, 0), (2.8, 50), (3.2, 100), (4.0, 100), (None, None)])
def test_battery_estimate_clamps_and_preserves_missing_value(volts, expected):
    getter = extract_method("__init__.py", "TibberLocalDataUpdateCoordinator", "get_sensor_value", {
        "SensorTag": object, "NODES_KEY": "nodes", "METRICS_KEY": "metrics", "OBIS_DATA_KEY": "obis_data",
        "BATTERY_LEVEL_ESTIMATED": "battery_estimated", "BATTERY_MIN_VOLTS": 2.4,
        "BATTERY_MAX_VOLTS": 3.2, "Number": Number,
    })
    c = SimpleNamespace(_get_metric_value_internal=lambda key: volts)
    assert getter(c, SimpleNamespace(section="metrics", key="battery_estimated")) == expected


@pytest.mark.parametrize("platform,types_key,entity_class", [
    ("sensor.py", "SENSOR_TYPES", "TibberLocalSensor"),
    ("binary_sensor.py", "BINARY_SENSOR_TYPES", "TibberLocalBinarySensor"),
])
def test_node_entities_are_created_even_if_initial_diagnostics_fail(platform, types_key, entity_class):
    tree = ast.parse((SOURCE / platform).read_text())
    setup = next(n for n in tree.body if isinstance(n, ast.AsyncFunctionDef) and n.name == "async_setup_entry")
    description = SimpleNamespace(tag=SimpleNamespace(section="nodes"))
    make_entity = Mock(return_value=object())
    ns = {"HomeAssistant": object, "ConfigEntry": object, "DOMAIN": "tibber_local",
          "OBIS_DATA_KEY": "obis_data", "NODES_KEY": "nodes", "METRICS_KEY": "metrics",
          "CONF_OBIS_CODES": "obis_codes", "DEFAULT_OBIS_CODES": ["power"], "_LOGGER": Mock(),
          types_key: [description], entity_class: make_entity}
    exec(compile(ast.Module(body=[setup], type_ignores=[]), platform, "exec"), ns)
    c = SimpleNamespace(data={})
    entry = SimpleNamespace(entry_id="test", data={})
    add = Mock()
    asyncio.run(ns["async_setup_entry"](SimpleNamespace(data={"tibber_local": {"test": c}}), entry, add))
    make_entity.assert_called_once_with(c, description)
    add.assert_called_once_with([make_entity.return_value])
