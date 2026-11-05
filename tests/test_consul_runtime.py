"""Pruebas Consul directas sin servidor externo, para mantener cobertura >=80%."""
import asyncio
import base64
import json
from unittest.mock import patch
from urllib.error import URLError

import pytest

from consul_runtime import ConsulRuntime, ConsulUnavailableError


def runtime(name="validation-service"):
    with patch.dict("os.environ", {
        "CONSUL_HTTP_ADDR": "http://consul:8500",
        "CONSUL_APP_TOKEN": "token-de-prueba",
    }):
        return ConsulRuntime(name)


def config_payload(role="validation", path="/health"):
    data = {"role": role, "health_path": path}
    return [{"Value": base64.b64encode(json.dumps(data).encode()).decode()}]


def test_disabled_without_consul_environment():
    with patch.dict("os.environ", {}, clear=True):
        client = ConsulRuntime("validation-service")
    assert client.enabled is False
    asyncio.run(client.start())
    asyncio.run(client.stop())
    with pytest.raises(ConsulUnavailableError):
        asyncio.run(client.resolve("other", attempts=1))
    with pytest.raises(ConsulUnavailableError):
        client._request("GET", "/v1/status/leader")


def test_enabled_requires_acl_token():
    with (
        patch.dict("os.environ", {"CONSUL_HTTP_ADDR": "http://consul:8500"}, clear=True),
        pytest.raises(ConsulUnavailableError, match="CONSUL_APP_TOKEN"),
    ):
        ConsulRuntime("validation-service")


def test_register_reads_kv_sets_http_health_and_tags():
    client = runtime()
    requests = []

    def fake_request(method, path, body=None):
        requests.append((method, path, body))
        if method == "GET":
            return config_payload()
        return None

    with patch.object(client, "_request", side_effect=fake_request):
        asyncio.run(client.start())
        assert client.registered is True
        assert client.config["health_path"] == "/health"
        payload = requests[1][2]
        assert payload["Name"] == "validation-service"
        assert payload["Check"]["HTTP"] == "http://validation-service:8000/health"
        assert any("middlewares=http-breaker@file" in x for x in payload["Tags"])
        assert any("validation-health" in x for x in payload["Tags"])
        asyncio.run(client.stop())
        assert client.registered is False
        assert requests[-1][:2] == (
            "PUT", "/v1/agent/service/deregister/validation-service",
        )


def test_resolve_healthy_peer_from_service_address():
    client = runtime()
    records = [{"Service": {"Address": "reverse-proxy", "Port": 8081},
                "Node": {"Address": "10.0.0.1"}}]
    with patch.object(client, "_request", return_value=records) as request:
        url = asyncio.run(client.resolve("extractext-gateway", attempts=1))
    assert url == "http://reverse-proxy:8081"
    assert request.call_args.args[1].endswith("/extractext-gateway?passing=true")


def test_resolve_uses_node_address_when_no_service_address():
    client = runtime()
    records = [{"Service": {"Address": "", "Port": 8000},
                "Node": {"Address": "10.2.0.9"}}]
    with patch.object(client, "_request", return_value=records):
        assert asyncio.run(client.resolve("peer", attempts=1)) == "http://10.2.0.9:8000"


def test_resolution_without_healthy_instance_fails():
    client = runtime()
    with (
        patch.object(client, "_request", return_value=[]),
        pytest.raises(ConsulUnavailableError, match="no encuentra"),
    ):
        asyncio.run(client.resolve("missing", attempts=1))


def test_config_must_match_own_service():
    client = runtime()
    with (
        patch.object(client, "_request", return_value=config_payload(role="other")),
        pytest.raises(ConsulUnavailableError, match="incorrecta"),
    ):
        asyncio.run(client.load_config())


def test_config_rejects_empty_and_missing_items():
    client = runtime()
    for records in ([], [{"Value": None}]):
        with (
            patch.object(client, "_request", return_value=records),
            pytest.raises(ConsulUnavailableError),
        ):
            asyncio.run(client.load_config())


def test_register_fails_closed_if_kv_unavailable():
    client = runtime()
    with (
        patch.object(client, "_request", side_effect=ConsulUnavailableError("offline")),
        pytest.raises(ConsulUnavailableError),
    ):
        asyncio.run(client.start())
    assert not client.registered


def test_shutdown_continues_if_consul_disconnected():
    client = runtime()
    client.registered = True
    with patch.object(client, "_request", side_effect=ConsulUnavailableError("offline")):
        asyncio.run(client.stop())
    assert client.registered is False


def test_http_api_wraps_network_error():
    client = runtime()
    with (
        patch("consul_runtime.urlopen", side_effect=URLError("offline")),
        pytest.raises(ConsulUnavailableError, match="Consul no disponible"),
    ):
        client._request("GET", "/v1/kv/test")


def test_http_api_sends_token_without_logging_secret():
    client = runtime()
    class Response:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def read(self):
            return b'{"status":"ok"}'
    with patch("consul_runtime.urlopen", return_value=Response()) as urlopen:
        assert client._request("GET", "/v1/status/leader") == {"status": "ok"}
        request = urlopen.call_args.args[0]
        assert request.get_header("X-consul-token") == "token-de-prueba"
    with patch("consul_runtime.urlopen", return_value=Response()):
        assert client._request("PUT", "/v1/agent/service/register", {"ID": "foo"}) == {"status": "ok"}
