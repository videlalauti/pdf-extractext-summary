"""Cliente directo de Consul HTTP API: registro, KV y descubrimiento saludable.

No existe Config Service: cada microservicio consulta Consul con su token ACL.
Solo libreria estandar. En tests fuera de Docker se desactiva si no hay URL.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


class ConsulUnavailableError(RuntimeError):
    pass


class ConsulRuntime:
    def __init__(self, name: str, port: int = 8000) -> None:
        self.name = name
        self.port = port
        self.base = os.getenv("CONSUL_HTTP_ADDR", "").rstrip("/")
        self.token = os.getenv("CONSUL_APP_TOKEN", "")
        if self.base and not self.token:
            raise ConsulUnavailableError("CONSUL_APP_TOKEN requerido cuando Consul esta habilitado")
        self.config: dict = {}
        self.registered = False

    @property
    def enabled(self) -> bool:
        return bool(self.base)

    def _request(self, method: str, path: str, body: object | None = None):
        if not self.enabled:
            raise ConsulUnavailableError("Consul no esta habilitado")
        headers = {"X-Consul-Token": self.token}
        payload = None
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = Request(self.base + path, data=payload, method=method, headers=headers)
        try:
            with urlopen(req, timeout=8) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except (HTTPError, URLError, ValueError, TimeoutError) as error:
            raise ConsulUnavailableError(f"Consul no disponible para {method} {path}: {type(error).__name__}") from error

    async def load_config(self) -> dict:
        """Lee un valor real de Consul KV con ACL por API HTTP."""
        path = f"/v1/kv/pdf-extractext/{quote(self.name, safe='')}/config"
        values = await asyncio.to_thread(self._request, "GET", path)
        if not isinstance(values, list) or not values:
            raise ConsulUnavailableError(f"Configuracion KV ausente: {self.name}")
        raw = values[0].get("Value")
        if not raw:
            raise ConsulUnavailableError(f"Configuracion KV vacia: {self.name}")
        cfg = json.loads(base64.b64decode(raw).decode("utf-8"))
        if cfg.get("role") != self.name.removesuffix("-service"):
            raise ConsulUnavailableError(f"Configuracion KV de servicio incorrecta: {self.name}")
        self.config = cfg
        return cfg

    def _tags(self) -> list[str]:
        slug = self.name.removesuffix("-service")
        hostname = f"{slug}.pdf.localhost"
        return [
            "traefik.enable=true",
            f"traefik.http.routers.{slug}.rule=Host(`{hostname}`)",
            f"traefik.http.routers.{slug}.entrypoints=web",
            f"traefik.http.routers.{slug}.middlewares=http-breaker@file",
            f"traefik.http.routers.{slug}.service={slug}",
            f"traefik.http.services.{slug}.loadbalancer.server.port={self.port}",
            f"traefik.http.routers.{slug}-health.rule=Host(`{hostname}`) && Path(`/health`)",
            f"traefik.http.routers.{slug}-health.entrypoints=web",
            f"traefik.http.routers.{slug}-health.priority=100",
            f"traefik.http.routers.{slug}-health.service={slug}",
        ]

    async def start(self) -> None:
        if not self.enabled:
            return
        cfg = await self.load_config()
        path = cfg.get("health_path", "/health")
        payload = {
            "ID": self.name,
            "Name": self.name,
            "Address": self.name,
            "Port": self.port,
            "Tags": self._tags(),
            "Check": {
                "HTTP": f"http://{self.name}:{self.port}{path}",
                "Interval": "5s",
                "Timeout": "2s",
                "DeregisterCriticalServiceAfter": "2m",
            },
        }
        await asyncio.to_thread(self._request, "PUT", "/v1/agent/service/register", payload)
        self.registered = True
        logger.info("registro en Consul Catalog: %s", self.name)

    async def resolve(self, name: str, attempts: int = 20) -> str:
        """Devuelve URL de un servicio SALUDABLE, usando Consul Discovery."""
        if not self.enabled:
            raise ConsulUnavailableError("Consul no esta habilitado")
        path = f"/v1/health/service/{quote(name, safe='')}?passing=true"
        for n in range(attempts):
            try:
                items = await asyncio.to_thread(self._request, "GET", path)
                if items:
                    item = items[0]
                    service = item["Service"]
                    address = service.get("Address") or item["Node"]["Address"]
                    port = int(service["Port"])
                    if address and 0 < port < 65536:
                        return f"http://{address}:{port}"
            except (ConsulUnavailableError, KeyError, TypeError, ValueError):
                if n == attempts - 1:
                    raise
            if n < attempts - 1:
                await asyncio.sleep(1)
        raise ConsulUnavailableError(f"Consul no encuentra instancias sanas de {name}")

    async def stop(self) -> None:
        if not self.registered:
            return
        try:
            await asyncio.to_thread(
                self._request, "PUT", f"/v1/agent/service/deregister/{quote(self.name, safe='')}"
            )
        except ConsulUnavailableError:
            logger.warning("No se pudo desregistrar %s: Consul no disponible", self.name)
        self.registered = False
