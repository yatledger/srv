"""HTTP-клиент к кластеру `dagdb` с ретраями и ротацией узлов (R5).

Публичные ручки: `POST /`, `GET /pool`, `GET /full`, `GET /health`. Поиск лидера
(опционально) — через внутреннюю `POST /mng/metrics` по токену. Никаких «тихих»
потерь: каждая ошибка классифицируется (`accepted` / `rejected` / `error`).
"""

import asyncio
from dataclasses import dataclass
from typing import Optional

import httpx

#: Статусы, при которых запрос имеет смысл повторить.
RETRYABLE_STATUS = {429, 503}


@dataclass
class ClientOutcome:
    """Результат одного запроса транзакции."""

    status: int
    #: `"accepted" | "rejected" | "error"`.
    kind: str
    reason: Optional[str]
    latency_ms: float
    transport_error: bool = False
    attempts: int = 1


class DagdbClient:
    """Асинхронный клиент кластера с пулом хостов и ротацией недоступного узла."""

    def __init__(self, hosts, timeout, retry, internal_token=None, verbose=False):
        if not hosts:
            raise ValueError("нужен хотя бы один хост")
        self.hosts = list(hosts)
        self.retry = retry
        self.internal_token = internal_token
        self.verbose = verbose
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(max_connections=None, max_keepalive_connections=None),
        )
        #: Индекс предпочтительного (лидера/первого доступного) узла.
        self._preferred = 0
        #: Счётчики ошибок по хостам (для информационного отчёта).
        self.errors_per_host = {h: 0 for h in hosts}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self):
        """Закрывает соединения."""
        await self._client.aclose()

    def _rotated(self):
        """Хосты, начиная с предпочтительного (для ротации при сбое)."""
        return self.hosts[self._preferred :] + self.hosts[: self._preferred]

    async def _request(self, method, path, host=None, **kwargs):
        """Один HTTP-запрос; `host` — конкретный узел либо предпочтительный."""
        base = host or self.hosts[self._preferred]
        url = base + path
        return await self._client.request(method, url, **kwargs)

    async def pool(self, host=None, limit=1000):
        """`GET /pool` — живой пул узлов (отсортирован «лёгкими вперёд»)."""
        resp = await self._request("get", f"/pool?limit={limit}", host=host)
        if resp.status_code != 200:
            return []
        data = resp.json()
        return [node for node in data.get("nodes", []) if isinstance(node, str)]

    async def full_total(self, host):
        """`GET /full` — итоговый размер DAG конкретного узла."""
        try:
            resp = await self._request("get", "/full?limit=1", host=host)
            if resp.status_code != 200:
                return 0
            return int(resp.json().get("total", 0))
        except (httpx.HTTPError, ValueError):
            return 0

    async def health(self, host):
        """`GET /health` — проверка живости узла."""
        try:
            resp = await self._request("get", "/health", host=host)
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

    async def find_leader(self, max_wait=20.0):
        """Определяет URL лидера через `POST /mng/metrics`.

        Требует `--internal-token`. Узел — лидер, если его собственный `id`
        совпадает с `current_leader` в его же метриках. Возвращает URL лидера
        либо `None`, если за отведённое время лидер не найден.
        """
        if not self.internal_token:
            return None
        deadline = asyncio.get_event_loop().time() + max_wait
        while asyncio.get_event_loop().time() < deadline:
            for host in self._rotated():
                try:
                    resp = await self._client.post(
                        host + "/mng/metrics",
                        headers={"x-internal-token": self.internal_token},
                    )
                    if resp.status_code != 200:
                        continue
                    metrics = resp.json()
                    leader = metrics.get("current_leader")
                    if leader is not None and metrics.get("id") == leader:
                        self._preferred = self.hosts.index(host)
                        return host
                except (httpx.HTTPError, ValueError):
                    continue
            await asyncio.sleep(0.2)
        return None

    async def send_tx(self, body):
        """Отправляет транзакцию `POST /` с ретраями на временные сбои.

        Возвращает [`ClientOutcome`] с классификацией. Транспортные ошибки
        исчерпывают `--retry` попыток и дают `kind == "error"`.
        """
        attempt = 0
        last_error = None
        while True:
            host = self.hosts[self._preferred]
            started = asyncio.get_event_loop().time()
            try:
                resp = await self._client.post(host + "/", json=body)
                latency_ms = (asyncio.get_event_loop().time() - started) * 1000.0
                status = resp.status_code
                if (status in RETRYABLE_STATUS or status >= 500) and attempt < self.retry:
                    attempt += 1
                    self._rotate()
                    await asyncio.sleep(min(0.02 * (2**attempt), 1.0))
                    continue
                if status == 200:
                    return ClientOutcome(
                        status, "accepted", None, latency_ms, attempts=attempt + 1
                    )
                if 400 <= status < 500:
                    reason = self._parse_message(resp.text)
                    return ClientOutcome(
                        status, "rejected", reason, latency_ms, attempts=attempt + 1
                    )
                reason = self._parse_message(resp.text) or f"http {status}"
                return ClientOutcome(
                    status, "error", reason, latency_ms, attempts=attempt + 1
                )
            except httpx.HTTPError as exc:
                last_error = exc
                self.errors_per_host[host] = self.errors_per_host.get(host, 0) + 1
                if attempt < self.retry:
                    attempt += 1
                    self._rotate()
                    await asyncio.sleep(min(0.02 * (2**attempt), 1.0))
                    continue
                return ClientOutcome(
                    0,
                    "error",
                    f"transport error: {exc}",
                    0.0,
                    transport_error=True,
                    attempts=attempt + 1,
                )

    def _rotate(self):
        """Переключает предпочтительный узел на следующий (ротация при сбое)."""
        self._preferred = (self._preferred + 1) % len(self.hosts)

    @staticmethod
    def _parse_message(body):
        """Извлекает `message` из тела `ApiResponse` (или возвращает как есть)."""
        try:
            import json

            data = json.loads(body)
            message = data.get("message")
            if message:
                return str(message)
        except (ValueError, AttributeError):
            pass
        return body.strip() or None
