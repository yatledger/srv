"""CLI-конфигурация нагрузчика и валидация параметров (R4)."""

import argparse
import os

DEFAULT_HOST = "http://127.0.0.1:21001"


def _normalize_host(host):
    """Приводит хост к виду `scheme://host[:port]` без завершающего слэша."""
    host = host.strip().rstrip("/")
    if not host:
        raise ValueError("пустой хост")
    if "://" not in host:
        host = "http://" + host
    return host


def build_parser():
    """Собирает парсер аргументов командной строки."""
    parser = argparse.ArgumentParser(
        prog="python -m loadgen",
        description="Нагрузчик dagdb: внешний HTTP-клиент к запущенному кластеру.",
    )
    parser.add_argument(
        "--hosts",
        nargs="+",
        default=[DEFAULT_HOST],
        help="адреса узлов кластера (по умолчанию один локальный узел)",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--tx", type=int, default=None, help="число транзакций")
    group.add_argument(
        "--duration-sec", type=int, default=None, help="длительность прогона, с"
    )
    parser.add_argument(
        "--tps", type=int, default=500, help="целевой суммарный TPS (0 — без ограничения)"
    )
    parser.add_argument("--concurrency", type=int, default=8, help="число sender-задач")
    parser.add_argument("--accounts", type=int, default=16, help="число аккаунтов")
    parser.add_argument(
        "--parents-min", type=int, default=2, help="минимум родителей на транзакцию"
    )
    parser.add_argument(
        "--parents-max", type=int, default=8, help="максимум родителей на транзакцию"
    )
    parser.add_argument("--seed", type=int, default=1, help="seed ГПСЧ (воспроизводимость)")
    parser.add_argument("--json-out", default=None, help="путь JSON-отчёта")
    parser.add_argument(
        "--retry", type=int, default=10, help="число повторов на временный сбой"
    )
    parser.add_argument("--timeout", type=int, default=30, help="таймаут HTTP-запроса, с")
    parser.add_argument(
        "--internal-token",
        default=os.environ.get("INTERNAL_API_TOKEN") or None,
        help="токен внутреннего API (поиск лидера); по умолчанию из INTERNAL_API_TOKEN",
    )
    parser.add_argument("--verbose", action="store_true", help="подробный вывод")
    return parser


class Config:
    """Проверенная конфигурация прогона."""

    def __init__(self, args):
        self.hosts = [_normalize_host(h) for h in args.hosts]
        self.tx = args.tx
        self.duration_sec = args.duration_sec
        self.tps = args.tps
        self.concurrency = args.concurrency
        self.accounts = args.accounts
        self.parents_min = args.parents_min
        self.parents_max = args.parents_max
        self.seed = args.seed
        self.json_out = args.json_out
        self.retry = args.retry
        self.timeout = args.timeout
        self.internal_token = args.internal_token
        self.verbose = args.verbose

    def validate(self):
        """Проверяет диапазоны; бросает `ValueError` с человекочитаемой причиной."""
        if not self.hosts:
            raise ValueError("нужен хотя бы один хост")
        if self.tx is None and self.duration_sec is None:
            self.tx = 500
        if self.tx is not None and self.tx <= 0:
            raise ValueError("--tx должен быть больше 0")
        if self.duration_sec is not None and self.duration_sec <= 0:
            raise ValueError("--duration-sec должен быть больше 0")
        if self.tps < 0:
            raise ValueError("--tps не может быть отрицательным")
        if self.concurrency < 1:
            raise ValueError("--concurrency должен быть не меньше 1")
        if self.accounts < 1:
            raise ValueError("--accounts должен быть не меньше 1")
        if self.parents_min < 2:
            raise ValueError("--parents-min должен быть не меньше 2")
        if self.parents_max < self.parents_min:
            raise ValueError("--parents-max не может быть меньше --parents-min")
        if self.retry < 0:
            raise ValueError("--retry не может быть отрицательным")
        if self.timeout <= 0:
            raise ValueError("--timeout должен быть больше 0")
        return self

    @property
    def tx_budget(self):
        """Бюджет транзакций (`None`, если прогон ограничен временем)."""
        return None if self.duration_sec is not None else self.tx

    @classmethod
    def parse(cls, argv=None):
        """Разбирает `argv` и возвращает готовую конфигурацию."""
        args = build_parser().parse_args(argv)
        return cls(args).validate()
