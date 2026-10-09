"""Генератор валидных транзакций (R3).

Строит транзакции, проходящие детерминированную валидацию state machine:
`prnts` — `k` живых узлов из `/pool`; `seq` — строго монотонный по адресу
(счётчик у каждого аккаунта); `var` корректной структуры; подпись — ed25519 над
каноническим хэшем из [`loadgen.crypto`]. Один настраиваемый генератор вместо
мёртвых «реалистичных» из исходного бота (R8).
"""

import asyncio

from .crypto import TRANSFER_TOKEN, ordered_sum, sign_message

#: Верхняя граница числа родителей (совпадает с `validate_parents`).
MAX_PARENTS = 100


class Prng:
    """Детерминированный xorshift64* ГПСЧ (без внешних зависимостей).

    При фиксированном `seed` параметры транзакций воспроизводимы (R7).
    Криптостойкость не требуется — генератор лишь разыгрывает нагрузку.
    """

    _DEFAULT_SEED = 0x9E3779B97F4A7C15
    _MASK = (1 << 64) - 1

    def __init__(self, seed):
        self.state = seed & self._MASK or self._DEFAULT_SEED

    def next_u64(self):
        x = self.state
        x ^= x >> 12
        x ^= (x << 25) & self._MASK
        x ^= x >> 27
        self.state = x & self._MASK
        return (self.state * 0x2545F4914F6CDD1D) & self._MASK

    def below(self, bound):
        """Случайное число в `[0, bound)` (0 при `bound == 0`)."""
        if bound <= 0:
            return 0
        return self.next_u64() % bound

    def range_inclusive(self, low, high):
        """Случайное число в `[low, high]` включительно."""
        if high <= low:
            return low
        return low + self.below(high - low + 1)


class Account:
    """Аккаунт-отправитель: адрес, ключ, монотонный счётчик `seq` и лок."""

    def __init__(self, index, sk, pk):
        self.index = index
        self.sk = sk
        self.addr = pk
        self.seq = 0
        # Сериализует выдачу `seq`: следующая транзакция аккаунта создаётся
        # только после получения ответа на предыдущую (монотонность V18).
        self.lock = asyncio.Lock()

    def next_seq(self):
        """Возвращает следующий `seq` (с 1). Вызывать под `self.lock`."""
        self.seq += 1
        return self.seq


class Generator:
    """Генератор подписанных транзакций по набору аккаунтов."""

    def __init__(self, keypairs, parents_min, parents_max):
        self.accounts = [
            Account(i, sk, pk) for i, (sk, pk) in enumerate(keypairs)
        ]
        self.parents_min = parents_min
        self.parents_max = parents_max

    def account_count(self):
        """Число аккаунтов."""
        return len(self.accounts)

    def effective_parent_count(self, pool_len, rng):
        """`k` родителей для размера пула `pool_len`; `None`, если пул меньше 2.

        `k = clamp(random(parents_min, parents_max), 2, min(100, pool_len))`.
        Диапазон числа родителей трактуется как зажимаемый: если задан минимум
        выше доступного, берётся максимум из возможного (`min(100, |pool|)`).
        """
        if pool_len < 2:
            return None
        max_allowed = min(MAX_PARENTS, pool_len)
        low = min(self.parents_min, max_allowed)
        high = min(max(self.parents_max, self.parents_min), max_allowed)
        low = min(low, high)
        return rng.range_inclusive(low, high)

    def build(self, index, parents, rng):
        """Строит подписанную транзакцию для аккаунта `index`.

        Увеличивает счётчик `seq` аккаунта. Вызывающий обязан удерживать
        `self.accounts[index].lock` до получения ответа от кластера.
        """
        account = self.accounts[index]
        seq = account.next_seq()
        to = self.accounts[rng.below(len(self.accounts))].addr
        val = rng.range_inclusive(1, 1_000_000)
        msg_len = rng.below(32)
        msg = "".join(chr(ord("a") + rng.below(26)) for _ in range(msg_len))
        tx = {
            "prnts": list(parents),
            "addr": account.addr,
            "seq": seq,
            "var": {"ca": account.addr, "to": to, "val": val, "msg": msg},
        }
        tx_hash = ordered_sum(tx, TRANSFER_TOKEN)
        sign = sign_message(account.sk, tx_hash)
        return {"tx": tx, "sign": sign, "func": TRANSFER_TOKEN}


def choose_parents(pool, k, rng):
    """Выбирает `k` уникальных родителей из пула.

    Пул отсортирован по числу активных родителей (`/pool`), поэтому берём окно
    из начала со случайным смещением — нагрузка распределяется по «лёгким» узлам.
    """
    if not pool or k <= 0:
        return []
    k = min(k, len(pool))
    if k == len(pool):
        return list(pool)
    start = rng.below(len(pool) - k + 1)
    return list(pool[start : start + k])


def derive_rng(seed, worker_id):
    """Детерминированно выводит ГПСЧ задачи из общего seed (R7)."""
    mixed = (worker_id * 0x9E3779B97F4A7C15) ^ seed
    return Prng(mixed)
