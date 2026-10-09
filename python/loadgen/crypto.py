"""Канонический хэш и подпись транзакции — порт `src/utils.rs` (R2).

Единственный источник истины по правилам хэширования/подписи для Python-нагрузчика.
Логика построчно повторяет `canonical_json`/`ordered_sum` из `src/utils.rs`:

* объекты — ключи рекурсивно сортируются (`sorted`);
* массивы — порядок элементов сохраняется;
* строки — JSON-экранирование как в `serde_json` (`json.dumps(..., ensure_ascii=False)`);
* числа — как в `serde_json` (целые — без дробной части);
* `func` добавляется в контент;
* `hash = blake3(canonical)` (hex);
* `sign = ed25519` над **сырыми** байтами хэша (hex), ключ — base58.

Без этого порта подпись не проходит проверку узла (`verify_signature`).
"""

import json

import base58
import blake3
from nacl.signing import SigningKey

#: Известное имя функции-перевода (совпадает с `graph::dag::TRANSFER_TOKEN`).
TRANSFER_TOKEN = "transferToken"


def _write_number(value, out):
    """Пишет число так, как это делает `serde_json::Number::to_string`."""
    if isinstance(value, int):
        out.append(str(value))
        return
    # `float`: у целых значений serde_json печатает без `.0`; иначе — кратчайшее
    # представление (repr в Python близок к Ryu; для наших `var` числа целые).
    if value == int(value):
        out.append(str(int(value)))
    else:
        out.append(repr(value))


def _write_json_string(value, out):
    """JSON-строка с экранированием как в `serde_json`."""
    out.append(json.dumps(value, ensure_ascii=False))


def _write_canonical(value, out):
    if value is None:
        out.append("null")
    elif value is True:
        out.append("true")
    elif value is False:
        out.append("false")
    elif isinstance(value, str):
        _write_json_string(value, out)
    elif isinstance(value, (int, float)):
        _write_number(value, out)
    elif isinstance(value, (list, tuple)):
        out.append("[")
        for i, item in enumerate(value):
            if i > 0:
                out.append(",")
            _write_canonical(item, out)
        out.append("]")
    elif isinstance(value, dict):
        out.append("{")
        for i, key in enumerate(sorted(value.keys())):
            if i > 0:
                out.append(",")
            _write_json_string(key, out)
            out.append(":")
            _write_canonical(value[key], out)
        out.append("}")
    else:
        raise TypeError(f"cannot canonically serialize {type(value)!r}")


def canonical_json(value):
    """Возвращает каноничную JSON-строку (ключи объектов отсортированы)."""
    out = []
    _write_canonical(value, out)
    return "".join(out)


def ordered_sum(tx, func):
    """Канонический хэш транзакции: `blake3(canonical_json(tx + func))` (hex).

    `tx` — словарь с полями `prnts`/`addr`/`seq`/`var`; `func` добавляется в
    подписываемый контент (K2), поэтому подмена `func` меняет хэш.
    """
    value = dict(tx)
    value["func"] = func
    canonical = canonical_json(value)
    return blake3.blake3(canonical.encode("utf-8")).hexdigest()


def sign_message(sk_base58, hash_hex):
    """Подписывает **сырые** байты хэша ed25519-ключом (base58), возвращает hex.

    Ключевой дефект исходного бота — второй `blake3` над digest. Узел подписывает
    и проверяет сырой digest `blake3(canonical)`, поэтому здесь хэш декодируется
    из hex и подписывается как есть.
    """
    seed = base58.b58decode(sk_base58)
    signing_key = SigningKey(seed)
    signed = signing_key.sign(bytes.fromhex(hash_hex))
    return signed.signature.hex()


def address_from_sk(sk_base58):
    """Публичный адрес (base58) для секретного ключа (base58)."""
    seed = base58.b58decode(sk_base58)
    return base58.b58encode(bytes(SigningKey(seed).verify_key)).decode()
