"""Golden-вектор канонического хэша/подписи (сверен с `src/utils.rs`).

Контрольная транзакция и ключ №0 из `keys.py` дают известные хэш и подпись,
побайтово совпадающие с Rust (`ordered_sum` + ed25519 над сырым digest).
"""

import base58
import pytest
from nacl.signing import VerifyKey

import keys
from loadgen import crypto

PK0, PK1 = keys.load_keys()[0][1], keys.load_keys()[1][1]
SK0 = keys.load_keys()[0][0]

#: Хэш и подпись для контрольной tx, вычисленные Rust-реализацией `utils.rs`.
GOLDEN_HASH = "1e935195da0c4dfe20b7dbf408907d923027b850e80474d4c5a61dbe199cabad"
GOLDEN_SIGN = (
    "989464ef8e95689b26f0e6bdf725019e35d151de6050b64fd063acc582b0a9d3"
    "ee29497e5ea7a70f34eaad454b75273a6f7740e38f14e8f689e448637191410d"
)


def control_tx():
    return {
        "prnts": ["aaa", "bbb"],
        "addr": PK0,
        "seq": 1,
        "var": {"ca": PK0, "to": PK1, "val": 100, "msg": "hello"},
    }


def test_golden_hash_matches_rust():
    assert crypto.ordered_sum(control_tx(), "transferToken") == GOLDEN_HASH


def test_golden_signature_matches_rust_and_verifies():
    assert crypto.sign_message(SK0, GOLDEN_HASH) == GOLDEN_SIGN
    # Подпись принимается верификатором узла (ed25519 над сырыми байтами хэша).
    VerifyKey(base58.b58decode(PK0)).verify(
        bytes.fromhex(GOLDEN_HASH), bytes.fromhex(GOLDEN_SIGN)
    )


def test_signature_is_over_raw_digest_not_second_hash():
    # Дефект исходного бота: подпись над blake3(blake3(canonical)). Узел такую
    # подпись отклоняет — здесь проверяем, что наш формат иной и корректен.
    import blake3

    double_hashed = blake3.blake3(bytes.fromhex(GOLDEN_HASH)).hexdigest()
    wrong = crypto.sign_message(SK0, double_hashed)
    assert wrong != GOLDEN_SIGN
    with pytest.raises(Exception):
        VerifyKey(base58.b58decode(PK0)).verify(
            bytes.fromhex(GOLDEN_HASH), bytes.fromhex(wrong)
        )


def test_func_is_covered_by_hash():
    left = crypto.ordered_sum(control_tx(), "transferToken")
    right = crypto.ordered_sum(control_tx(), "otherFunc")
    assert left != right


def test_no_concatenation_collision():
    a = {**control_tx(), "var": {"ca": "a", "to": "bc"}}
    b = {**control_tx(), "var": {"ca": "ab", "to": "c"}}
    assert crypto.ordered_sum(a, "transferToken") != crypto.ordered_sum(b, "transferToken")


def test_parent_order_matters_and_keys_are_sorted():
    tx_reordered_keys = {
        "seq": 1,
        "var": {"msg": "hello", "val": 100, "to": PK1, "ca": PK0},
        "addr": PK0,
        "prnts": ["aaa", "bbb"],
    }
    assert crypto.ordered_sum(tx_reordered_keys, "transferToken") == GOLDEN_HASH
    # Порядок массива родителей сохраняется (хэш меняется).
    swapped = {**control_tx(), "prnts": ["bbb", "aaa"]}
    assert crypto.ordered_sum(swapped, "transferToken") != GOLDEN_HASH


def test_canonical_json_escapes_and_sorts():
    assert crypto.canonical_json({"b": 1, "a": "x"}) == '{"a":"x","b":1}'
    assert crypto.canonical_json(["z", True, None]) == '["z",true,null]'
    assert crypto.canonical_json({"s": "привет"}) == '{"s":"привет"}'


def test_address_from_sk_matches_keypair():
    assert crypto.address_from_sk(SK0) == PK0
