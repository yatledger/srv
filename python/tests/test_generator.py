"""Тесты генератора: монотонность `seq`, границы родителей, детерминизм (R3, R7)."""

import asyncio

import keys
from loadgen.generator import MAX_PARENTS, Generator, Prng, derive_rng
from loadgen.generator import choose_parents as _choose_parents
from loadgen.runner import account_block


def make_generator(accounts=4, pmin=2, pmax=4):
    keypairs = keys.load_keys()[:accounts]
    return Generator(keypairs, parents_min=pmin, parents_max=pmax)


def test_seq_is_strictly_monotonic_sequential():
    gen = make_generator(accounts=1)
    rng = Prng(10)
    parents = ["a", "b"]
    last = 0
    for _ in range(100):
        body = gen.build(0, parents, rng)
        seq = body["tx"]["seq"]
        assert seq == last + 1
        last = seq


def test_parallel_issuance_never_repeats_seq():
    gen = make_generator(accounts=8)
    parents = ["a", "b"]

    async def worker(index, iterations):
        rng = Prng(100 + index)
        seen = []
        for _ in range(iterations):
            account = gen.accounts[index]
            async with account.lock:
                body = gen.build(index, parents, rng)
                seen.append(body["tx"]["seq"])
        return index, seen

    async def main():
        return await asyncio.gather(*[worker(i, 200) for i in range(8)])

    results = asyncio.run(main())
    seen_pairs = set()
    for index, seqs in results:
        assert seqs == list(range(1, 201)), "seq внутри аккаунта не монотонен"
        for seq in seqs:
            assert (index, seq) not in seen_pairs, "повтор (аккаунт, seq)"
            seen_pairs.add((index, seq))
    assert len(seen_pairs) == 8 * 200


def test_parameters_deterministic_for_seed():
    left = make_generator(accounts=3)
    right = make_generator(accounts=3)
    for i in range(3):
        assert left.accounts[i].addr == right.accounts[i].addr
    body_left = left.build(0, ["a", "b"], derive_rng(1, 0))
    body_right = right.build(0, ["a", "b"], derive_rng(1, 0))
    assert body_left == body_right


def test_effective_parent_count_respects_bounds():
    gen = make_generator(accounts=1, pmin=2, pmax=8)
    rng = Prng(3)
    assert gen.effective_parent_count(0, rng) is None
    assert gen.effective_parent_count(1, rng) is None
    for pool_len in range(2, 20):
        k = gen.effective_parent_count(pool_len, rng)
        assert 2 <= k <= min(8, pool_len)

    gen = make_generator(accounts=1, pmin=50, pmax=150)
    rng = Prng(4)
    assert gen.effective_parent_count(3, rng) == 3
    k = gen.effective_parent_count(500, rng)
    assert 50 <= k <= MAX_PARENTS


def test_derive_rng_varies_by_seed_and_worker():
    assert derive_rng(1, 0).next_u64() == derive_rng(1, 0).next_u64()
    assert derive_rng(1, 0).next_u64() != derive_rng(2, 0).next_u64()
    assert derive_rng(1, 0).next_u64() != derive_rng(1, 1).next_u64()
    assert derive_rng(0, 0).next_u64() != 0


def test_choose_parents_bounds_and_uniqueness():
    pool = [str(i) for i in range(50)]
    rng = Prng(3)
    for k in range(2, 51):
        parents = _choose_parents(pool, k, rng)
        assert len(parents) == k
        assert len(set(parents)) == k
    assert _choose_parents(pool, 0, rng) == []
    assert _choose_parents([], 2, rng) == []
    assert len(_choose_parents(pool, 999, rng)) == len(pool)


def test_account_blocks_cover_all_accounts_once():
    for workers in range(1, 9):
        for accounts in range(1, 18):
            effective = max(1, min(workers, accounts))
            seen = [0] * accounts
            total = 0
            for worker_id in range(workers):
                start, length = account_block(worker_id, workers, accounts)
                assert start + length <= accounts
                if worker_id >= effective:
                    assert length == 0
                    continue
                assert length > 0
                for slot in range(start, start + length):
                    seen[slot] += 1
                    total += 1
            assert total == accounts
            assert all(count == 1 for count in seen)
