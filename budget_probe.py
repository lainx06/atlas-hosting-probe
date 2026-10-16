"""Budget-store feasibility probe (T-03, stage D): synthetic units only.

Not the production budget system. It exercises the database operations that the judge-access
design (docs/HOSTING_PROBE.md section 5a) relies on, against scratch tables, so a hosted run can
show that they behave as designed on the real database: atomic multi-cap reservation, rollback,
idempotent request IDs, consistent lock order, and retention of uncertain dispatched costs.

No provider key, no model request, no real money. Amounts are integer "units". Every function takes
an open psycopg 3 connection created with autocommit=True, so that each `conn.transaction()`
block is a real top-level transaction (with autocommit off, a block that follows a plain read
would only be a savepoint inside an unfinished transaction). This module imports no driver.
"""

import threading
import time
from collections.abc import Callable

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS probe_cap (
        cap_key text PRIMARY KEY,
        cap_units bigint NOT NULL CHECK (cap_units >= 0),
        reserved_units bigint NOT NULL DEFAULT 0 CHECK (reserved_units >= 0),
        spent_units bigint NOT NULL DEFAULT 0 CHECK (spent_units >= 0),
        CHECK (reserved_units + spent_units <= cap_units)
    )""",
    """CREATE TABLE IF NOT EXISTS probe_reservation (
        request_id text PRIMARY KEY,
        state text NOT NULL CHECK (state IN ('reserved', 'dispatched', 'settled', 'released')),
        amount_units bigint NOT NULL CHECK (amount_units > 0),
        actual_units bigint,
        cap_keys text[] NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now()
    )""",
)


class Refused(Exception):
    """A cap row would be exceeded (or does not exist); the whole reservation rolls back."""


def setup(conn) -> None:
    """Create the scratch tables if absent and empty them."""
    with conn.transaction():
        for statement in SCHEMA:
            conn.execute(statement)
        conn.execute("TRUNCATE probe_reservation, probe_cap")


def seed_caps(conn, caps: dict[str, int]) -> None:
    """Reset to the given cap rows, with nothing reserved or spent and no reservations."""
    with conn.transaction():
        conn.execute("TRUNCATE probe_reservation, probe_cap")
        for key, cap in caps.items():
            conn.execute(
                "INSERT INTO probe_cap (cap_key, cap_units) VALUES (%s, %s)", (key, cap)
            )


def reserve(conn, request_id: str, amount: int, cap_keys: list[str]) -> str:
    """Reserve `amount` against every cap in one transaction: all caps or nothing.

    The request ID is the primary key, so a repeated ID never reserves twice. Cap rows are
    updated in sorted key order, so concurrent callers always lock in the same order. Each cap
    update is a single conditional UPDATE: under READ COMMITTED a waiting caller re-checks the
    condition after the other transaction ends.
    Returns "reserved", "duplicate:<state>" or "refused:<cap_key>".
    """
    keys = sorted(set(cap_keys))
    try:
        with conn.transaction():
            inserted = conn.execute(
                "INSERT INTO probe_reservation (request_id, state, amount_units, cap_keys) "
                "VALUES (%s, 'reserved', %s, %s) ON CONFLICT (request_id) DO NOTHING "
                "RETURNING request_id",
                (request_id, amount, keys),
            ).fetchone()
            if inserted is None:
                existing = conn.execute(
                    "SELECT state FROM probe_reservation WHERE request_id = %s",
                    (request_id,),
                ).fetchone()
                return f"duplicate:{existing[0]}"
            for key in keys:
                updated = conn.execute(
                    "UPDATE probe_cap SET reserved_units = reserved_units + %s "
                    "WHERE cap_key = %s AND reserved_units + spent_units + %s <= cap_units",
                    (amount, key, amount),
                ).rowcount
                if updated != 1:
                    raise Refused(key)
    except Refused as refusal:
        return f"refused:{refusal.args[0]}"
    return "reserved"


def mark_dispatched(conn, request_id: str) -> bool:
    """Commit 'dispatched' BEFORE the request is sent, so a crash afterwards retains the cost."""
    with conn.transaction():
        return (
            conn.execute(
                "UPDATE probe_reservation SET state = 'dispatched', updated_at = now() "
                "WHERE request_id = %s AND state = 'reserved'",
                (request_id,),
            ).rowcount
            == 1
        )


def _locked(conn, request_id: str):
    return conn.execute(
        "SELECT state, amount_units, cap_keys FROM probe_reservation "
        "WHERE request_id = %s FOR UPDATE",
        (request_id,),
    ).fetchone()


def settle(conn, request_id: str, actual: int) -> str:
    """Replace a dispatched reservation with the actual usage. Idempotent per request ID."""
    with conn.transaction():
        row = _locked(conn, request_id)
        if row is None:
            return "unknown"
        state, amount, keys = row
        if state == "settled":
            return "already-settled"
        if state != "dispatched":
            return f"invalid-state:{state}"
        if actual > amount:
            return "overrun-retained"  # stays dispatched and reserved for an owner to reconcile
        for key in sorted(keys):
            conn.execute(
                "UPDATE probe_cap SET reserved_units = reserved_units - %s, "
                "spent_units = spent_units + %s WHERE cap_key = %s",
                (amount, actual, key),
            )
        conn.execute(
            "UPDATE probe_reservation SET state = 'settled', actual_units = %s, "
            "updated_at = now() WHERE request_id = %s",
            (actual, request_id),
        )
        return "settled"


def release(conn, request_id: str) -> str:
    """Release a reservation that was never dispatched. A dispatched one is retained."""
    with conn.transaction():
        row = _locked(conn, request_id)
        if row is None:
            return "unknown"
        state, amount, keys = row
        if state == "dispatched":
            return "retained-dispatched"
        if state != "reserved":
            return f"invalid-state:{state}"
        for key in sorted(keys):
            conn.execute(
                "UPDATE probe_cap SET reserved_units = reserved_units - %s WHERE cap_key = %s",
                (amount, key),
            )
        conn.execute(
            "UPDATE probe_reservation SET state = 'released', updated_at = now() "
            "WHERE request_id = %s",
            (request_id,),
        )
        return "released"


def snapshot(conn) -> dict[str, tuple[int, int, int]]:
    """cap_key -> (cap, reserved, spent)."""
    rows = conn.execute(
        "SELECT cap_key, cap_units, reserved_units, spent_units FROM probe_cap"
    ).fetchall()
    return {key: (cap, reserved, spent) for key, cap, reserved, spent in rows}


def count_reservations(conn, request_id: str) -> int:
    return conn.execute(
        "SELECT count(*) FROM probe_reservation WHERE request_id = %s", (request_id,)
    ).fetchone()[0]


def race(connect: Callable, calls: list[Callable]) -> list:
    """Run each call(conn) on its own connection and thread, released together by a barrier."""
    barrier = threading.Barrier(len(calls))
    results: list = [None] * len(calls)

    def worker(index: int) -> None:
        conn = connect()
        try:
            barrier.wait(timeout=30)
            results[index] = calls[index](conn)
        except Exception as exc:  # noqa: BLE001 - recorded as the result of that attempt
            results[index] = f"error:{type(exc).__name__}"
        finally:
            conn.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(len(calls))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    return results


ROUNDS = 20


def scenario_single_cap_race(connect: Callable) -> dict:
    """Two concurrent requests compete for a cap that admits only one."""
    outcomes = []
    with connect() as conn:
        for n in range(ROUNDS):
            seed_caps(conn, {"pool": 100})
            got = race(
                connect,
                [
                    lambda c, n=n: reserve(c, f"race-{n}-a", 60, ["pool"]),
                    lambda c, n=n: reserve(c, f"race-{n}-b", 60, ["pool"]),
                ],
            )
            outcomes.append((sorted(got), snapshot(conn)["pool"][1]))
    ok = all(g == ["refused:pool", "reserved"] and r == 60 for g, r in outcomes)
    return {
        "name": "two concurrent requests, budget admits one",
        "ok": ok,
        "detail": f"{ROUNDS} rounds; exactly one reserved, 60 units held, every round: {ok}",
    }


def scenario_multi_cap_rollback(connect: Callable) -> dict:
    """A reservation refused by its second cap leaves no partial change anywhere."""
    with connect() as conn:
        seed_caps(conn, {"a": 100, "b": 50})
        result = reserve(conn, "multi-1", 60, ["b", "a"])
        state, rows = snapshot(conn), count_reservations(conn, "multi-1")
    ok = (
        result == "refused:b"
        and state == {"a": (100, 0, 0), "b": (50, 0, 0)}
        and rows == 0
    )
    return {
        "name": "multi-cap refusal rolls back everything",
        "ok": ok,
        "detail": f"result={result}; caps unchanged and no reservation row: {ok}",
    }


def scenario_lock_order(connect: Callable) -> dict:
    """Requests naming the same caps in opposite orders never deadlock."""
    bad = []
    with connect() as conn:
        for n in range(ROUNDS):
            seed_caps(conn, {"a": 1000, "b": 1000})
            got = race(
                connect,
                [
                    lambda c, n=n: reserve(c, f"lock-{n}-x", 10, ["a", "b"]),
                    lambda c, n=n: reserve(c, f"lock-{n}-y", 10, ["b", "a"]),
                ],
            )
            if got != ["reserved", "reserved"] or snapshot(conn)["a"][1] != 20:
                bad.append(got)
    return {
        "name": "opposite cap orders, no deadlock",
        "ok": not bad,
        "detail": f"{ROUNDS} rounds; failures: {bad[:3] or 'none'}",
    }


def scenario_idempotent_ids(connect: Callable) -> dict:
    """A repeated request ID reserves once and settles once, even when concurrent."""
    with connect() as conn:
        seed_caps(conn, {"pool": 100})
        got = race(connect, [lambda c: reserve(c, "dup-1", 40, ["pool"])] * 2)
        reserved_once = (
            sorted(g.split(":")[0] for g in got) == ["duplicate", "reserved"]
            and snapshot(conn)["pool"][1] == 40
        )
        mark_dispatched(conn, "dup-1")
        settled = race(connect, [lambda c: settle(c, "dup-1", 25)] * 2)
        spent_once = sorted(settled) == ["already-settled", "settled"] and snapshot(
            conn
        )["pool"] == (100, 0, 25)
    return {
        "name": "duplicate request ID reserves and settles once",
        "ok": reserved_once and spent_once,
        "detail": f"reserve results {sorted(got)}; settle results {sorted(settled)}",
    }


def scenario_uncertain_cost(connect: Callable) -> dict:
    """A dispatched reservation survives a lost connection, is not released, and settles later."""
    with connect() as conn:
        seed_caps(conn, {"pool": 100})
        reserve(conn, "unc-1", 60, ["pool"])
        mark_dispatched(conn, "unc-1")
    with (
        connect() as fresh
    ):  # a new connection stands for a restart or timeout recovery
        released = release(fresh, "unc-1")
        still_held = snapshot(fresh)["pool"] == (100, 60, 0)
        blocked = reserve(fresh, "unc-2", 60, ["pool"]) == "refused:pool"
        settled = settle(fresh, "unc-1", 10)
        final = snapshot(fresh)["pool"] == (100, 0, 10)
    ok = (
        released == "retained-dispatched"
        and still_held
        and blocked
        and settled == "settled"
        and final
    )
    return {
        "name": "uncertain dispatched cost retained across a new connection",
        "ok": ok,
        "detail": (
            f"release={released}; held={still_held}; another request blocked={blocked}; "
            f"settle={settled}; final ok={final}"
        ),
    }


def run_all(connect: Callable) -> list[dict]:
    """Run every scenario on scratch tables and return their results (no secrets involved)."""
    started = time.time()
    with connect() as conn:
        connected = time.time() - started
        setup(conn)
    results = [
        {
            "name": "first connection and setup",
            "ok": True,
            "detail": (
                f"{connected:.2f}s to open the first connection "
                "(name lookup, TLS, login and any compute start combined)"
            ),
        },
        scenario_single_cap_race(connect),
        scenario_multi_cap_rollback(connect),
        scenario_lock_order(connect),
        scenario_idempotent_ids(connect),
        scenario_uncertain_cost(connect),
    ]
    results.append(
        {
            "name": "total",
            "ok": all(r["ok"] for r in results),
            "detail": f"{time.time() - started:.1f}s",
        }
    )
    return results
