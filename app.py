"""ATLAS hosting probe, stage D: canary plus budget-store feasibility (synthetic units only).

Checks, reported separately. No provider key, no model request, no ATLAS code.
  1. Build/import and 2. secret canary, as in stage A. Both are public.
  3. Budget-store probe. The app is public, so this step is protected: it can run only when a
     DATABASE_URL secret AND a PROBE_ACCESS_CODE secret exist and the visitor has typed the
     matching code. Without all three, no database connection is opened. It uses scratch tables
     and synthetic integer units, and never prints the URL or the code.
"""

import hashlib
import hmac
import platform
import sys
import time

import budget_probe
import streamlit as st

STARTED = time.time()


def secret(name: str) -> str | None:
    try:
        value = st.secrets.get(name)
    except (FileNotFoundError, KeyError):  # no secrets configured
        return None
    return value if isinstance(value, str) and value else None


def canary_report() -> dict:
    value = secret("CANARY")
    if value is None:
        return {"present": False}
    return {
        "present": True,
        "sha256_prefix": hashlib.sha256(value.encode()).hexdigest()[:8],
    }


def code_matches(given: str, expected: str) -> bool:
    """Constant-time comparison; an empty entry never matches."""
    return bool(given) and hmac.compare_digest(given.encode(), expected.encode())


def run_budget_probe(url: str) -> list[dict]:
    import psycopg  # imported here so the page renders without a database driver

    def connect():
        return psycopg.connect(url, autocommit=True, connect_timeout=15)

    try:
        return budget_probe.run_all(connect)
    except Exception as exc:  # noqa: BLE001 - the type only: messages may contain the host name
        return [
            {
                "name": "budget probe",
                "ok": False,
                "detail": f"stopped: {type(exc).__name__}",
            }
        ]


st.title("ATLAS hosting probe: stage D")
st.caption(
    "Canary and budget-store probe. Synthetic units only. No provider key or request."
)

st.subheader("1. Build and import")
st.write(
    {
        "python": sys.version.split()[0],
        "streamlit": st.__version__,
        "platform": platform.platform(),
        "process_uptime_s": round(time.time() - STARTED, 1),
        "page_rendered_at_unix": int(time.time()),
    }
)

st.subheader("2. Secret store (canary)")
st.write(canary_report())

st.subheader("3. Budget-store probe (scratch tables, synthetic units)")
database_url = secret("DATABASE_URL")
access_code = secret("PROBE_ACCESS_CODE")
if database_url is None:
    st.write("No DATABASE_URL secret configured: skipped.")
elif access_code is None:
    st.write("Disabled: no PROBE_ACCESS_CODE secret is configured.")  # fail closed
else:
    typed = st.text_input("Probe access code", type="password")
    if not typed:
        st.write("Enter the probe access code to enable the test.")
    elif not code_matches(typed, access_code):
        st.write("Not authorized.")
    elif st.button("Run budget probe"):  # reached only after a successful authorization
        st.write(run_budget_probe(database_url))
