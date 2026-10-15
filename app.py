"""ATLAS hosting probe, stage A: canary only.

Two checks, reported separately. This file makes no network requests and holds no credentials.
  1. Build/import: the page renders, so the host built the environment and started Streamlit.
  2. Secret store: a harmless canary secret named CANARY is present. The page shows only
     presence and the first 8 hex digits of its SHA-256, never the value.
"""

import hashlib
import platform
import sys
import time

import streamlit as st

STARTED = time.time()


def canary_report() -> dict:
    try:
        value = st.secrets.get("CANARY")
    except (FileNotFoundError, KeyError):  # no secrets configured
        value = None
    if not isinstance(value, str) or not value:
        return {"present": False}
    return {
        "present": True,
        "sha256_prefix": hashlib.sha256(value.encode()).hexdigest()[:8],
    }


st.title("ATLAS hosting probe: stage A")
st.caption("Canary only. No ATLAS code or data, no network requests, no real keys.")

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
