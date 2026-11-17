"""Shared test setup.

The collector reads its config and signing key at import time, so a throwaway
Ed25519 key and environment are created here, before any test imports it.
Converter tests run convert.py as a subprocess, against local folders or a
stand-in `rclone` (tests/fakebin/rclone) that maps the remote `fake:` to a folder.
"""
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

ROOT = Path(__file__).resolve().parents[1]
CONVERT = ROOT / "converter" / "convert.py"
FAKEBIN = Path(__file__).resolve().parent / "fakebin"
sys.path.insert(0, str(ROOT / "collector"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

_session = Path(tempfile.mkdtemp(prefix="kalshi-tests-"))
_key = _session / "test.key"
_key.write_bytes(ed25519.Ed25519PrivateKey.generate().private_bytes(
    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
os.environ.update(KALSHI_KEY_ID="test-key", KALSHI_KEY_PATH=str(_key),
                  DATA_DIR=str(_session / "data"), N_CONN="2", N_SHARDS="1", SHARD="0")


def run_convert(raw, out, work, *extra, env=None, timeout=600):
    """Run convert.py; return (exit code, list of log messages without timestamps)."""
    cmd = [sys.executable, str(CONVERT), "--raw", str(raw), "--out", str(out), "--work", str(work),
           "--threads", "2", "--memory-limit", "2GB", *map(str, extra)]
    r = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)
    lines = [l.split(" ", 2)[2] if l[:2] == "20" else l for l in r.stderr.splitlines()]
    return r.returncode, lines, r.stdout


@pytest.fixture
def fake_remote(tmp_path):
    """A folder posing as the B2 bucket, plus the environment that makes `rclone` the stand-in."""
    remote = tmp_path / "remote"
    (remote / "bucket").mkdir(parents=True)
    os.chmod(FAKEBIN / "rclone", 0o755)
    env = dict(os.environ, PATH=f"{FAKEBIN}{os.pathsep}{os.environ['PATH']}", FAKE_REMOTE=str(remote))
    return remote / "bucket", env
