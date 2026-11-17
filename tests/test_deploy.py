"""deploy/deploy.sh against a simulated VM: a local bare repo as GitHub, stand-ins for
sudo/systemctl/journalctl/pip, and shards that can be told to come up unhealthy."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(sys.platform == "win32" or not shutil.which("flock"),
                                reason="deploy.sh runs on the Linux VM")

FAKES = {
    "sudo": '#!/bin/sh\nexec "$@"\n',
    "systemctl": '#!/bin/sh\necho "$*" >> "$FAKE_LOG"\n',
    # Healthy shards report every connection up; shards listed in FAKE_UNHEALTHY never do.
    "journalctl": """#!/bin/sh
unit=$2; shard=${unit#kalshi-collector@}
case ",$FAKE_UNHEALTHY," in *",$shard,"*) echo "msgs=10 (5/s) gaps=0 markets=9 conns_up=3/4" ;;
  *) echo "msgs=10 (5/s) gaps=0 markets=9 conns_up=4/4" ;; esac
""",
}


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def vm(tmp_path):
    origin, dev, repo, state, units, bin_ = (tmp_path / n for n in ("origin.git", "dev", "vm-repo", "state", "units", "bin"))
    for d in (state, units, bin_, state / ".venv" / "bin"):
        d.mkdir(parents=True)
    for name, body in FAKES.items():
        (bin_ / name).write_text(body)
        (bin_ / name).chmod(0o755)
    (state / ".venv/bin/python").symlink_to(sys.executable)
    (state / ".venv/bin/pip").write_text('#!/bin/sh\necho "pip $*" >> "$FAKE_LOG"\n')
    (state / ".venv/bin/pip").chmod(0o755)

    git(tmp_path, "init", "--bare", "-q", "-b", "main", str(origin))
    shutil.copytree(ROOT, dev, ignore=shutil.ignore_patterns(".git", "__pycache__", ".pytest_cache"))
    git(dev, "init", "-q", "-b", "main")
    git(dev, "-c", "user.email=t@t", "-c", "user.name=t", "add", "-A")
    git(dev, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "initial")
    git(dev, "remote", "add", "origin", str(origin))
    git(dev, "push", "-q", "origin", "main", "main:deploy")
    git(tmp_path, "clone", "-q", "-b", "deploy", str(origin), str(repo))
    git(repo, "checkout", "-q", "--detach", "origin/deploy")

    log = tmp_path / "calls.log"
    env = dict(os.environ, PATH=f"{bin_}:{os.environ['PATH']}", FAKE_LOG=str(log), FAKE_UNHEALTHY="",
               REPO=str(repo), VENV=str(state / ".venv"), STATE_DIR=str(state), SYSTEMD_DIR=str(units),
               HEALTH_TIMEOUT="3", HEALTH_POLL="1", DEPLOY_HEALTHCHECK_URL="")

    class VM:
        def commit(self, path, text, msg="change"):
            (dev / path).write_text(text)
            git(dev, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qam", msg)
            return git(dev, "rev-parse", "HEAD")

        def promote(self):                       # what CI does when tests pass
            git(dev, "push", "-q", "origin", "main", "main:deploy")

        def deploy(self, unhealthy=""):
            log.write_text("")
            r = subprocess.run([str(repo / "deploy/deploy.sh")], env=dict(env, FAKE_UNHEALTHY=unhealthy),
                               capture_output=True, text=True, timeout=60)
            # The VM runs deploy.sh from its own checkout, so it is whatever version is checked out.
            return r.returncode, r.stdout + r.stderr, log.read_text().splitlines()

        head = property(lambda self: git(repo, "rev-parse", "HEAD"))
        bad = property(lambda self: (state / ".deploy-bad-commit").read_text().strip()
                       if (state / ".deploy-bad-commit").exists() else None)
        units_dir = units
        dev_dir = dev

    return VM()


def restarts(calls):
    return [c.split()[-1] for c in calls if c.startswith("restart kalshi-collector@")]


def test_nothing_new_is_a_quiet_no_op(vm):
    code, out, calls = vm.deploy()
    assert code == 0 and out == "" and calls == []


def test_collector_change_rolls_shards_one_at_a_time(vm):
    new = vm.commit("collector/collector.py", (vm.dev_dir / "collector/collector.py").read_text() + "\n# tweak\n")
    vm.promote()
    code, out, calls = vm.deploy()
    assert code == 0, out
    assert vm.head == new
    assert restarts(calls) == ["kalshi-collector@0", "kalshi-collector@1"]
    assert sorted(p.name for p in vm.units_dir.iterdir()) == sorted(
        p.name for p in (vm.dev_dir / "deploy/systemd").iterdir())          # first run installs units
    assert "daemon-reload" in calls


def test_change_outside_collector_does_not_restart(vm):
    vm.commit("converter/count_markets.py", (vm.dev_dir / "converter/count_markets.py").read_text() + "\n")
    vm.promote()
    vm.deploy()                                    # installs units on the first run
    vm.commit("converter/count_markets.py", (vm.dev_dir / "converter/count_markets.py").read_text() + "\n")
    vm.promote()
    code, out, calls = vm.deploy()
    assert code == 0 and restarts(calls) == [] and "no restart needed" in out


def test_unhealthy_shard_rolls_back_and_is_not_retried(vm):
    vm.deploy()
    old = vm.head
    bad = vm.commit("collector/collector.py", (vm.dev_dir / "collector/collector.py").read_text() + "\n# bad\n")
    vm.promote()
    code, out, calls = vm.deploy(unhealthy="1")
    assert code == 1 and "Rolling back" in out
    assert vm.head == old and vm.bad == bad
    # shard 0 and 1 went to the new code; both were put back, 0 first
    assert restarts(calls) == ["kalshi-collector@0", "kalshi-collector@1", "kalshi-collector@0", "kalshi-collector@1"]

    code, out, calls = vm.deploy()                 # same bad commit: left alone
    assert code == 0 and calls == [] and vm.head == old

    fixed = vm.commit("collector/collector.py", (vm.dev_dir / "collector/collector.py").read_text() + "\n# fixed\n")
    vm.promote()
    code, out, calls = vm.deploy()
    assert code == 0 and vm.head == fixed and vm.bad is None


def test_first_shard_failing_leaves_the_second_untouched(vm):
    vm.deploy()
    vm.commit("collector/collector.py", (vm.dev_dir / "collector/collector.py").read_text() + "\n# x\n")
    vm.promote()
    code, out, calls = vm.deploy(unhealthy="0")
    assert code == 1 and restarts(calls) == ["kalshi-collector@0", "kalshi-collector@0"]


def test_code_that_does_not_compile_never_restarts_anything(vm):
    vm.deploy()
    old = vm.head
    vm.commit("collector/collector.py", "def broken(:\n")
    vm.promote()
    code, out, calls = vm.deploy()
    assert code == 1 and "does not compile" in out and restarts(calls) == [] and vm.head == old


def test_new_requirements_are_installed(vm):
    vm.deploy()
    vm.commit("collector/requirements.txt", (vm.dev_dir / "collector/requirements.txt").read_text() + "tzdata\n")
    vm.promote()
    code, out, calls = vm.deploy()
    assert code == 0 and any(c.startswith("pip install") for c in calls)


def test_local_edits_on_the_vm_block_deploys(vm, tmp_path):
    vm.deploy()
    repo = Path(vm.units_dir).parent / "vm-repo"
    (repo / "collector/collector.py").write_text("# edited by hand on the VM\n")
    vm.commit("collector/collector.py", (vm.dev_dir / "collector/collector.py").read_text() + "\n# y\n")
    vm.promote()
    code, out, calls = vm.deploy()
    assert code == 1 and "local changes" in out and restarts(calls) == []


def test_deploy_can_update_itself(vm):
    """A deploy that rewrites deploy.sh (here: longer, so old byte offsets point mid-file)
    must still finish cleanly; bash keeps reading a script file while it runs."""
    vm.deploy()
    script = vm.dev_dir / "deploy/deploy.sh"
    text = script.read_text()
    body = text.replace("set -euo pipefail\n", "set -euo pipefail\n" + "# padding\n" * 200, 1)
    (vm.dev_dir / "collector/collector.py").write_text((vm.dev_dir / "collector/collector.py").read_text() + "\n# z\n")
    new = vm.commit("deploy/deploy.sh", body)
    vm.promote()
    code, out, calls = vm.deploy()
    assert code == 0, out
    assert vm.head == new and restarts(calls) == ["kalshi-collector@0", "kalshi-collector@1"]
    assert "command not found" not in out and "syntax error" not in out
