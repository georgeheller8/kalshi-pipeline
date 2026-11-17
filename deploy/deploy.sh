#!/usr/bin/env bash
# Pull-based deploy for the collector VM, run every 2 minutes by kalshi-deploy.timer.
#
# 1. Fetch the `deploy` branch. CI moves it only to commits on main whose tests passed.
# 2. Nothing new (or the new commit already failed here once): exit quietly.
# 3. Check out the commit, install changed dependencies, install changed systemd units.
# 4. If collector code or its unit changed: restart the shards ONE AT A TIME, waiting for
#    each to report all its connections up before touching the next, so at most one
#    shard's markets are ever missing data.
# 5. A shard that doesn't come back healthy: roll back to the previous commit, restart
#    that shard on the old code, remember the bad commit so it isn't retried, and exit 1.
#
# Everything lives inside main(), so bash has parsed the whole script before step 3
# can overwrite this file with a new version.
set -euo pipefail

main() {
    local repo=${REPO:-/home/ubuntu/kalshi-pipeline}
    local venv=${VENV:-/home/ubuntu/collector/.venv}
    local state=${STATE_DIR:-/home/ubuntu/collector}
    local branch=${DEPLOY_BRANCH:-deploy}
    local shards=${SHARDS:-"0 1"}
    local timeout=${HEALTH_TIMEOUT:-240}
    local unit_dir=${SYSTEMD_DIR:-/etc/systemd/system}
    local sudo=${SUDO:-sudo}
    local bad_file="$state/.deploy-bad-commit"
    local lock="$state/.deploy.lock"

    exec 9>"$lock"
    flock -n 9 || { echo "another deploy is running"; return 0; }

    cd "$repo"
    if ! git fetch --quiet origin "$branch"; then
        echo "could not fetch origin/$branch (no network, or CI hasn't created it yet)"
        return 1
    fi
    local old new
    old=$(git rev-parse HEAD)
    new=$(git rev-parse "origin/$branch")
    [[ "$old" == "$new" ]] && return 0
    if [[ -f "$bad_file" && "$(cat "$bad_file")" == "$new" ]]; then
        return 0   # already failed here; waiting for a newer commit
    fi
    if ! git diff --quiet HEAD; then
        echo "local changes in $repo: refusing to deploy over them"
        ping_hc fail "local changes in $repo"
        return 1
    fi

    local changed
    changed=$(git diff --name-only "$old" "$new")
    echo "deploying ${new:0:10} (was ${old:0:10}):"
    sed 's/^/  /' <<<"$changed"
    git checkout --quiet --detach "$new"

    if grep -qx 'collector/requirements.txt' <<<"$changed"; then
        echo "installing collector dependencies"
        "$venv/bin/pip" install --quiet -r collector/requirements.txt
    fi

    # systemd units: install any that changed, then reload
    local units_changed=0 collector_unit_changed=0 f name
    for f in deploy/systemd/*; do
        name=$(basename "$f")
        if ! cmp -s "$f" "$unit_dir/$name"; then
            echo "installing $name"
            $sudo cp "$f" "$unit_dir/$name"
            units_changed=1
            [[ "$name" == kalshi-collector@.service ]] && collector_unit_changed=1
        fi
    done
    if (( units_changed )); then
        $sudo systemctl daemon-reload
        $sudo systemctl enable --quiet b2-upload.timer kalshi-deploy.timer
    fi

    if ! grep -q '^collector/' <<<"$changed" && (( ! collector_unit_changed )); then
        echo "collector unchanged: no restart needed"
        rm -f "$bad_file"
        ping_hc "" "deployed ${new:0:10} (no restart)"
        return 0
    fi

    if ! "$venv/bin/python" -m py_compile collector/collector.py; then
        rollback "$old" "$new" "" "collector.py does not compile"
        return 1
    fi

    local s since
    for s in $shards; do
        since=$(date '+%Y-%m-%d %H:%M:%S')
        echo "restarting shard $s"
        $sudo systemctl restart "kalshi-collector@$s"
        if ! wait_healthy "$s" "$since" "$timeout"; then
            rollback "$old" "$new" "$s" "shard $s not healthy within ${timeout}s"
            return 1
        fi
        echo "shard $s healthy"
    done
    rm -f "$bad_file"
    echo "deployed ${new:0:10}"
    ping_hc "" "deployed ${new:0:10}"
}

# Healthy = a stats line since the restart showing every connection up (conns_up=N/N).
wait_healthy() {
    local shard=$1 since=$2 timeout=$3 waited=0
    while (( waited < timeout )); do
        sleep "${HEALTH_POLL:-5}"
        waited=$(( waited + ${HEALTH_POLL:-5} ))
        if journalctl -u "kalshi-collector@$shard" --since "$since" -o cat 2>/dev/null \
                | grep -Eq 'conns_up=([0-9]+)/\1( |$)'; then
            return 0
        fi
    done
    return 1
}

rollback() {
    local old=$1 new=$2 shard=$3 why=$4 s
    echo "DEPLOY FAILED: $why. Rolling back to ${old:0:10}"
    echo "$new" > "$bad_file"
    git checkout --quiet --detach "$old"
    # Put back the old unit files too, in case the new ones were the problem.
    for f in deploy/systemd/*; do
        cmp -s "$f" "$unit_dir/$(basename "$f")" || $sudo cp "$f" "$unit_dir/$(basename "$f")"
    done
    $sudo systemctl daemon-reload
    # Shards restarted onto the new code (up to and including the one that failed) go back
    # to the old code, one at a time. Shards not reached yet never left the old code.
    if [[ -n "$shard" ]]; then
        local since
        for s in $shards; do
            echo "restarting shard $s on ${old:0:10}"
            since=$(date '+%Y-%m-%d %H:%M:%S')
            $sudo systemctl restart "kalshi-collector@$s" || true
            [[ "$s" == "$shard" ]] && break
            wait_healthy "$s" "$since" "$timeout" || true
        done
    fi
    ping_hc fail "rolled back ${new:0:10}: $why"
}

ping_hc() {   # ping_hc "" msg  -> success;  ping_hc fail msg -> failure
    [[ -z "${DEPLOY_HEALTHCHECK_URL:-}" ]] && return 0
    local url=$DEPLOY_HEALTHCHECK_URL
    [[ -n "$1" ]] && url="${url%/}/$1"
    curl -fsS -m 10 --retry 3 --data-raw "$2" "$url" >/dev/null || true
}

# On one line: bash reads the whole line, exit included, before main can replace this file.
main "$@"; exit $?
