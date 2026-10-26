#!/bin/sh
# The gate (docs/development/workflow.md, section 1): formatting, lints as
# errors, then both test suites in full, the focused one, then the fuzzy
# one. It stays deterministic: none of its checks runs an agent
# (workflow.md, section 5).
set -eu

cd "$(dirname "$0")/.."

# Each step's time, for the landing flow's reports (agents'
# docs/design/landing-flow.md, 10): `gate-step NAME SECONDS EXIT` on stderr.
step() {
    step_name=$1
    shift
    step_start=$(date +%s%N)
    step_exit=0
    "$@" || step_exit=$?
    step_ms=$((($(date +%s%N) - step_start) / 1000000))
    printf 'gate-step %s %d.%03d %d\n' "$step_name" $((step_ms / 1000)) $((step_ms % 1000)) "$step_exit" >&2
    return "$step_exit"
}

# skein's programming-model.md, 2: every binary installs the fail-stop hook first.
step fail-stop python3 -I -B scripts/fail_stop.py
step fmt cargo fmt --all --check
step clippy cargo clippy --workspace --all-targets -- -D warnings
step wasm cargo clippy --target wasm32-unknown-unknown -p smith-agent-shell -- -D warnings
export NEXTEST_DOUBLE_SPAWN=0
step build cargo nextest run --workspace --no-run
step focused cargo nextest run --workspace
step fuzzy cargo nextest run --workspace --profile fuzzy
