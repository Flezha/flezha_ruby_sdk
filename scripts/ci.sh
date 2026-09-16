#!/usr/bin/env bash
# The three tiers this repository runs, and no controller anywhere.
#
#   unit        the gem's own suite, including the fixture checks that read the
#               contract set
#   local mode  the declared graph run in process, over the five capabilities
#               the language layer owns
#   smoke       the fake controller on a socket directory, this repository's own
#               test app launched against it, and the binary's exit code
#
# The fake controller and the contract set are fetched from one core release
# and checked against that release's manifest. The native library links the
# Ruby it runs in, so it is built here from the wrapper crate, which takes the
# core from its git tag: this needs cargo and git, and no checkout of the core.
#
#   scripts/ci.sh --core <dir|url> [--version 0.1.0]
#
# What this cannot see is the fifteen-capability vocabulary the controller
# owns: persistence, effectively-once, correction, partitioning, workflows,
# liveness, restarts. Those are end-to-end scenarios and they run in the core's
# own tree, against the commit it pins this repository at.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CORE=""
VERSION="0.1.0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --core)    CORE="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
[[ -n "${CORE}" ]] || { echo "--core <dir|url> is required" >&2; exit 1; }

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# Every dependency at the requirement the gemspec states, so the pins live in
# one file.
step "installing"
ruby -e 'Gem::Specification.load("flezha.gemspec").dependencies.each { |d| puts "#{d.name}\t#{d.requirement}" }' \
  | while IFS=$'\t' read -r name requirement; do
      gem install --conservative --no-document "${name}" -v "${requirement}"
    done

# A debug build: the tiers measure behaviour, and it builds in a fraction of
# the time.
step "building the extension"
RB_SYS_CARGO_PROFILE=dev rake compile

step "fetching the core ${VERSION}"
scripts/fetch-core.sh --from "${CORE}" --version "${VERSION}"

# The local-mode tests drive the fixture classes `test_app/` declares, so both
# tiers are one run of the suite.
step "unit and local mode"
rake test

step "smoke"
# The runner reads the same socket directory the fake controller binds in, and
# the connect fixture is what pins the variable's name.
SOCKETS="$(mktemp -d)"
INPUTS="${SOCKETS}/inputs.txt"
printf 'alpha\nbeta\n' > "${INPUTS}"
export FLEZHA_SOCKET_FILES_PATH="${SOCKETS}"
export FLEZHA_TRANSPORT=uds

# The runner ends at end of stream on its standard input, so it is held open
# for as long as the tier runs and closed when it is over.
mkfifo "${SOCKETS}/stdin"
sleep 120 > "${SOCKETS}/stdin" &
HOLD=$!
ruby test_app/runner.rb smoke > "${SOCKETS}/runner.log" 2>&1 < "${SOCKETS}/stdin" &
RUNNER=$!

set +e
.core/flz-fake-controller \
  --socket-dir "${SOCKETS}" \
  --factory do_nothing --operative echo-1 --channel input \
  --inputs "${INPUTS}" --expect-outputs 2 --timeout-ms 60000
CODE=$?
set -e

kill "${HOLD}" "${RUNNER}" 2>/dev/null || true
if (( CODE != 0 )); then
  echo "the runner's own log:"
  sed 's/^/  /' "${SOCKETS}/runner.log"
fi
rm -rf "${SOCKETS}"

# The tier's verdict is the binary's, never the runner's: a terminal
# disconnect ends the runner non-zero on purpose, so the orchestrator that owns
# it restarts it.
exit "${CODE}"
