#!/bin/sh
set -eu

# Base-image preparation for the SlateDB workload harness.
#
# The system under test is the vendored `slatedb-driver` binary (built from
# .workers/driver/, x86_64-unknown-linux-musl, static-pie — depends on slatedb
# by path with default-features off). No toolchain is required in the image:
# workloads are python3 scripts that drive the vendored binary. Everything is
# offline — the binary travels in git. To rebuild it:
#   rustup target add x86_64-unknown-linux-musl
#   cd .workers/driver && cargo build --release --target x86_64-unknown-linux-musl
#   cp target/x86_64-unknown-linux-musl/release/slatedb-driver \
#      .workers/vendor/bin/slatedb-driver

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DRIVER_BIN="${ROOT}/.workers/vendor/bin/slatedb-driver"

if [ ! -f "${DRIVER_BIN}" ]; then
  echo "missing vendored slatedb-driver at ${DRIVER_BIN}" >&2
  echo "build it: cd .workers/driver && cargo build --release --target x86_64-unknown-linux-musl" >&2
  exit 1
fi
chmod +x "${DRIVER_BIN}"

# Smoke the binary so a broken vendor is caught at image-prep, not mid-run.
"${DRIVER_BIN}" --help >/dev/null

echo "build.sh: slatedb-driver staged at .workers/vendor/bin/slatedb-driver"
