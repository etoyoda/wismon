#!/bin/bash
set -eEuo pipefail
# bourne shell rejected by -e and unknown pipefail
set +e
nng=0

assert_f() {
  fnam="$1"
  if [[ -f "$fnam" ]]; then
    echo "OK $fnam present"
  else
    echo "NG $fnam missing"
    nng=$(( $nng + 1 ))
  fi
}

assert_nf() {
  fnam="$1"
  if [[ -f "$fnam" ]]; then
    echo "NG $fnam present"
    nng=$(( $nng + 1 ))
  else
    echo "OK $fnam missing"
  fi
}

assert_f /nwp/m2/wismon2-2026-10.tar
assert_nf /nwp/m2/wismon2-2026-09.tar
assert_f /nwp/m2/wismon2-2026-09.tar.gz
assert_f /nwp/a2/wismon2-2026-09.tar.gz
echo Found $nng errors
