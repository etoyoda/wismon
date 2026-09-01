#!/bin/bash
PATH=/bin:/usr/bin:/usr/local/bin
set -Eeuo pipefail

set $(date --date '1 day ago' +'%Y-%m')
thismon=$1
set $(date --date '31 day ago' +'%Y-%m')
prevmon=$1

cd /nwp/m1
for dir in 2*-*[0-9]
do
  if [[ "${thismon}" > "${dir}" && ! -f "${dir}.tar.gz" ]] ; then
    tar czf "${dir}.tar.gz" ${dir}
  fi
  if [[ "${prevmon}" > "${dir}" && -f "${dir}.tar.gz" ]] ; then
    rm -rf ${dir}
  fi
done
