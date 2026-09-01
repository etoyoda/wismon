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
  archname=/nwp/a1/${dir}/wismon-${dir}.tar.gz
  if [[ "${thismon}" > "${dir}" && ! -f "${archname}" ]] ; then
    [[ -d /nwp/a1/${dir} ]] || mkdir /nwp/a1/${dir}
    tar czf "${archname}" ${dir}
  fi
  if [[ "${prevmon}" > "${dir}" && -f "${archname}" ]] ; then
    rm -rf ${dir}
  fi
done
