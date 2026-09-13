#!/bin/bash
PATH=/bin:/usr/bin:/usr/local/bin
set -Eeuo pipefail
shopt -s nullglob

set $(date --date '1 day ago' +'%Y-%m')
thismon=$1
set $(date --date '28 day ago' +'%Y-%m')
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

cd /nwp/m2
for tar in wismon2-*.tar
do
  # if a tarball for a past month exists and that is not found in a2 directory
  if [[ "wismon2-${thismon}.tar" > "${tar}" && \
	  ! -f "/nwp/a2/${tar}.gz" ]]; then
    cp -f ${tar} /nwp/a2/${tar}
    gzip -f -9 /nwp/a2/${tar}
  fi
  # if a tarball before last month exists and that is found in a2 directory
  if [[ "wismon2-${prevmon}.tar" > "${tar}" && \
	  -f "/nwp/a2/${tar}.gz" ]]; then
    rm -f ${tar}
  fi
done
find . -ctime +31 -a \( -name '*-*.txt' -o -name '*-*.png' \) -delete
