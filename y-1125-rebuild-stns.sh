#!/bin/bash
set -xeEuo pipefail
shopt -s nullglob

merge=../convobs-merge.rb

: > convwis.txt
: > convgts.txt

for tgz in /nwp/a2/2???-??/wismon2-2???-??.tar.gz
do
  : tgz=$tgz
  tar -xf $tgz --wildcards 'convwis-*.txt' 'convgts-*.txt' || true
  for line in wis gts
  do
    txts=(conv${line}-*.txt)
    if (( ${#txts[@]} > 0 )) ; then
      ruby $merge conv${line}.txt "${txts[@]}" > updated.txt
      mv -f updated.txt conv${line}.txt
      rm -f "${txts[@]}"
    fi
  done
done
