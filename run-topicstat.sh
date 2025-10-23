#!/bin/bash
set -e
PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin
hash -r
base=`dirname $0`
cd /nwp/m1
export LANG=C
export TZ=UTC
set - `date --date 'now - 1 hour' +'%Y-%m %Y%m%d'`
ym=$1
ymd=$2
test -d $ym || mkdir $ym
cd $ym
ruby ${base}/wnm-topicstat.rb 2> topics.log > topics${ymd}.txt
exit 0
