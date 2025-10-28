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
ruby ${base}/wnm-topicstat.rb jmagc 2> topics.log > topics${ymd}.txt
ruby ${base}/wnm-topicstat.rb jmagc -gc=jp-jma-global-cache 2> topicsj.log \
 > topicsj${ymd}.txt
ruby ${base}/topicstat-ctab.rb topics$ymd.txt 2> ctab.log > ctab$ymd.txt
ruby ${base}/topicstat-ctab.rb topicsj$ymd.txt 2> ctabj.log > ctabj$ymd.txt
exit 0
