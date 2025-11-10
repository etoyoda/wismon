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
ruby ${base}/topicstat-ctab.rb topics${ymd}.txt 2> ctab.log > ctab${ymd}.txt
ruby ${base}/wnm-topicstat.rb jmagc -gc=jp-jma-global-cache 2> topicsj.log \
 > topicsj${ymd}.txt
ruby ${base}/topicstat-ctab.rb topicsj${ymd}.txt 2> ctabj.log > ctabj${ymd}.txt
ruby ${base}/wnm-topicstat.rb devgc 2> topicsd.log > topicsd${ymd}.txt
ruby ${base}/topicstat-ctab.rb topicsd${ymd}.txt 2> ctabd.log > ctabd${ymd}.txt

cd /nwp/m1

prev=''
if test -f gtshist-jmagc.txt
then prev='-prev=gtshist-jmagc.txt'
fi
ruby ${base}/wnm-gtshist.rb jmagc $prev > z.gtsj.txt
ln -f gtshist-jmagc.txt gtshist-jmagc-prev.txt
mv -f z.gtsj.txt gtshist-jmagc.txt

prev=''
if test -f gtshist-devgc.txt
then prev='-prev=gtshist-devgc.txt'
fi
ruby ${base}/wnm-gtshist.rb devgc $prev > z.gtsj.txt
ln -f gtshist-devgc.txt gtshist-devgc-prev.txt
mv -f z.gtsj.txt gtshist-devgc.txt

time ruby ${base}/wnm-convobs.rb > z.convobs.txt 2> convobs.log
ln -f convobs.txt convobs-prev.txt
mv -f z.convobs.txt convobs.txt

exit 0
