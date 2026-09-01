#!/usr/bin/bash
set -Eeuo pipefail
PATH=/bin:/usr/bin:/usr/local/bin
TZ=UTC
export TZ
: ${bindir:=$(dirname $0)}
if [ X"$bindir" = X"." ]; then
  bindir=$(pwd)
fi
: ${nwp:=${HOME}/nwp-test}
: ${base:=${nwp}/m2}
: ${refhour:=$(date +'%Y-%m-%dT%HZ')}

test -d ${base} || mkdir ${base}
cd ${base}

basetime=$(ruby -rtime -e 'puts(Time.at(((Time.parse(ARGV.first.sub(/Z/,":00:00Z")).to_i-600)/86400-1)*86400).utc.strftime("%Y-%m-%dT%H:%M:%SZ"))' $refhour)
ymd=$(ruby -rtime -e 'puts(Time.parse(ARGV.first).utc.strftime("%Y-%m-%d"))' $basetime)
export ymd
prevday=$(ruby -rtime -e 'puts(Time.at(Time.parse(ARGV.first).to_i-86400).utc.strftime("%Y-%m-%dT%H:%M:%SZ"))' $basetime)
ymdp=$(ruby -rtime -e 'puts(Time.parse(ARGV.first).utc.strftime("%Y-%m-%d"))' $prevday)
export ymdp

gtsbf=${nwp}/p0/${ymd}/obsbf-${ymd}.tar
if test ! -f ${gtsbf} ; then
  echo missing ${gtsbf}
  exit 16
fi

test ! -f z.txt || rm -f z.txt

filt=--topic='gts-I(SI|SM|SN|UD|UJ|UK|UP|US|UW)'
time ruby ${bindir}/convobs-stnlist.rb $filt ${gtsbf} > convgts-${ymd}.txt 2> loggts-${ymd}.txt
if test -f convgts.txt; then
  ruby ${bindir}/convobs-merge.rb convgts-${ymd}.txt convgts.txt > z.txt
  mv -f z.txt convgts.txt
else
  cp -f convgts-${ymd}.txt convgts.txt
fi

wisbf=${nwp}/p0/${ymd}/wisbf-${ymd}.tar
if test ! -f ${wisbf} ; then
  echo missing ${wisbf}
  exit 16
fi
wistm=${nwp}/p0/${ymd}/wistm-${ymd}.tar
if test -f ${wistm} ; then
  wisbf="${wisbf} ${wistm}"
fi

time ruby ${bindir}/convobs-stnlist.rb ${wisbf} > convwis-${ymd}.txt 2> logwis-${ymd}.txt
if test -f convwis.txt; then
  ruby ${bindir}/convobs-merge.rb convwis-${ymd}.txt convwis.txt > z.txt
  mv -f z.txt convwis.txt
else
  cp -f convwis-${ymd}.txt convwis.txt
fi

if test -x ${bindir}/act-m2pics.sh ; then
  bash ${bindir}/act-m2pics.sh
fi
