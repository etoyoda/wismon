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

if [[ X"$ymd" = X"" ]]; then
  basetime=$(ruby -rtime -e 'puts(Time.at(((Time.parse(ARGV.first.sub(/Z/,":00:00Z")).to_i)/86400-1)*86400).utc.strftime("%Y-%m-%dT%H:%M:%SZ"))' $refhour)
  ymd=$(ruby -rtime -e 'puts(Time.parse(ARGV.first).utc.strftime("%Y-%m-%d"))' $basetime)
fi
export ymd

test ! -f z.txt || rm -f z.txt

gwjp=${nwp}/p0/${ymd}/gwjp-${ymd}.tar
time ruby ${bindir}/convobs-stnlist.rb --topic=. ${gwjp} > convgwjp-${ymd}.txt 2> loggwjp-${ymd}.txt
if test -f convgwjp.txt ; then
  ruby ${bindir}/convobs-merge.rb convgwjp-${ymd}.txt convgwjp.txt > z.txt
  mv -f z.txt convgwjp.txt
else
  cp -f convgwjp-${ymd}.txt convgwjp.txt
fi

gwde=${nwp}/p0/${ymd}/gwde-${ymd}.tar
time ruby ${bindir}/convobs-stnlist.rb --topic=. ${gwde} > convgwde-${ymd}.txt 2> loggwde-${ymd}.txt
if test -f convgwde.txt ; then
  ruby ${bindir}/convobs-merge.rb convgwde-${ymd}.txt convgwde.txt > z.txt
  mv -f z.txt convgwde.txt
else
  cp -f convgwde-${ymd}.txt convgwde.txt
fi

ruby ${bindir}/convobs-diff.rb --unify convgwjp-${ymd}.txt convgts-${ymd}.txt > gwjpgts-${ymd}.txt
ruby ${bindir}/convobs-diff.rb --unify convgwjp-${ymd}.txt convwis-${ymd}.txt > gwjpwis-${ymd}.txt
ruby ${bindir}/convobs-diff.rb --unify convgwde-${ymd}.txt convgts-${ymd}.txt > gwdegts-${ymd}.txt
ruby ${bindir}/convobs-diff.rb --unify convgwde-${ymd}.txt convwis-${ymd}.txt > gwdewis-${ymd}.txt
ruby ${bindir}/convobs-diff.rb --unify convgwde-${ymd}.txt convgwjp-${ymd}.txt > gwdegwjp-${ymd}.txt
ruby ${bindir}/convobs-diff.rb --unify convgwjp-${ymd}.txt convgwde-${ymd}.txt > gwjpgwde-${ymd}.txt
ruby ${bindir}/convobs-diff.rb --unify convgts-${ymd}.txt convgwjp-${ymd}.txt > gtsgwjp-${ymd}.txt
