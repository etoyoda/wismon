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

cd /nwp/m1
CONVOBS=convobs.txt

if test -x /usr/bin/gmt
then
  YPS=y.convobs.ps
  YTXT=ytmp.txt
  gmt set \
  MAP_FRAME_TYPE plain \
  MAP_TICK_LENGTH_PRIMARY 0.1c \
  FONT_ANNOT_PRIMARY 6p
  REGION="-R-180/180/-90/90"
  PROJ="-JQ0/5i"
  # DATE=$(awk '{print $2}' $CONVOBS | sort -r | head -10 | tail -1)
  DATE=$(TZ=UTC+0 date +%Y%m%dT0000 --date '24 hours ago')
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $7 ~ /synop/){print $6, $5}' $CONVOBS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '($2 >= "'${DATE}'" && $7 ~ /temp/){print $6, $5}' $CONVOBS > $YTXT
  gmt psxy $REGION $PROJ -Sx3p -W0.5p,blue -O -K < $YTXT >> $YPS
  gmt pslegend $REGION $PROJ -Dg-180/-45+w1.1i+jTL+o0.1i -F+gwhite+p0.25p+r3p -O >> $YPS <<ENDLEGEND
H 6p,Helvetica-Bold black WIS2 Data Coverage
G 0p
H 6p,Helvetica-Bold black ${DATE}Z/PT24
G 1p
S 0.05i c 2p orange 0.25p 0.2i SYNOP
S 0.05i x 3p blue 0.5p 0.2i TEMP
ENDLEGEND
  gmt psconvert $YPS -A+m0.2c -Tg -P
  rm -f $YPS $YTXT gmt.conf gmt.history
  mv -f y.convobs.png convobs.png
fi


exit 0
