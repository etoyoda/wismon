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

cd /nwp/m1

prev=''
if test -f gtshist-jmagc.txt
then prev='-prev=gtshist-jmagc.txt'
fi
ruby ${base}/wnm-gtshist.rb jmagc $prev > z.gtsj.txt
ln -f gtshist-jmagc.txt gtshist-jmagc-prev.txt
mv -f z.gtsj.txt gtshist-jmagc.txt

rm -f convobs-cur.txt convobs.log
ruby ${base}/wnm-convobs.rb > convobs-cur.txt 2> convobs.log
if [ -f convobs.txt ]; then
  ln -f convobs.txt convobs-prev.txt
else
  touch convobs.txt
fi
rm -f z.convobs2.txt
ruby ${base}/convobs-merge.rb convobs.txt convobs-cur.txt > z.convobs2.txt
mv -f z.convobs2.txt convobs.txt

cd /nwp/m1
CONVOBS=convobs-cur.txt

##--- BEGIN DRAWING
if test -x /usr/bin/gmt
then
  YPS=y.convobs.ps
  YTXT=ytmp.txt
  YTXT2=ytmp2.txt
  gmt set \
  MAP_FRAME_TYPE plain \
  MAP_TICK_LENGTH_PRIMARY 0.1c \
  FONT_ANNOT_PRIMARY 6p
  REGION="-R-180/180/-90/90"
  PROJ="-JQ0/5i"
  # DATE=$(awk '{print $2}' $CONVOBS | sort -r | head -10 | tail -1)
  DATE=$(TZ=UTC+0 date +%Y%m%dT0000 --date '24 hours ago')
  rm -f $YPS $YTXT $YTXT2 
  : map 1 - SYNOP and TEMP
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /synop/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '(!/DROP/ && $2 >= "'${DATE}'" && $8 ~ /temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT2
  gmt psxy $REGION $PROJ -St3p -W0.5p,blue -O -K < $YTXT2 >> $YPS
  awk '(/DROP/ && $2 >= "'${DATE}'" && $8 ~ /temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT2
  gmt psxy $REGION $PROJ -Si3p -W0.5p,cyan -O -K < $YTXT2 >> $YPS
  gmt pslegend $REGION $PROJ -Dg-170/-34+w1.0i+jTL+o0.1i -F+gwhite+p0.25p+r2p -O >> $YPS <<ENDLEGEND
H 6p,Helvetica-Bold WIS2 Data Coverage
G 0p
H 6p,Helvetica-Bold ${DATE}Z/PT24
G 1p
S 0.05i c 2p orange 0.25p 0.1i SYNOP
G -6.5p
S 0.50i t 3p - 0.5p,blue 0.55i TEMP
ENDLEGEND
  gmt psconvert $YPS -A+m0.2c -Tg -P
  rm -f $YPS $YTXT $YTXT2
  mv -f y.convobs.png convobs.png
  : map 2 - SHIP and BUOY
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /\/ship/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /\/wind-profile/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT
  gmt psxy $REGION $PROJ -Ss3p -Gyellow -W0.25p,red -O -K < $YTXT >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /drifting-buoy/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT2
  gmt psxy $REGION $PROJ -Sd3p -W0.4p,cyan -O -K < $YTXT2 >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /moored-buoy/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT2
  gmt psxy $REGION $PROJ -Si3p -W0.4p,blue -O -K < $YTXT2 >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /wave-buoy/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVOBS > $YTXT2
  gmt psxy $REGION $PROJ -Sa3p -W0.4p,blue -O -K < $YTXT2 >> $YPS
  gmt pslegend $REGION $PROJ -Dg-180/-65+w2.6i+jTL+o0.1i -F+gwhite+p0.25p+r3p -O >> $YPS <<ENDLEGEND
H 6p,Helvetica-Bold WIS2 Data Coverage ${DATE}Z/PT24
G 0p
S 0.05i c 2p orange 0.20p 0.1i SHIP
G -6.5p
S 0.45i d 3p - 0.20p,cyan 0.5i DRIFT
G -6.5p
S 0.85i i 3p - 0.20p,blue 0.9i MOORED
G -6.5p
S 1.45i a 3p - 0.20p,blue 1.5i WAVE
G -6.5p
S 1.85i s 3p yellow 0.20p,red 1.9i WPROF
ENDLEGEND
  gmt psconvert $YPS -A+m0.2c -Tg -P
  rm -f $YPS $YTXT $YTXT2 gmt.conf gmt.history
  mv -f y.convobs.png convobs2.png
fi
##--- END DRAWING

if [ -f /nwp/bin/send_png_mail.rb ] && [ -f convobs.png ]; then
  ruby /nwp/bin/send_png_mail.rb convobs.png
else
  user=$(whoami)
  {
    echo "From: $user"
    echo "To: $user"
    echo "Subject: run-topicstat.sh $ymd"
    echo ""
    echo "wismon updated topic statistics for $ymd."
    echo "https://toyoda-eizi.net/nwp/m1"
  } | /usr/sbin/sendmail -t
fi

exit 0
