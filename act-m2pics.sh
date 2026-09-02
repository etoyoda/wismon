#!/bin/bash

# act-m2pics.sh
#
# Conventional Observations の観測地点一覧から GMT を用いて
# 全球カバレッジ図を作成する。
#
# 入力:
#   convgts-YYYYMMDD.txt
#   convwis-YYYYMMDD.txt
#   onlygts-YYYYMMDD.txt
#   onlywis-YYYYMMDD.txt
#
# 出力:
#   convgts.png
#       GTS の SYNOP/TEMP カバレッジ
#
#   convwis.png
#       WIS2 の SYNOP/TEMP カバレッジ
#
#   convwis2.png
#       WIS2 の SHIP/BUOY/WIND PROFILER カバレッジ
#
#   onlygts.png
#       GTS に存在し WIS2 に存在しない観測点
#
#   onlywis.png
#       WIS2 に存在し GTS に存在しない観測点
#
# 直近24時間に報告された Conventional Observations を対象とし、
# GTS と WIS2 の観測データ流通状況およびカバレッジ差分を
# 地理的に可視化する。

set -e
PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin
hash -r
cd /nwp/m2
export LANG=C
export TZ=UTC

: ${ymd:?}
: ${ymdp:?}

CONVWIS=convwis-${ymd}.txt
CONVGTS=convgts-${ymd}.txt
ONLYGTS=onlygts-${ymd}.txt
ONLYWIS=onlywis-${ymd}.txt
DATE=${ymdp}T00

if test ! -f $CONVWIS ; then
  echo missing $CONVWIS
  exit 16
fi

if test ! -f $CONVGTS ; then
  echo missing $CONVGTS
  exit 16
fi

##--- BEGIN DRAWING
if test -x /usr/bin/gmt
then
  YPS=y.convwis.ps
  YPNG=y.convwis.png
  YTXT=ytmp.txt
  YTXT2=ytmp2.txt
  gmt set \
  MAP_FRAME_TYPE plain \
  MAP_TICK_LENGTH_PRIMARY 0.1c \
  FONT_ANNOT_PRIMARY 6p
  REGION="-R-180/180/-90/90"
  PROJ="-JQ0/5i"
  : ${DATE:?}
  rm -f $YPS $YTXT $YTXT2 

  : map 0 - SYNOP and TEMP /GTS/
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /gts-IS[IMN]/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVGTS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '(!/DROP/ && $2 >= "'${DATE}'" && $8 ~ /gts-IU[PSK]/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVGTS > $YTXT2
  gmt psxy $REGION $PROJ -St3p -W0.5p,blue -O -K < $YTXT2 >> $YPS
  awk '(/DROP/ && $2 >= "'${DATE}'" && $8 ~ /gts-IUD/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVGTS > $YTXT2
  gmt psxy $REGION $PROJ -Si3p -W0.5p,cyan -O -K < $YTXT2 >> $YPS
  gmt pslegend $REGION $PROJ -Dg-170/-34+w1.0i+jTL+o0.1i -F+gwhite+p0.25p+r2p -O >> $YPS <<ENDLEGEND
H 6p,Helvetica-Bold GTS Data Coverage
G 0p
H 6p,Helvetica-Bold ${DATE}Z/PT24
G 1p
S 0.05i c 2p orange 0.25p 0.1i SYNOP
G -6.5p
S 0.50i t 3p - 0.5p,blue 0.55i TEMP
ENDLEGEND
  gmt psconvert $YPS -A+m0.2c -Tg -P
  rm -f $YPS $YTXT $YTXT2
  mv -f $YPNG convgts.png

  : map 1 - SYNOP and TEMP
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /synop/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '(!/DROP/ && $2 >= "'${DATE}'" && $8 ~ /temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT2
  gmt psxy $REGION $PROJ -St3p -W0.5p,blue -O -K < $YTXT2 >> $YPS
  awk '(/DROP/ && $2 >= "'${DATE}'" && $8 ~ /temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT2
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
  mv -f $YPNG convwis.png

  : map 2 - SHIP and BUOY
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /\/ship/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /\/wind-profile/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT
  gmt psxy $REGION $PROJ -Ss3p -Gyellow -W0.25p,red -O -K < $YTXT >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /drifting-buoy/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT2
  gmt psxy $REGION $PROJ -Sd3p -W0.4p,cyan -O -K < $YTXT2 >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /moored-buoy/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT2
  gmt psxy $REGION $PROJ -Si3p -W0.4p,blue -O -K < $YTXT2 >> $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /wave-buoy/ && $7==$7+0 && $6==$6+0){print $7, $6}' $CONVWIS > $YTXT2
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
  rm -f $YPS $YTXT $YTXT2
  mv -f $YPNG convwis2.png

  : map 3 - ONLY GTS
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /gts-IS[MNI]/ && $7==$7+0 && $6==$6+0){print $7, $6}' $ONLYGTS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '(!/DROP/ && $2 >= "'${DATE}'" && $8 ~ /gts-IU[PSK]/ && $7==$7+0 && $6==$6+0){print $7, $6}' $ONLYGTS > $YTXT2
  gmt psxy $REGION $PROJ -St3p -W0.5p,blue -O -K < $YTXT2 >> $YPS
  awk '(/DROP/ && $2 >= "'${DATE}'" && $8 ~ /gts-IUD/ && $7==$7+0 && $6==$6+0){print $7, $6}' $ONLYGTS > $YTXT2
  gmt psxy $REGION $PROJ -Si3p -W0.5p,cyan -O -K < $YTXT2 >> $YPS
  gmt pslegend $REGION $PROJ -Dg-170/-34+w1.0i+jTL+o0.1i -F+gwhite+p0.25p+r2p -O >> $YPS <<ENDLEGEND
H 6p,Helvetica-Bold GTS-WIS2 Diff Coverage
G 0p
H 6p,Helvetica-Bold ${DATE}Z/PT24
G 1p
S 0.05i c 2p orange 0.25p 0.1i SYNOP
G -6.5p
S 0.50i t 3p - 0.5p,blue 0.55i TEMP
ENDLEGEND
  gmt psconvert $YPS -A+m0.2c -Tg -P
  mv -f $YPNG onlygts.png
  rm -f $YPS $YTXT $YTXT2

  : map 4 - ONLY WIS
  gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
  awk '($2 >= "'${DATE}'" && $8 ~ /synop/ && $7==$7+0 && $6==$6+0){print $7, $6}' $ONLYWIS > $YTXT
  gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
  awk '(!/DROP/ && $2 >= "'${DATE}'" && $8 ~ /temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $ONLYWIS > $YTXT2
  gmt psxy $REGION $PROJ -St3p -W0.5p,blue -O -K < $YTXT2 >> $YPS
  awk '(/DROP/ && $2 >= "'${DATE}'" && $8 ~ /temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $ONLYWIS > $YTXT2
  gmt psxy $REGION $PROJ -Si3p -W0.5p,cyan -O -K < $YTXT2 >> $YPS
  gmt pslegend $REGION $PROJ -Dg-170/-34+w1.0i+jTL+o0.1i -F+gwhite+p0.25p+r2p -O >> $YPS <<ENDLEGEND
H 6p,Helvetica-Bold WIS2-GTS Diff Coverage
G 0p
H 6p,Helvetica-Bold ${DATE}Z/PT24
G 1p
S 0.05i c 2p orange 0.25p 0.1i SYNOP
G -6.5p
S 0.50i t 3p - 0.5p,blue 0.55i TEMP
ENDLEGEND
  gmt psconvert $YPS -A+m0.2c -Tg -P
  mv -f $YPNG onlywis.png
  rm -f $YPS $YTXT $YTXT2

  rm -f gmt.conf gmt.history
fi
##--- END DRAWING
