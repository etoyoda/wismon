#!/bin/bash

set -e
PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin
hash -r
export LANG=C
export TZ=UTC

LHS=$1
shift
RHS=$1

if test ! -f "$LHS" ; then echo missing lhs="$LHS"; exit 16; fi
if test ! -f "$RHS" ; then echo missing lhs="$RHS"; exit 16; fi

YPS=y.convdiff.ps
YPNG=y.convdiff.png
YTXT1=y.diff.txt
YTXT=y.synop.txt
YTXT2=y.temp.txt
YTXT3=y.tempdrop.txt

test ! -f $YTXT1 || rm -f $YTXT1
convobs-diff.rb "$LHS" "$RHS" > $YTXT1
wc -l $YTXT1

gmt set \
  MAP_FRAME_TYPE plain \
  MAP_TICK_LENGTH_PRIMARY 0.1c \
  FONT_ANNOT_PRIMARY 6p
  REGION="-R-180/180/-90/90"
  PROJ="-JQ0/5i"
rm -f $YPS $YTXT $YTXT2 $YTXT3

gmt pscoast $REGION $PROJ -B30g30 -Dc -A5000 -W0.25p -N1/0.25p -P -K > $YPS
awk '($8 ~ /gts-IS[IMN]|synop/ && $7==$7+0 && $6==$6+0){print $7, $6}' $YTXT1 > $YTXT
wc -l $YTXT
gmt psxy $REGION $PROJ -Sc2p -Gorange -W0.25p -O -K < $YTXT >> $YPS
awk '(!/DROP/ && $8 ~ /gts-IU[PSK]|temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $YTXT1 > $YTXT2
wc -l $YTXT2
gmt psxy $REGION $PROJ -St3p -W0.5p,blue -O -K < $YTXT2 >> $YPS
awk '(/DROP/ && $8 ~ /gts-IUD|temp/ && $7==$7+0 && $6==$6+0){print $7, $6}' $YTXT1 > $YTXT3
wc -l $YTXT3
gmt psxy $REGION $PROJ -Si3p -W0.5p,cyan -O -K < $YTXT3 >> $YPS
gmt pslegend $REGION $PROJ -Dg-170/-34+w1.0i+jTL+o0.1i -F+gwhite+p0.25p+r2p -O >> $YPS <<ENDLEGEND
H 6p,Helvetica-Bold $(basename ${LHS} .txt)
G 0p
H 6p,Helvetica-Bold $(basename ${RHS} .txt)
G 1p
S 0.05i c 2p orange 0.25p 0.1i SYNOP
G -6.5p
S 0.50i t 3p - 0.5p,blue 0.55i TEMP
ENDLEGEND
gmt psconvert $YPS -A+m0.2c -Tg -P
rm -f $YPS $YTXT $YTXT2 $YTXT3
rm -f $YTXT1
ls -l $YPNG 
echo ok $YPNG 
