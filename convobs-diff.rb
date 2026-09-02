#!/usr/bin/ruby

# convobs-diff.rb
#
# 観測地点統計ファイルの差分抽出ツール。
#
# 最初に指定したファイルを基準集合とし、続いて指定した
# ファイル群に含まれる WSI を順次除外する。
#
# 入力は convobs.txt, convwis.txt, convgts.txt 等の
# 観測地点一覧ファイルを想定する。
#
# 典型的には、
#
#   convobs-diff.rb convgts.txt convwis.txt
#
# により、
#
#   GTS に存在するが WIS2 には存在しない観測地点
#
# を抽出できる。
#
# 逆に、
#
#   convobs-diff.rb convwis.txt convgts.txt
#
# とすると、
#
#   WIS2 に存在するが GTS には存在しない観測地点
#
# を抽出できる。
#
# 出力は基準ファイル中の元の行を保持したまま、
# 差分として残った観測地点のみを出力する。

db=Hash.new
fnam=ARGV.shift
File.open(fnam,'r:UTF-8'){|fp|
  fp.each{|line|
    wsi=line.split(/\t/,2).first
    db[wsi]=line
  }
}
ARGV.each{|fnam|
  File.open(fnam,'r:UTF-8'){|fp|
    fp.each{|line|
      wsi=line.split(/\t/,2).first
      db.delete(wsi)
    }
  }
}
db.keys.sort.each{|wsi|
  puts db[wsi]
}
