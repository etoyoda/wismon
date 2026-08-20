# run-topicstat.sh - wismon日次メンテナンス・集計バッチ

## 概要

wismon パッケージにおける唯一の cron ジョブ。

当初は Topic 統計を日次更新するためのスクリプトであったが、現在は WNM アーカイブ解析、GTS履歴更新、観測データ集計、監視図作成、通知メール送信など、日次バッチ処理全体の起動スクリプトとなっている。

通常は cron から1日1回実行する。

## 主な処理

実行される処理は大きく分けて以下の5つである。

1. Topic統計生成
2. カテゴリ別統計生成
3. GTS流通履歴更新
4. 観測データ履歴更新
5. カバレッジ図作成・通知

## 入力

主に以下を利用する。

- `wnmsaver.rb` が生成した WNM tar.gz ファイル
- 過去の統計ファイル
- 過去の GTS 履歴ファイル
- 過去の観測履歴ファイル

## 出力

- Topic統計
- カテゴリ別統計
- GTS履歴
- 観測履歴
- カバレッジ図 PNG
- メール通知

---

## 1. Topic統計生成

### 実行スクリプト

```text
wnm-topicstat.rb
topicstat-ctab.rb
```

出力
Plain Text
1
topicsYYYYMMDD.txt
2
ctabYYYYMMDD.txt
その他の行を表示する
内容

過去24時間の WNM を解析し、

Topic別流通件数
平均データサイズ

を集計する。

さらに Topic をカテゴリ分類した集計表を生成する。

日本GC限定統計
Plain Text
1
topicsjYYYYMMDD.txt
2
ctabjYYYYMMDD.txt
その他の行を表示する

も同時生成する。

これは

Plain Text
1
global-cache = jp-jma-global-cache
その他の行を表示する

のみを対象とした集計である。

2. GTS流通履歴更新
実行スクリプト
Plain Text
1
wnm-gtshist.rb
その他の行を表示する
出力
Plain Text
1
gtshist-jmagc.txt
2
``
その他の行を表示する
内容

GTS-to-WIS2 メッセージの流通履歴を更新する。

更新前ファイルは

Plain Text
1
gtshist-jmagc-prev.txt
その他の行を表示する

として保存する。

3. 観測データ履歴更新
実行スクリプト
Plain Text
1
wnm-convobs.rb
2
convobs-merge.rb
その他の行を表示する
出力
Plain Text
1
convobs.txt
その他の行を表示する
内容

観測データの出現履歴を収集する。

収集結果を過去履歴へマージし、長期履歴を維持する。

更新前ファイルは

Plain Text
1
convobs-prev.txt
その他の行を表示する

として保存する。

4. 観測カバレッジ図作成

GMT が利用可能な場合に実行する。

Plain Text
1
/usr/bin/gmt
その他の行を表示する
出力
Plain Text
1
convobs.png
2
convobs2.png
その他の行を表示する
convobs.png

対象:

SYNOP
TEMP

直近24時間に観測された地点を世界地図上へ描画する。

記号:

橙丸: SYNOP
青三角: TEMP
水色逆三角: DROP
convobs2.png

対象:

SHIP
DRIFTING BUOY
MOORED BUOY
WAVE BUOY
WIND PROFILER

直近24時間の観測点分布を表示する。

目的

Global Cache 経由で実際に流通している観測データの地理的カバレッジを視覚的に確認する。

5. 通知メール送信
優先方法
Plain Text
1
/nwp/bin/send_png_mail.rb
その他の行を表示する

が存在する場合は PNG 付きメールを送信する。

代替方法

存在しない場合は sendmail により更新通知を送信する。

本文には公開ディレクトリへの URL を記載する。

実行ディレクトリ
Plain Text
1
/nwp/m1
その他の行を表示する

配下で処理を行う。

日別統計は

Plain Text
1
YYYY-MM/
その他の行を表示する

形式の月ディレクトリへ保存する。

例:

Plain Text
1
2026-08/
2
topics20260819.txt
3
ctab20260819.txt
その他の行を表示する
関連スクリプト
wnm-topicstat.rb
topicstat-ctab.rb
wnm-gtshist.rb
wnm-convobs.rb
convobs-merge.rb
備考

ファイル名には「topicstat」と付いているが、現在は Topic 統計だけでなく、wismon が生成する日次レポート類のほぼ全てを更新する統括バッチとして運用している。
