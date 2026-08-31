# wnm-obscache.rb - WIS2観測データキャッシュ生成ツール

## 一言で言うと

**WIS2 WNM が参照する実データを取得し、既存の GTS データと共存できる観測データキャッシュとして p0 に蓄積するためのブリッジプログラム。**

## 概要

`wnm-obscache.rb` は `wnmsaver.rb` が収集した WNM (WIS2 Notification Message) を解析し、WNM が参照する実データを取得してローカルキャッシュへ保存するプログラムである。

目的は解析そのものではなく、

```text
WNM
 ↓
観測データ取得
 ↓
観測データキャッシュ生成
```

を行うことである。

従来の `wnm-convobs.rb` は

```text
WNM
 ↓
データ取得
 ↓
BUFR解析
 ↓
観測地点一覧生成
```

までを一度に実施していたが、本プログラムは取得処理のみを担当する。

WIS2.0で配信されているデータであれば観測でなくても保存できる。

## 目的

- WIS2経由の観測データをローカル保存する
- GTSデータと同様の利用基盤を構築する
- データ取得とデータ解析を分離する
- 後続の解析プログラムによる再利用を可能にする

## 設計方針

### 取得のみを担当する

本プログラムは以下のみを担当する。

- WNM解析
- URL取得
- Base64データ展開
- 実データ保存

行わない処理:

- BUFR解析
- WSI推定
- プロット生成
- 観測地点抽出

これらは他のプログラムが担当する。

## 入力

### WNMアーカイブ

既定では

```text
/nwp/m0/jmagc??.tar.gz
```

を対象とする。

これらは `wnmsaver.rb` が時間単位で生成した WNM アーカイブである。

WNM アーカイブ内で WNM をフィルタする選択についてはコマンドラインオプション参照。

### 重複チェックと日界処理

本プログラムは繰り返し起動され、同じ日は同一出力ファイルに追記していくことを想定している。
同一の　WNM アーカイブを複数貴処理してしまわないよう、出力ファイルより mtime が古い WNM アーカイブはスキップする。

当日分の

```text
wisbf-YYYY-MM-DD.tar
```

がまだ存在しない場合は、前回処理時刻を推定する。

推定値は

```ruby
Time.now - 3600
```

とする。

目的は、00UTC直後の初回実行において、直前に確定した

```text
jmagc23.tar.gz
```

のみを処理対象とすることである。

例:

```text
00:10 UTC 起動

現在時刻 - 3600秒
↓
前日 23:10 UTC

jmagc22.tar.gz
  mtime = 23:00 UTC
  → 対象外

jmagc23.tar.gz
  mtime = 00:00 UTC
  → 対象
```

これにより前々時間帯の WNM アーカイブを重複処理しない。

`wnm-obscache.rb` は通常毎時10分頃に起動されるため、
3600秒のマージンで十分である。

また起動時刻が多少変更されても理解しやすい値である。

## コマンドラインオプション

### `--topic=REGEXP`
 
処理対象とする Topic を Ruby 正規表現で指定する。

既定値:

```text
(synop|temp|ship|wind-profile|buoys)
```

#### 例: SYNOPのみ
 
```bash
wnm-obscache.rb --topic='synop'
```
 
#### 例: SYNOPとTEMPのみ
 
```bash
wnm-obscache.rb --topic='synop|temp'
```

### `--gc=GLOBALCACHE`

処理対象とする Global Cache を指定する。

既定値:

```text
jp-jma-global-cache
```

例:

```bash
wnm-obscache.rb --gc=us-noaa-global-cache
```

指定した Global Cache が付与された WNM のみを処理する。

### --opfx=PREFIX

出力ファイル名の接頭辞を変更する。

既定値:

```text
wisbf
```

## 出力

### 観測データキャッシュ

保存先:

```text
/nwp/p0/incomplete
```

実際には `incomplete` シンボリックリンクの指す当日ディレクトリへ保存する。

例:

```text
/nwp/p0/incomplete
    ->
2026-08-26.new
```

出力ファイル:

```text
wisbf-2026-08-26.tar
```

※ 保存先ディレクトリが書き込み不可の場合、カレントディレクトリに同名のファイルを書き出す。非運用ユーザによる試験を想定した機能。

### Syslog

運用時の cron から起動された場合のログは次で取得できる:

$ journalctl -t wnm-obscache --facility=news

対話的に実行した場合は user ファシリティでログ出力されるので

$ journalctl -t wnm-obscache --facility=user

## 実行タイミング

cron により毎時10分頃に起動する。

目的は直前までにローテーションされた WNM アーカイブを取り込み、観測データキャッシュへ反映することである。

## 処理対象の決定

処理対象は

```text
/nwp/m0/jmagc??.tar.gz
```

のうち、

```text
mtime > 前回処理時刻
```

を満たすファイルとする。

### 前回処理時刻

前回処理時刻は出力ファイル

```text
wisbf-YYYY-MM-DD.tar
```

の mtime を利用する。

存在しない場合は

```ruby
Time.now - 3600
```

を用いる。

そのため初回実行時は直近1時間のアーカイブを処理する。

### 利点

追加の状態管理ファイルを必要としない。

```text
wisbf-YYYY-MM-DD.tar
```

自身がチェックポイントとして機能する。

## データ取得

WNM の

```json
links[].href
```

を参照して実データを取得する。

また、

```json
content.encoding = "base64"
```

の場合は、埋め込まれたデータを展開して利用する。

## 保存形式

取得したデータは tar アーカイブへ追記保存する。

WNM保存時のエントリ名

```text
1234AAAA-topic.json
```

は

```text
1234AAAA-topic.bin
```

へ変換して保存する。

取得データの内容は変更しない。

## 並列処理

20スレッドでデータ取得を行う。

```ruby
THREADS = 20
```

ダウンロード処理を並列化し、大量のWNMを短時間で処理する。

tarへの書き込みは Mutex により排他制御を行う。

## p0ローテーションとの連携

本プログラムは既存の p0 運用基盤を利用する。

### 日界処理

```text
run-prep0.sh
```

により

```text
YYYY-MM-DD.new
```

が

```text
YYYY-MM-DD
```

へリネームされる。

### gzip圧縮

```text
act-p0-housekeep.sh
```

により tar は gzip 圧縮される。

例:

```text
wisbf-2026-08-26.tar
↓
wisbf-2026-08-26.tar.gz
```

### 保持期間

既存の p0 ポリシーに従う。

- 約1週間保持
- その後削除

## 他パッケージとの関係

### feedfollow

```text
GTS
 ↓
feedfollow
 ↓
obsbf-YYYY-MM-DD.tar
```

### wismon

```text
WIS2
 ↓
wnm-obscache.rb
 ↓
wisbf-YYYY-MM-DD.tar
```

### bufrconv

入力:

```text
p0
```

出力:

```text
p2/*-plot/
```

### stnlocator（構想）

入*:

```text
p0
```

出力:

```text
**/*-stnloc/
```

## 将来構想

解析プログラムは取得元を意識しない構成を目指す。

```text
                +----------+
GTS ----------> |          |
                |   p0     |
WIS2 ---------> |          |
                +----------+
                     |
        +------------+------------+
        |            |            |
      plot        emagram     stnlocator
```

解析プログラムは

- feedfollow
- wismon

を直接意識せず、

```text
観測データが p0 に存在する
```

ことのみを前提とする。
