# wnm-topicstat.rb - WNM保存アーカイブから Topic別流通量を集計するツール

## 概要

`wnmsaver.rb` が作成した tar.gz アーカイブを読み込み、Topic ごとの WNM 件数および平均データサイズを集計するプログラム。

WIS2 ノードや Global Cache において、

- どの Topic が多く流通しているか
- Topic ごとのデータ量はどの程度か
- 特定 Global Cache の流通傾向

を把握する目的で使用する。

## 目的

- Topic ごとの流通件数を集計する
- Topic ごとの平均データサイズを算出する
- Global Cache ごとの差異を分析する
- ノードごとの配信状況を比較する

## 入力

`wnmsaver.rb` が作成した tar.gz ファイル。

対象系列:

```text
/nwp/m0/jmagc??.tar.gz
/nwp/m0/devgc??.tar.gz
/nwp/m0/devnode??.tar.gz
```

各 tar.gz に保存された WNM JSON を解析する。
なお、jmagc 系列は wismon3.service が取得していて、devgc, devnode 系列は現在取得していない。

## 出力

CSV形式。

```text
件数,平均サイズ(kB),Topic
```

例:

```text
 12345,      8.321,origin/a/wis2/jma/jp/bufr
  6543,   1024.532,origin/a/wis2/noaa/us/grib2
```

## 主な処理

### WNM読み込み

tar.gz 内の全 JSON ファイルを読み込む。

### Topic復元

`wnmsaver.rb` が保存時に短縮した Topic 名を、可能な範囲で元の Topic 名へ復元する。

例:

```text
_d_c_w_p_a_
↓
_data_core_weather_prediction_analysis_
```

```text
_d_c_w_p_f_
↓
_data_core_weather_prediction_forecast_
```

その後、

```text
_
↓
/
```

へ変換して Topic 表記とする。

### 件数集計

Topic ごとに WNM 数をカウントする。

```ruby
ts[topic] += 1
```

### データサイズ推定

WNM 内の以下の項目からデータサイズを取得する。

優先順位:

1. links[].length
2. properties.content.size

取得できたサイズから平均値を算出する。

### Global Cache フィルタ

WNM の

```json
properties.global-cache
```

を参照し、指定された Global Cache のみを集計できる。

## コマンドラインオプション

### 系列選択

```bash
wnm-topicstat.rb jmagc
```

対象:

```text
jmagc
devgc
devnode
```

### Global Cache 選択

```bash
wnm-topicstat.rb -gc=Tokyo
```

指定した Global Cache の WNM のみを集計する。

### 高速モード

```bash
wnm-topicstat.rb -fast
```

各系列について最初の tar.gz ファイルのみを処理する。

大まかな傾向を確認したい場合に利用する。

## 出力項目

### 件数

その Topic の WNM 数。

### 平均サイズ(kB)

WNM が指し示すデータサイズの平均値。

データサイズ情報が存在する WNM のみを対象とする。

### Topic

復元後の Topic 名。
## 関連ファイル

- `wnmsaver.rb`
  - WNM収集・保存

- `jmagc??.tar.gz`
  - 東京GC監視結果

- `devgc??.tar.gz`
  - 開発GC監視結果

- `devnode??.tar.gz`
  - 開発ノード監視結果

## 備考

本プログラムは WNM を解析するものであり、WIS2 データ本体は取得しない。

集計されるサイズは WNM 内に記録されたサイズ情報であり、実際のダウンロード結果を測定したものではない。

## 一言で言うと

**保存済みWNMアーカイブを読み込み、Topic別の流通件数と平均データサイズを集計する分析ツール。**
