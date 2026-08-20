# wnm-convobs.rb - Conventional Observations の観測地点一覧を生成するツール

## 概要

ここでいう **convobs** は *Conventional Observations* を意味し、気象分野で一般に使用される「非衛星観測」の総称である。

対象には以下が含まれる。

- SYNOP
- TEMP
- SHIP
- DRIFTING BUOY
- MOORED BUOY
- WAVE BUOY
- WIND PROFILER

本プログラムは `wnmsaver.rb` が保存した WNM を解析し、さらに WNM が指す実データ（主に BUFR）を取得・解析することで、Conventional Observations の観測地点一覧を生成する。

通常は `run-topicstat.sh` から日次実行される。

## 目的

- WIS2 経由で流通した Conventional Observations を解析する
- 実際に観測地点が存在したかを確認する
- 観測地点ごとの最新出現時刻を記録する
- 観測データの地理的カバレッジを確認する
- Global Cache 上の観測データ流通状況を監視する

## 入力

### WNMアーカイブ

`wnmsaver.rb` が生成した tar.gz ファイル。

既定値:

```text
/nwp/m0/jmagc[0-9][0-9].tar.gz
```

### 対象Topic

既定値:

```text
synop
temp
ship
wind-profile
buoys
```

を含む Topic。

### 実データ

WNM に含まれる

```json
links[].href
```

または

```json
content.value
```

から取得する。

## 出力

タブ区切りテキスト。

```text
WSI    LatestTime    RefTime    StationID ...
```

形式。

1行が1観測地点に対応する。

出力は通常 `convobs-cur.txt` として保存され、後続処理で履歴ファイルへ統合される。

## 主な処理

### 1. WNM読み込み

保存済み tar.gz を読み込み、

- Topic
- Global Cache
- データURL

を取得する。

### 2. Global Cache選別

既定では

```text
jp-jma-global-cache
```

由来の WNM のみを処理する。

### 3. データ取得

WNM が示す URL へアクセスし実データを取得する。

対応形式:

- HTTP取得
- Base64埋め込みデータ

### 4. BUFRデコード

取得した BUFR を解析し、

- 観測地点
- 緯度
- 経度
- WSI
- 観測時刻

を抽出する。

以下のライブラリを利用する。

```text
bufrscan
bufrdump
```

### 5. WSI構築

可能な場合は BUFR 内に格納された

```text
001125
001126
001127
001128
```

から WSI を構築する。

例:

```text
0-20001-0-47646
```

### 6. WSI補完

正式な WSI が存在しない場合は、

- WMO Block Number
- WMO Station Number
- Ship Identifier
- Radiosonde Identifier
- TEMP DROP識別子

などから擬似識別子を生成する。

これにより観測地点を継続的に追跡できる。

### 7. 最新観測時刻管理

同じ観測地点が複数回出現した場合は、

```text
最後に観測された時刻
```

のみを保持する。

過去の履歴は `convobs-merge.rb` で統合する。

## Topic名復元

`wnmsaver.rb` 保存時に短縮された Topic 名を復元する。

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

さらに

```text
_
↓
/
```

へ変換し、本来の Topic 表記に戻す。

## 並列処理

20スレッドでデータ取得・解析を行う。

```ruby
THREADS = 20
```

WNM の解析とデータ取得を並列化し、大量データでも短時間で処理できるようにしている。

## コマンドラインオプション

### Global Cache選択

```bash
wnm-convobs.rb --gc=jp-jma-global-cache
```

指定した Global Cache のみを対象とする。

### Topic選択

```bash
wnm-convobs.rb --topic='synop|temp'
```

正規表現で対象 Topic を指定できる。

### 入力ファイル指定

```bash
wnm-convobs.rb sample.tar.gz
```

任意のアーカイブを対象に解析できる。

## 出力項目

### WSI

観測地点識別子。

正式な WSI が存在しない場合は推定識別子を生成する。

### 最新観測時刻

その観測地点が最後に確認された時刻。

### 観測種別

BUFR カテゴリおよびサブカテゴリ。

### 緯度・経度

観測地点位置。

### Topic

観測データを受信した WIS2 Topic。

## 利用例

### 最新観測地点一覧生成

```bash
ruby wnm-convobs.rb > convobs-cur.txt
```

### SYNOPのみ解析

```bash
ruby wnm-convobs.rb --topic='synop'
```

### TEMPのみ解析

```bash
ruby wnm-convobs.rb --topic='temp'
```

## 関連スクリプト

- `wnmsaver.rb`
  - WNM収集・保存

- `run-topicstat.sh`
  - 日次バッチ処理

- `convobs-merge.rb`
  - 観測地点履歴の統合

## 処理フロー

```text
WNM tar.gz
    ↓
WNM解析
    ↓
URL取得
    ↓
実データ取得
    ↓
BUFRデコード
    ↓
WSI抽出
    ↓
観測地点一覧生成
    ↓
convobs-cur.txt
```

## 備考

本プログラムは WNM を集計するだけではなく、WNM が参照する実データまで取得して解析する。

そのため `wnm-topicstat.rb` よりも負荷の高い処理を行う。

生成された観測地点一覧は、

- 観測データの流通確認
- WIS2カバレッジ監視
- 世界地図への観測点描画

などに利用される。
