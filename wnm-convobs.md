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

本プログラムは観測地点を一意に識別するため、BUFR中の WSI
(001125〜001128) を優先的に利用する。

しかし実運用では WSI が欠損しているデータが少なくないため、
いくつかの規則に従って補完用WSIを生成する。

#### (1) 地上観測所番号から推定

WSI が存在せず、

```text
001001 WMO block number
001002 WMO station number
```

が存在する場合は、

```text
TSI = block * 1000 + station
```

を生成して WSI に変換する。

地上観測の場合:

```text
0-20000-0-TTTTT?
```

高層観測の場合:

```text
0-20001-0-TTTTT?
```

例:

```text
47 + 646
↓
47646

0-20000-0-47646?
```

一部、WSI付き通報とWSIなし通報が併存する地点があるので、両者の特性を区別して記録できるよう、この推定WSIは「?」を付加した形にする。

---

#### (2) 船舶識別子から推定

WSI が存在せず、

```text
001011 Ship or mobile land station identifier
```

が存在する場合は

```text
0-20003-0-SHIPID
```

または

```text
0-20003-1-SHIPID
```

を生成する。

高層観測の場合のみ issue number に 1 を用いる。

---

#### (3) ブイ識別子から推定

WSI が存在せず、

```text
001087 WMO Marine Obs. Platform Ext Identifier
```

が存在する場合は

```text
0-20002-0-IDENTIFIER
```

を生成する。多様なテンプレートでこの形式が用いられる。

---

#### (4) ブイ番号から推定

WSI が存在せず、

```text
001003 A1
001020 BW
001005 NNN - Buoy number
```

が利用できる場合は、

これらを組み合わせた識別子を生成し、

```text
0-20002-0-A1BWNNN
```

の形式でWSIを構築する。浮遊ブイで TM315009 の時に使われる。

---

#### (5) ブイ番号が欠損しているブイ

BUFR Category 0、Subcategory 7 であり上記のブイ番号が欠損しているときは、

```text
001015 Station or Site Name
```

を用いて、

```text
0-65534-1015-NAME
```

を生成する。

Issuer 65534 は本プログラム内で使用するローカル識別番号である。現在のところ、ベルギーの WESTHINDER 灯台でだけこの形式が用いられる。

---

#### (6) Dropsonde識別

BUFR Category 2、Subcategory 7 の場合は、

- DROP
- Platform identifier
- Topic名

を組み合わせて識別子を生成する。

形式:

```text
0-65534-9052-NAME
```

ここで 9052 は BUFR Template 309052 (Dropsonde) に由来する。

---

#### (7) NIL高層通報

BUFRを含まない

```text
TTAA NIL
PPBB NIL
```

等の高層通報を受信した場合は、

TSIを抽出して

```text
0-20001-0-TTTTT
```

の形式で観測所を登録する。

位置情報は保持されない。

---

#### (8) 高層観測における WSI補正

一部の高層観測データでは、

```text
0-20000-0-XXXXX
```

が使用されている。

本来 issuer 20000 は地上観測用であるため、異常なWSIとして記録する。

識別子末尾に

```text
!
```

を付加し、

```text
0-20000-0-XXXXX!
```

として保存する。

これにより通常の地上観測と区別する。

---

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
