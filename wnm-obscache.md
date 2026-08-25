# wnm-obscache.rb - WIS2観測データキャッシュ生成ツール（構想）

## 概要

`wnm-obscache.rb` は、`wnmsaver.rb` が収集した WNM (WIS2 Notification Message) を解析し、WNM が参照する実データを取得してローカルに保存するプログラムである。

目的は解析そのものではなく、

```text
WNM
 ↓
観測データ取得
 ↓
観測データキャッシュ生成
```

を行うことである。

従来の `wnm-convobs.rb` は、

```text
WNM
 ↓
データ取得
 ↓
BUFR解析
 ↓
観測地点一覧生成
```

までを一度に実施していたが、本プログラムは取得処理を独立させる。

## 背景

現在のシステム構成は以下のようになっている。

```text
/nwp/m0
    WIS2 WNM 保存領域

/nwp/m1
    WIS2解析結果

/nwp/p0
    GTS観測データ保存領域

/nwp/p2
    観測データ解析結果
```

`feedfollow` パッケージは GTS から取得した観測報を

```text
/nwp/p0
```

に保存している。

一方、`wnm-convobs.rb` は WIS2 データを取得しているにも関わらず、一時的に解析して捨てているため、後続処理による再利用ができない。

今後、

- 観測地点分析
- 入電状況比較
- WSI品質監視
- 遅延統計
- station locator
- 各種BUFR解析

などを行うためには、WIS2経由で取得した観測データそのものを保存する方が望ましい。

## 目的

- WIS2経由の観測データをローカル保存する
- GTSデータと同様の利用基盤を構築する
- BUFR解析とデータ取得を分離する
- 後続の解析プログラムによる再利用を可能にする

## 設計方針

### 取得のみを担当する

本プログラムは、

- WNM解析
- URL取得
- 実データ保存

のみを担当する。

行わない処理:

- BUFR解析
- WSI推定
- プロット生成
- 観測地点抽出

これらは別プログラムに委ねる。

## 入力

### WNMアーカイブ

`wnmsaver.rb` が生成した

```text
/nwp/m0/jmagcHH.tar.gz
```

を利用する。

例:

```text
jmagc00.tar.gz
jmagc01.tar.gz
...
jmagc23.tar.gz
```

### 対象データ

当面は Conventional Observations を対象とする。

例:

```text
synop
temp
ship
wind-profile
buoys
```

将来的には任意の Topic を対象にできるようにする。

## 出力

### 観測データキャッシュ

保存先の第一候補は

```text
/nwp/p0
```

である。

理由:

- GTS観測データが既に存在する
- 既存ツール群が利用可能
- データ種別としては「観測データ」でありWIS2固有ではない

## ディレクトリ構成案

### 案1

```text
/nwp/p0/
    2026-08-25.new/
        obsbf-2026-08-25.tar
        obsbf-2026-08-25.wis2.tar
```

### 案2（推奨）

```text
/nwp/p0/
    2026-08-25.new/
        gts/
            obsbf.tar

        wis2/
            obsbf.tar
```

取得経路が明確になるため後者が望ましい。

## 起動方式

### 採用案

1時間ごとに生成される

```text
jmagcHH.tar.gz
```

単位で処理する。

```text
MQTT
 ↓
wnmsaver.rb
 ↓
jmagc13.tar
 ↓
gzip
 ↓
jmagc13.tar.gz
 ↓
wnm-obscache.rb
 ↓
p0
```

### 利点

どこまで処理したかを管理しやすい。

```text
jmagc13.tar.gz
```

を処理した時点で、

```text
13時台のWNMは全て処理済み
```

とみなせる。

MQTTメッセージ単位でのチェックポイント管理が不要になる。

## 処理済管理

処理済みファイルは

```text
/nwp/p0/wis2.done/
```

などで管理する。

例:

```text
jmagc00.done
jmagc01.done
...
```

未処理の tar.gz のみを対象とする。

## レイテンシ

本方式では、

```text
観測発生
 ↓
WNM
 ↓
時刻HHで集積
 ↓
HH+1 にローテーション
 ↓
ダウンロード
```

となる。

最大遅延は概ね1時間程度である。

## 既存システムとの連携

### feedfollow

GTS観測データ取得。

```text
GTS
 ↓
feedfollow
 ↓
p0
```

### wismon

WIS2観測データ取得。

```text
WIS2
 ↓
wnm-obscache.rb
 ↓
p0
```

### bufrconv

入力:

```text
p0
```

出力:

```text
p2/*-plot/
``*

### stnlocator（構*）

入力:

```text
p0
```

出力:

*``text
p2/*-stnloc/
```

## 将**想

観測データの取得元を意*せず、

```text
p0
```

のみ**力として解析できる構成を目*す。

理想形:

```text
                *----------+
GTS ----------* |         *|
                |   p0    *|
WIS2 ---------> |         *|
                +----------+
   *                 |
        +------*-----+------------+
        |     *      |            |
      plot*    *emagram      stnlocator*        |            |            *
      p2/          p2/          p*/
```

解析**グラムは

- feedfollow
- wismon

を直接意識*ず、

```text
観*データが p* に存在する
```

ことのみを前提と*る。

## 一言で言うと

**WIS2 W*M*が参照する実*ータを取得し、GTSデ*タと同様に再利用可能な観測データキャッシュ*して p0 に*積するためのブリッジプログラム。**
````*
