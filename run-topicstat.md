## 1. Topic統計生成

### 実行スクリプト

- `wnm-topicstat.rb`
- `topicstat-ctab.rb`

### 出力

```text
topicsYYYYMMDD.txt
ctabYYYYMMDD.txt
```

### 内容

過去24時間の WNM を解析し、

- Topic別流通件数
- 平均データサイズ

を集計する。

さらに Topic をカテゴリ分類した集計表を生成する。

### 日本GC限定統計

```text
topicsjYYYYMMDD.txt
ctabjYYYYMMDD.txt
```

も同時生成する。

これは

```text
global-cache = jp-jma-global-cache
```

のみを対象とした集計である。

---

## 2. GTS流通履歴更新

### 実行スクリプト

- `wnm-gtshist.rb`

### 出力

```text
gtshist-jmagc.txt
```

### 内容

GTS-to-WIS2 メッセージの流通履歴を更新する。

更新前ファイルは

```text
gtshist-jmagc-prev.txt
```

として保存する。

---

## 3. 観測データ履歴更新

### 実行スクリプト

- `wnm-convobs.rb`
- `convobs-merge.rb`

### 出力

```text
convobs.txt
```

### 内容

観測データの出現履歴を収集する。

収集結果を過去履歴へマージし、長期履歴を維持する。

更新前ファイルは

```text
convobs-prev.txt
```

として保存する。

---

## 4. 観測カバレッジ図作成

GMT が利用可能な場合に実行する。

```text
/usr/bin/gmt
```

### 出力

```text
convobs.png
convobs2.png
```

### convobs.png

対象:

- SYNOP
- TEMP

直近24時間に観測された地点を世界地図上へ描画する。

記号:

- 橙丸: SYNOP
- 青三角: TEMP
- 水色逆三角: DROP

### convobs2.png

対象:

- SHIP
- DRIFTING BUOY
- MOORED BUOY
- WAVE BUOY
- WIND PROFILER

直近24時間の観測点分布を表示する。

### 目的

Global Cache 経由で実際に流通している観測データの地理的カバレッジを視覚的に確認する。

---

## 5. 通知メール送信

### 優先方法

```text
/nwp/bin/send_png_mail.rb
```

が存在する場合は PNG 付きメールを送信する。

### 代替方法

存在しない場合は sendmail により更新通知を送信する。

本文には公開ディレクトリへの URL を記載する。

---

## 実行ディレクトリ

```text
/nwp/m1
```

配下で処理を行う。

日別統計は

```text
YYYY-MM/
```

形式の月ディレクトリへ保存する。

例:

```text
2026-08/
  topics20260819.txt
  ctab20260819.txt
```

---

## 関連スクリプト

- `wnm-topicstat.rb`
- `topicstat-ctab.rb`
- `wnm-gtshist.rb`
- `wnm-convobs.rb`
- `convobs-merge.rb`

---

## 備考

ファイル名には「topicstat」と付いているが、現在は Topic 統計だけでなく、wismon が生成する日次レポート類のほぼ全てを更新する統括バッチとして運用している。
