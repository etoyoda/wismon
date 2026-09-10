# wnm-topicstat.sh

## 概要

WNMアーカイブ (`jmagc??.tar.gz`) を集計し、

- Topic別流通統計
- Topicカテゴリ別集計
- 日本 Global Cache 限定統計
- GTS-to-WIS2 流通履歴

を更新する日次バッチである。

処理結果は `/nwp/m1` 以下へ保存される。

---

## 1. Topic統計生成

### 実行スクリプト

- `wnm-topicstat.rb`
- `topicstat-ctab.rb`

### 出力

```text
YYYY-MM/topicsYYYYMMDD.txt
YYYY-MM/ctabYYYYMMDD.txt
```

例:

```text
2026-09/topics20260908.txt
2026-09/ctab20260908.txt
```

### 内容

WNM アーカイブを解析し、

- Topic別流通件数
- 総データ量
- 平均データサイズ

などを集計する。

続いて Topic をカテゴリへ分類し、

```text
ctabYYYYMMDD.txt
```

としてカテゴリ別集計表を生成する。

---

## 2. 日本 Global Cache 限定統計

### 実行スクリプト

```text
wnm-topicstat.rb -gc=jp-jma-global-cache
topicstat-ctab.rb
```

### 出力

```text
YYYY-MM/topicsjYYYYMMDD.txt
YYYY-MM/ctabjYYYYMMDD.txt
```

### 内容

WNM のうち

```text
properties.global-cache = jp-jma-global-cache
```

が付与された通知のみを対象として集計する。

Topic統計およびカテゴリ別集計を、日本 Global Cache 視点で作成する。

---

## 3. GTS流通履歴更新

### 実行スクリプト

- `wnm-gtshist.rb`

### 出力

```text
gtshist-jmagc.txt
```

### バックアップ

更新前ファイルは

```text
gtshist-jmagc-prev.txt
```

として保存する。

### 内容

GTS-to-WIS2 Topic を抽出し、

- TTAAii
- CCCC
- 初出日時
- 最終出現日時

などの履歴情報を更新する。

既存の

```text
gtshist-jmagc.txt
```

が存在する場合は、それを参照して履歴を継続更新する。

---

## 実行ディレクトリ

処理は

```text
/nwp/m1
```

配下で実行される。

日次統計は月別ディレクトリへ保存する。

例:

```text
/nwp/m1
├── 2026-09/
│   ├── topics20260908.txt
│   ├── ctab20260908.txt
│   ├── topicsj20260908.txt
│   └── ctabj20260908.txt
├── gtshist-jmagc.txt
└── gtshist-jmagc-prev.txt
```

---

## 関連スクリプト

- `wnm-topicstat.rb`
- `topicstat-ctab.rb`
- `wnm-gtshist.rb`

---

## 備考

処理対象日は

```bash
date --date 'now - 1 hour'
```

により決定される。

統計ファイルは処理対象日の属する月ディレクトリへ保存される。

月別ディレクトリが存在しない場合は自動作成される。

本バッチは WIS2 WNM の流通状況を継続監視するための定期集計処理であり、Topic統計および GTS-to-WIS2 流通履歴の更新を担当する。
