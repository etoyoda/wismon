# wnmsaver.rb - WIS2 MQTT → 時間別 tar.gz 保存を行うメイン収集デーモン

## 概要

WIS2 MQTT ブローカーから配信される WIS2 Notification Message (WNM) を受信し、JSON メッセージを時間単位の tar アーカイブとして保存する常駐プログラム。

通常は直接起動せず、`wismon3.service` または `wismon4.service` から起動する。

## 目的

- WIS2 で流通する WNM を継続的に収集する
- 後続の統計処理や調査のために生メッセージを保存する
- MQTT 接続断が発生しても自動的に再接続して収集を継続する

## 入力

- MQTT Broker
- 購読対象 Topic
- `/usr/local/etc/wismon-cfg.json`

## 出力

時間単位の tar ファイル。

例:

```text
wnm-2026081913.tar
wnm-2026081914.tar
```

実際のファイル名は `-o` オプションで指定したパターンに従う。

tar 内には WNM を JSON ファイルとして保存する。

例:

```text
1913AAAA-jma_jp_bufr.json
1913AAAB-jma_jp_synop.json
1913AAAC-noaa_us_satellite.json
```

保存される JSON は WNM 本文そのものであり、WIS2 データ本体ではない。
実データを取得するには、WNM に含まれる URL を参照して別途ダウンロードする必要がある。

## 主な処理

### MQTT 接続

設定ファイルまたはコマンドラインで指定された MQTT Broker に TLS 接続する。

### Topic 購読

指定された Topic を Subscribe する。

例:

```text
origin/a/wis2/#
cache/a/wis2/#
```

### WNM 保存

受信したメッセージを変更せず、そのまま JSON として保存する。

保存ファイル名には以下の情報を含める。

- 受信時刻（`date +%d%H` の4桁数字)
- 連番（`AAAA`, `AAAB`, ...）
- 短縮トピック

トピックは、cache/a/wis2 または origin/a/wis2 を除去し、スラッシュを _ に置換し、85文字を超える場合は上位の階層から順に1文字に短縮する。
たとえば _data_core_weather_prediction_analysis が _d_c_w_p_a とされることがある。

### 時間単位ローテーション

保存先 tar ファイルは 1 時間ごとに切り替える。

```text
13時台 → xxx13.tar
14時台 → xxx14.tar
```

これにより、書き込みプロセスと干渉することなく、直近1時間前までの履歴を追うことができる。

### 自動圧縮

ローテーション後の tar ファイルは gzip 圧縮する。

```text
xxx13.tar
↓
xxx13.tar.gz
```

wismon3.service の設定ではファイル名の %H だけが可変部であり、24時間前の tar.gz ファイルは上書きされる。

24時間以上前の WNM が指示するデータは、WIS2 Global Cache 上で既に削除されている可能性が高く、アクセスすると 404 エラーとなる。
さらに大量の失敗アクセスを繰り返すと、相手側から異常トラフィックまたはサイバー攻撃と判断されるおそれがある。
研究以外の目的では古い tar.gz ファイルに不用意にアクセスできるべきではないからである。

## 設定

接続設定は `/usr/local/etc/wismon-cfg.json` から取得する。

設定例:

```json
{
  "globalbroker": "mqtts://user:password@broker.example.com/origin/a/wis2/#"
}
```

起動時に設定キーを指定する。

```bash
wnmsaver.rb globalbroker
```

設定値は次のように解釈される。

- ホスト名
- ポート番号
- ユーザー名
- パスワード
- Topic

## コマンドラインオプション

```text
-h, --host=
    MQTT Broker ホスト名

-p, --port=
    MQTT Broker ポート番号

-u, --user=
    ユーザー名

-s, --pass=
    パスワード

-o, --out=
    出力ファイル名パターン。Ruby strftime のパターンでファイル名
```

## 障害対応

以下の例外発生時は再接続して動作を継続する。

- `MQTT::ProtocolException`
- `Timeout::Error`

長期間の連続運転を前提としている。

## 終了処理

SIGTERM などの終了シグナル受信時は、

1. tar ファイルをクローズ
2. 終了メッセージを出力
3. 正常終了

を行う。

## 関連ファイル

- `wismon3.service`
  - WNM 収集サービス（東京GCの監視）

- `wismon4.service`
  - WNM 収集サービス（米国GBから一部電文の監視）

- `wismon-cfg.json`
  - MQTT 接続設定

## 処理フロー

```text
systemd
   ↓
wismon3.service / wismon4.service
   ↓
wnmsaver.rb
   ↓
MQTT Broker へ接続
   ↓
Topic 購読
   ↓
WNM 受信
   ↓
時間別 tar へ保存
   ↓
旧 tar を gzip 圧縮
```

## 備考

本プログラムは **収集専用コンポーネント** である。

以下の処理は行わない。

- WNM 内容の解析
- KPI 算出
- データ品質チェック
- 統計集計

後続の解析・監視スクリプトは、本プログラムが生成した tar / tar.gz ファイルを入力として利用する。
