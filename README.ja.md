# Talk2Sheet

Talk2Sheet は、Excel / CSV に対して自然言語でデータ分析を行うためのオープンソースのフルスタックフレームワークです。

ユーザーの質問をもとに、ワークブック内で適切な対象シートを選び、実行可能な分析プランへ変換し、pandas で実行した結果と pipeline 情報をフロントエンドへ返します。

現在の安定版リリースは `v0.3.1` です。

## 現在の対象範囲

現在のリリースは次に集中しています。

- ワークブック内で一度に 1 つのシートを対象にする分析
- workbook レベルの multi-sheet 質問検出と分解ガイド
- 自然言語によるスプレッドシート分析
- clarification と follow-up 文脈を持つ複数ターン対話
- シート横断の順次分析をタスクステップとして進めるワークフロー
- 実行範囲、routing、結果テーブル、チャートの可視化
- 回答コピー、CSV/PNG エクスポート、リロード後のローカルセッション復元
- 英語 / 中国語 / 日本語のドキュメントと UI

現在対応しているもの：

- ファイルアップロード、sheet 一覧、プレビュー
- ワークブック内で 1 つの対象シートを選ぶ auto routing
- 同一 workbook 内の順次 multi-sheet 分析（A を先に分析し、その後 B に進む）
- タスクステップカード（未実行 / 実行中 / 完了 / 失敗）
- 「次のステップへ」ワンクリックで同スコープのまま次シートへ移動
- analysis anchor の引き継ぎによる metric / dimension / time スコープの安定化
- 前ステップ vs 現ステップ比較カード（A/B 比較、join なし）
- multi-sheet 質問に対する clarification と分解ヒント
- 行数、合計、平均、重複除去件数
- 期間比較: 前期比 / 前年比、差分、比率
- Top N / ranking
- 条件フィルター + groupby + Top N の複合質問
- detail rows
- detail + summary の構造化回答カード（結論 / 根拠 / 注意点）
- 日 / 週 / 月粒度の trend 分析
- チャート推奨、チャート文脈メタ情報、描画不可時のテキストフォールバック
- 軽量な時系列 forecast
- `auto / text / chart` mode 切替
- ユーザーが確認できる分析パイプライン、sheet routing 要約、構造化回答
- task_step 可観測イベント（`task_step_started` / `task_step_completed` / `task_step_failed`）と request_id の紐付け
- routing 理由と説明文の可視化（`reason` / `explanation` / `explanation_code`）
- シート/列の clarification カードを分けて表示し、選択後は自然な確認文で同じ質問を継続
- 結果カードの「続けて質問」提案（入力欄へ自動反映後に編集して送信可能）
- intent 回帰コーパスとオフライン評価（CI に統合）

現在まだ対応していないもの：

- 1 ターン内での複数シート join や multi-sheet 複合分析
- 高度統計
- 因果推論
- 本番向け object storage と永続 session backend

## リポジトリ構成

```text
apps/
  api/   FastAPI バックエンド
  web/   Vue 3 フロントエンド
docs/    英中日アーキテクチャ文書
packages/contracts/  生成済み OpenAPI 契約成果物
```

## ドキュメント

- English: [README.md](./README.md)
- 中文: [README.zh-CN.md](./README.zh-CN.md)
- 日本語: このファイル
- アーキテクチャ: [docs/architecture.ja.md](./docs/architecture.ja.md)
- 変更履歴: [CHANGELOG.md](./CHANGELOG.md)
- 最新リリースノート: [docs/releases/v0.3.1.md](./docs/releases/v0.3.1.md)

## 使い方

1. Excel または CSV ファイルをアップロードする
2. workbook 内の sheet をプレビューし、必要に応じて対象 sheet を選ぶ
3. 自然言語で質問する
4. multi-sheet 質問の場合は clarification を受け、まず 1 シートから開始する
5. routing、実行範囲、表、チャートとあわせて結果を確認する
6. 必要に応じて follow-up で別シートへ順次切り替える

## ローカル開発

### クイックスタート

1. `.env.example` を `.env` にコピーする
2. LLM ベースの計画や回答を使いたい場合は `TALK2SHEET_LLM_API_KEY` を設定する
3. バックエンドを起動する
4. フロントエンドを起動する
5. `http://127.0.0.1:5173` を開く

`TALK2SHEET_LLM_API_KEY` が空でもアプリは起動しますが、プロバイダ設定によっては一部の計画や回答経路が非 LLM 動作にフォールバックします。

### バックエンド

```bash
cp .env.example .env
cd apps/api
python3.11 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
python3.11 -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

### フロントエンド

```bash
cd apps/web
npm install
npm run dev -- --host 127.0.0.1 --port 5173
```

開発環境では、フロントエンドはデフォルトで `http://127.0.0.1:8000/api` に接続します。

## Security

`.env.example` を元にローカルの `.env` を作成し、モデルプロバイダ関連の設定はそこに記入してください。

バックエンドは `TALK2SHEET_LLM_API_KEY` を優先し、空の場合は `OPENAI_API_KEY` もフォールバックとして受け付けます。

`.env`、ユーザーがアップロードした表計算ファイル、実行時メタデータ、API キーやパスワードは公開リポジトリへコミットしないでください。

## Docker

```bash
docker compose up --build
```

起動後のアクセス先：

- Web: `http://localhost:8080`
- API: `http://localhost:8000`

コンテナ内で LLM ベースの計画や回答生成を有効にしたい場合は、次のように設定します。

```bash
cp .env.example .env
# .env を編集して TALK2SHEET_LLM_API_KEY=... を設定
docker compose up --build
```

現在の `docker-compose.yml` は provider 設定と LLM 設定を `api` コンテナへ引き渡します。キー自体はローカルの `.env` にのみ保存し、リポジトリへは含めないでください。

Docker Hub への接続が不安定な環境では、`.env` でベースイメージを上書きできます。

```bash
TALK2SHEET_PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.11-slim
TALK2SHEET_NODE_IMAGE=docker.m.daocloud.io/node:20-alpine
TALK2SHEET_NGINX_IMAGE=docker.m.daocloud.io/library/nginx:1.27-alpine
```

## 実装概要

現在のバックエンド主要フロー：

1. workbook context 構築
2. workbook routing（1 ターン 1 シート実行 + multi-sheet clarification / 分解ガイド）
3. capability guard
4. planner と意図理解
5. validation と repair
6. exact または通常実行
7. 構造化 answer generation
8. `meta`、`pipeline`、`answer`、終了イベントを SSE で返却

現在のフロントエンドが備えるもの：

- workbook 関連の状態管理
- conversation 関連の状態管理
- clarification interaction loop
- カテゴリ分けされた example prompt
- execution pipeline visibility
- sheet routing visibility
- routing explanation visibility（`reason`、`explanation`、`explanation_code`）

## 契約と検証

契約成果物は FastAPI の runtime schema から生成されます。

- OpenAPI 出力: `python apps/api/scripts/export_openapi.py`
- 契約整合性チェック: `python apps/api/scripts/check_contract_artifacts.py`
- フロント API 型生成: `cd apps/web && npm run generate:types`

検証コマンド：

- API: `pytest -q apps/api`
- Intent 回帰評価: `python apps/api/scripts/eval_intent_cases.py`
- Web: `cd apps/web && npm run ci`
- 全体: `make ci-check`

## よくある確認ポイント

- アップロードがすぐ失敗する:
  ファイル形式が `.xlsx`、`.xls`、`.csv` か確認し、非常に大きい場合はより小さいワークブックで試してください。
- プレビューや対話エラーに `request_id` が表示される:
  その `request_id` をバックエンドログと突き合わせて原因を追えます。
- Docker は起動したのに LLM 回答が出ない:
  `.env` に `TALK2SHEET_LLM_API_KEY` が入っているか、編集後に `docker compose up --build` をやり直したか確認してください。
- リロード後に復元されたセッションが消える:
  以前アップロードしたファイルがもう利用できないため、再アップロードが必要です。
