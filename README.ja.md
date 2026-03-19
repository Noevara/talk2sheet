# Talk2Sheet

Talk2Sheet は、Excel / CSV に対して自然言語でデータ分析を行うためのオープンソースのフルスタックフレームワークです。

ユーザーの質問をもとに、ワークブック内で適切な単一シートを選び、実行可能な分析プランへ変換し、pandas で実行した結果と pipeline 情報をフロントエンドへ返します。

## v0.1.0 の対象範囲

現在のリリースは次に集中しています。

- ワークブック内単一シートのスマートルーティング
- 自然言語によるスプレッドシート分析
- clarification と follow-up 文脈を持つ複数ターン対話
- 実行範囲、routing、結果テーブル、チャートの可視化
- 英語 / 中国語 / 日本語のドキュメントと UI

現在対応しているもの：

- ファイルアップロード、sheet 一覧、プレビュー
- ワークブック内単一シートの auto routing
- 行数、合計、平均、重複除去件数
- Top N / ranking
- detail rows
- trend 分析と basic chart
- 軽量な時系列 forecast
- `auto / text / chart` mode 切替
- 構造化された planner / validator / repair / exact execution / answer generation パイプライン

現在まだ対応していないもの：

- 複数シート join や multi-sheet 複合分析
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

## ローカル開発

### バックエンド

```bash
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

`.env`、ユーザーがアップロードした表計算ファイル、実行時メタデータ、API キーやパスワードは公開リポジトリへコミットしないでください。

## Docker

```bash
docker compose up --build
```

起動後のアクセス先：

- Web: `http://localhost:8080`
- API: `http://localhost:8000`

Docker Hub への接続が不安定な環境では、`.env` でベースイメージを上書きできます。

```bash
TALK2SHEET_PYTHON_IMAGE=docker.m.daocloud.io/library/python:3.11-slim
TALK2SHEET_NODE_IMAGE=docker.m.daocloud.io/node:20-alpine
TALK2SHEET_NGINX_IMAGE=docker.m.daocloud.io/library/nginx:1.27-alpine
```

## エンジニアリング概要

現在のバックエンド主要フロー：

1. workbook context 構築
2. 単一シート routing
3. capability guard
4. planner と semantic intent 理解
5. validation と repair
6. exact または通常実行
7. 構造化 answer generation
8. `meta`、`pipeline`、`answer`、終了イベントを SSE で返却

現在のフロントエンドが備えるもの：

- workbook feature state
- conversation feature state
- clarification interaction loop
- カテゴリ分けされた example prompt
- execution pipeline visibility
- sheet routing visibility

## 契約と検証

契約成果物は FastAPI の runtime schema から生成されます。

- OpenAPI 出力: `python apps/api/scripts/export_openapi.py`
- 契約整合性チェック: `python apps/api/scripts/check_contract_artifacts.py`
- フロント API 型生成: `cd apps/web && npm run generate:types`

検証コマンド：

- API: `pytest -q apps/api`
- Web: `cd apps/web && npm run ci`
- 全体: `make ci-check`
