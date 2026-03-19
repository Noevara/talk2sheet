# Talk2Sheet アーキテクチャ

## 1. 位置づけ

Talk2Sheet は、Excel / CSV を対象とするオープンソースの表計算対話分析フレームワークです。現時点では次の範囲に集中しています。

- ワークブック内で 1 つのシートへルーティングして分析すること
- 自然言語を構造化された分析プランへ変換すること
- pandas ベースで実行可能な分析パイプラインを持つこと
- clarification、follow-up、mode 切替、文脈継承を伴う複数ターン対話
- pipeline 情報、実行範囲、結果、最終回答を構造化して UI に返すこと

現状では汎用 BI や高度統計プラットフォームとしては位置づけていません。境界は明確です。

- 対応済み: ワークブック内単一シート選択、auto routing、follow-up、detail / summary / ranking / trend / basic chart / lightweight forecast
- 未対応: 複数シート join、複合 multi-sheet 分析、シート間リレーション推論、高度統計、因果推論、複雑な予測ワークフロー

## 2. 実装済みの機能

### 2.1 プロダクト機能

- ファイルアップロード、シート一覧、プレビュー
- ワークブック内単一シートルーティング
  - 質問内の明示的な sheet 指定
  - sheet 名 / 列名に基づく auto routing
  - 候補が競合した場合の sheet clarification
  - `sheet_override` による手動切替の優先
- 自然言語分析
  - 行数、合計、平均、重複除去件数
  - Top N / ranking
  - detail rows
  - trend 分析と basic chart
  - 軽量な時系列 forecast
- 複数ターン対話
  - 直近ターンの pipeline summary を保持
  - semantic な文脈とシート文脈の継承
  - clarification ループ
- フロントエンド上の実行可視化
  - execution disclosure
  - sheet routing summary
  - result table / detail table / simple chart
  - structured answer segments

### 2.2 直近で完了したリファクタ項目

一時ロードマップにあった以下の項目は、すでに現行実装へ統合されています。

- `P0`
  - clarification のフロント閉ループ
  - `mode = auto / text / chart`
  - `App.vue` の初期分割
  - OpenAPI 連携による契約同期
  - 基礎 observability
- `P1`
  - `services/storage/` へのファイル保存抽象化
  - feature 単位のフロント構成
  - layered CI validation
- `P2`
  - semantic intent layer
  - `P2-2A`: cross-sheet 実行ではなく、ワークブック内単一シート routing を実装

## 3. エンドツーエンドの処理フロー

1 回のリクエストの主経路は次のとおりです。

1. フロントエンドが `/api/files/upload` へファイルを送信
2. バックエンドが `services/storage/` を通してファイルを保存し、`file_id` を返す
3. フロントエンドが sheet preview を読み込み、workbook / active sheet 文脈を構築
4. ユーザーが以下を含む質問を送信
   - `chat_text`
   - `mode`
   - `sheet_index`
   - `sheet_override`
   - `conversation_id`
   - `clarification_resolution`
5. バックエンドの `stream_spreadsheet_chat()` が
   - conversation session を新規作成または再利用
   - workbook context を構築
   - 対象シートを routing
   - そのシートの sampled dataframe を読み込み
   - 分析オーケストレータへ渡す
6. 分析オーケストレータ内部では
   - capability guard
   - planner
   - semantic intent understanding
   - validation / repair
   - exact execution または通常実行
   - answer generation
7. バックエンドは SSE で次を順に返す
   - `meta`
   - `pipeline`
   - `answer`
   - `EOS`
8. フロントエンドはこれらをメッセージカードに組み立て、scope、routing、chart、detail rows、structured answer を表示する

## 4. バックエンド構成

バックエンド本体は `apps/api/app/` 配下にあります。現時点では大規模な DDD への全面移行ではなく、責務ごとのパッケージ分割を採用しています。

### 4.1 API と基盤

- `apps/api/app/main.py`
  FastAPI エントリ、CORS、middleware、統一例外処理
- `apps/api/app/schemas.py`
  HTTP request / response contract
- `apps/api/app/observability.py`
  `X-Request-ID` と構造化ログ補助
- `apps/api/app/api/routes/files.py`
  upload、sheet list、preview endpoint
- `apps/api/app/api/routes/spreadsheet.py`
  SSE chat endpoint

### 4.2 ストレージ層

- `apps/api/app/services/storage/`
  ファイル保存の抽象インターフェース
- `local_file_store.py`
  現行の標準実装。アップロードをチャンクでローカル保存し、メタデータも管理
- `object_storage_file_store.py`
  オブジェクトストレージ向けの拡張ポイント

これは `P1-1` の成果であり、ファイル永続化を spreadsheet 業務コードから切り離しています。

### 4.3 Spreadsheet pipeline 層

- `apps/api/app/services/spreadsheet/pipeline/`
  ファイル読み込み、sheet metadata、header detection、preview 生成、workbook context 構築
- `workbook_context.py`
  sheet routing に必要な workbook レベル情報を構築
- `sheet_metadata.py`
  sheet descriptor の取得

この層が、生ファイルを workbook metadata と analyzable dataframe に変換します。

### 4.4 Routing 層

- `apps/api/app/services/spreadsheet/routing/sheet_router.py`
  ワークブック内単一シート routing
- `router_types.py`
  routing decision の型

現在の優先順位は概ね次のとおりです。

1. 単一シート workbook の即時確定
2. clarification resolution
3. 質問文内の明示的な sheet 指定
4. 手動切替の `sheet_override`
5. 直前ターンの follow-up 継承
6. sheet 名、列名、hint に基づく auto scoring
7. 候補が拮抗した場合の clarification
8. requested sheet への fallback

### 4.5 Planning と semantic 層

- `apps/api/app/services/spreadsheet/planning/`
  planner provider、heuristic rule、LLM planner、follow-up planning、guardrail
- `intent_models.py`
  構造化された `AnalysisIntent`
- `intent_understanding.py`
  semantic intent の解釈
- `intent_accessors.py`
  planner / answer / memory で共通利用する accessor

これは `P2-1` の中心であり、単なる緩い `intent` 文字列ではなく、次のような構造を保持します。

- `target_metric`
- `target_dimension`
- `comparison_type`
- `time_scope`
- `answer_expectation`
- `clarification`

### 4.6 Analysis / quality / execution 層

- `apps/api/app/services/spreadsheet/analysis/`
  planner、validation、execution、answer generation を束ねる最上位オーケストレータ
- `apps/api/app/services/spreadsheet/quality/`
  capability guard、validator、repair、governance policy
- `apps/api/app/services/spreadsheet/execution/`
  selection / transform / exact execution / pivot / formula metric 実装
- `apps/api/app/services/spreadsheet/core/`
  schema、i18n、serialization などの共有契約

現在の分析パスには以下がすでに含まれます。

- intent レベル clarification の短絡
- unsupported capability の遮断
- exact execution disclosure
- structured pipeline metadata

### 4.7 Conversation と answer 層

- `apps/api/app/services/spreadsheet/conversation/`
  session memory、follow-up context、rule-based / llm-based summarizer、formatter
- `conversation_memory.py`
  in-memory session、turn summary、dataframe cache

会話層は現在、次の情報を保持します。

- recent pipeline summary
- analysis intent summary
- 直近の `sheet_index / sheet_name`
- clarification resolution payload

## 5. フロントエンド構成

フロントエンド本体は `apps/web/src/` 配下にあります。現在は単一巨大コンポーネントから、feature-oriented な構成へ移行済みです。

### 5.1 トップレベル構成

- `apps/web/src/app/AppShell.vue`
  workbook feature、conversation feature、locale switcher をメインワークスペースへ構成する

### 5.2 Workbook feature

- `apps/web/src/features/workbook/composables/useWorkbook.ts`
  file upload、sheet selection、preview load、`pendingSheetOverride`
- `apps/web/src/features/workbook/components/WorkbookFeaturePanel.vue`
  workbook 側 feature wrapper
- `apps/web/src/components/WorkbookPreviewPanel.vue`
  preview panel と sheet tabs

### 5.3 Conversation feature

- `apps/web/src/features/conversation/composables/useConversation.ts`
  question state、message list、conversation id、clarification follow-up、request assembly
- `apps/web/src/features/conversation/composables/useSseChat.ts`
  SSE request と streaming state
- `apps/web/src/features/conversation/components/ConversationFeaturePanel.vue`
  conversation 側 feature wrapper

### 5.4 共通 UI コンポーネント

- `apps/web/src/components/ConversationComposer.vue`
  input、mode switch、example / guide popover
- `apps/web/src/components/ConversationMessage.vue`
  answer card、sheet routing summary、execution disclosure、detail table、chart、structured answer
- `apps/web/src/components/ClarificationOptions.vue`
  clarification interaction
- `apps/web/src/components/DataTable.vue`
  preview / result table
- `apps/web/src/components/SimpleChart.vue`
  軽量チャート描画

### 5.5 契約と i18n

- `apps/web/src/generated/api-types.ts`
  OpenAPI 由来の生成型
- `apps/web/src/types.ts`
  フロント側のラッパー型
- `apps/web/src/lib/api.ts`
  HTTP / SSE 呼び出し
- `apps/web/src/lib/chatPayload.ts`
  SSE payload 正規化
- `apps/web/src/i18n/messages.ts`
  英中日文案。能力境界、sheet routing、カテゴリ別 example prompt もここで管理

## 6. 契約、テスト、エンジニアリング護栏

### 6.1 契約ガバナンス

- `apps/api/scripts/export_openapi.py`
  FastAPI runtime schema から OpenAPI を出力
- `apps/web/scripts/generate_api_types.py`
  フロント型を生成
- `apps/api/scripts/check_contract_artifacts.py`
  契約生成物のズレを検出

現在の契約フローは次のとおりです。

- backend schema -> OpenAPI -> frontend types
- `sheet_override`、clarification payload、sheet routing metadata などもこの経路で同期される

### 6.2 テストと CI

- API: `pytest -q apps/api`
- Web: `npm run ci`
- frontend CI の内容
  - feature boundary check
  - lint
  - typecheck
  - vitest
  - build

### 6.3 基礎 observability

システムにはすでに request 単位の基礎 observability が入っています。

- `X-Request-ID`
- 構造化 request log
- pipeline metadata 内の `observability`
  - `request_id`
  - `request_total_ms`
  - stage timings

## 7. 現在の境界

### 7.1 現在対応しているもの

- workbook-aware な単一シート auto routing
- sheet clarification と manual override
- follow-up context 継承
- semantic intent
- detail rows / summary table / chart / lightweight forecast
- ユーザーから見える execution pipeline と scope disclosure

### 7.2 まだ未対応のもの

- 複数シート join と複合 multi-sheet 分析
- multi-sheet relationship reasoning と join planning
- 高度統計
- 長時間実行の async job orchestration
- 永続 session store と分散 dataframe cache
- 本番向け object storage 実装

## 8. 今後のアーキテクチャ方向

次段階では、現在の設計を活かしながら次を進めるのが妥当です。

1. 単一シート routing の上に `P2-2B` 相当の multi-sheet planning を積み上げ、いきなり unrestricted join へ飛ばない
2. session store と dataframe cache を Redis や DB backed な差し替え可能アダプタへ拡張する
3. planner / repair / capability governance をより明確な policy surface に分割する
4. フロントの pipeline 表示を debug 情報寄りのものから、よりプロダクト化された説明 UI へ進化させる
5. 本番運用向けに async job、audit log、object storage を追加する
