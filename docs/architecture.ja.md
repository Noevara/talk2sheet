# Talk2Sheet アーキテクチャ

## 1. 位置づけ

Talk2Sheet は、Excel / CSV を対象とするオープンソースの表計算対話分析フレームワークです。現在のアーキテクチャは次の範囲に集中しています。

- workbook routing と 1 ターン 1 シート実行
- multi-sheet 質問の検出、clarification、順次分解（A を先に、次に B）
- 自然言語を構造化された分析プランへ変換すること
- pandas ベースで実行可能な分析パイプラインを持つこと
- clarification、follow-up、mode 切替、文脈継承を伴う複数ターン対話
- pipeline 情報、実行範囲、結果、最終回答を構造化して UI に返すこと

現状では汎用 BI や高度統計プラットフォームとしては位置づけていません。境界は明確です。

- 対応済み: workbook routing、1 ターン 1 シート実行、順次 multi-sheet follow-up、detail / summary / ranking / trend / basic chart / lightweight forecast
- 未対応: 1 ターン内の複数シート join、自由な複合 multi-sheet 実行、シート間リレーション推論、高度統計、因果推論、複雑な予測ワークフロー

## 2. 実装済みの機能

### 2.1 プロダクト機能

- ファイルアップロード、シート一覧、プレビュー
- ファイル保存は `services/storage/` を通す構成に分離済み
- ワークブック内単一シートルーティング
  - 質問内の明示的な sheet 指定
  - sheet 名 / 列名に基づく auto routing
  - multi-sheet 質問に対する clarification と分解ヒント
  - follow-up での順次シート切替
  - 候補が競合した場合の sheet clarification
  - `sheet_override` による手動切替の優先
  - 同一 workbook 内でターンごとに対象シートを特定し、そのシート上で分析を完結できる
  - 複数シート join 実行にはまだ対応していない
- 自然言語分析
  - 行数、合計、平均、重複除去件数
  - Top N / ranking
  - detail rows
  - trend 分析と basic chart
  - 軽量な時系列 forecast
  - 緩い文字列 intent だけに依存せず、semantic intent を解釈できる
- 複数ターン対話
  - 直近ターンの pipeline summary を保持
  - semantic な文脈とシート文脈の継承
  - clarification ループ
- フロントエンド上の実行可視化
  - 実行パイプラインの可視化
  - sheet routing summary
  - result table / detail table / simple chart
  - structured answer segments
  - `mode = auto / text / chart`
  - OpenAPI 連携による契約同期
  - 基礎 observability
- フロントエンドは機能単位で分けた構成へ整理済み
- CI には段階的な検証が入っている

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

バックエンド本体は `apps/api/app/` 配下にあります。現時点では、責務ごとに分けたパッケージ構成で、全面的な再設計なしに継続改善しやすい形を取っています。

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

この層によって、ファイル保存の責務を spreadsheet 分析ロジックから切り離し、将来の object storage や別の永続化方式を追加しやすくしています。

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
3. 手動切替の `sheet_override`
4. follow-up シート切替解決（明示指定 / 前シート / 別シート）
5. multi-sheet clarification と分解分岐
6. 質問文内の明示的な sheet 指定
7. 直前ターンの follow-up 継承
8. sheet 名、列名、hint に基づく auto scoring
9. 候補が拮抗した場合の clarification
10. requested sheet への fallback

### 4.5 Planning と semantic 層

- `apps/api/app/services/spreadsheet/planning/`
  planner provider、heuristic rule、LLM planner、follow-up planning、guardrail
- `intent_models.py`
  構造化された `AnalysisIntent`
- `intent_understanding.py`
  semantic intent の解釈
- `intent_accessors.py`
  planner / answer / memory で共通利用する accessor

この層の重要な点は、単なる緩い `intent` 文字列ではなく、次のような構造を保持することです。

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
  answer card、sheet routing summary / 理由説明、execution disclosure、detail table、chart、structured answer
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
- intent 回帰（v0.3 コーパス）: `python apps/api/scripts/eval_intent_cases.py`
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
  - `multi_sheet_detected`
  - `clarification_sheet_count`
  - `sheet_switch_count`
  - `multi_sheet_failure_reason`
  - `multi_sheet_top_failure_reasons`（プロセス内の軽量集計）

### 6.4 request_id を使った切り分け

アップロード、プレビュー、対話リクエストが失敗した場合、フロントエンドのエラー文言には可能な限り `request_id` が含まれます。切り分けは次の順で進めるのが実用的です。

1. UI に表示された `request_id` を控える
2. バックエンドログで同じ `request_id` を検索する
3. まず次のイベントを確認する
   - `http_request_started / http_request_completed`
   - `http_exception / request_validation_failed`
   - `file_upload_started / file_upload_completed`
   - `file_preview_loaded`
   - `spreadsheet_chat_stream_requested / spreadsheet_chat_stream_opened`
   - `spreadsheet_chat_started / spreadsheet_chat_completed / spreadsheet_chat_failed`
4. HTTP は成功しているのに SSE だけ失敗している場合は、pipeline の `observability.request_id` と突き合わせる

これで、ユーザーが見たエラーをどのバックエンド段階の失敗かに短く結び付けられます。

## 7. 現在の境界

### 7.1 現在対応しているもの

- workbook-aware な単一シート auto routing
- workbook レベルの multi-sheet clarification / 分解ガイド
- follow-up での順次シート分析（A→B）
- sheet clarification と manual override
- follow-up context 継承
- semantic intent の理解
- detail rows / summary table / chart / lightweight forecast
- ユーザーから見える execution pipeline と scope disclosure

### 7.2 まだ未対応のもの

- 1 ターン内の複数シート join と自由な複合 multi-sheet 分析
- multi-sheet relationship reasoning と join planning
- 高度統計
- 長時間実行の async job orchestration
- 永続 session store と分散 dataframe cache
- 本番向け object storage 実装

## 8. 今後のアーキテクチャ方向

次のステップでは、現在の設計を置き換えるのではなく、その上に能力を積み上げるのが妥当です。

1. 現在の順次 multi-sheet フローの上に、明示的な制約付きで cross-sheet relationship モデルと join planning を導入する
2. session store と dataframe cache を Redis や DB backed な差し替え可能アダプタへ拡張する
3. planner / repair / capability governance をより明確な policy surface に分割する
4. フロントの pipeline 表示を debug 情報寄りのものから、よりプロダクト化された説明 UI へ進化させる
5. 本番運用向けに async job、audit log、object storage を追加する
