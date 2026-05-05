# Changelog

All notable changes to the EbbingFlow project will be documented in this file starting from the first official release.

## [Unreleased]
### Added
- **Intent-aware memory priority**: Retrieval now classifies each turn as `fact`, `summary`, `long_term`, or `semantic`, then applies source-specific budgets and multipliers in `HybridScorer`.
- **Narrative prompt lane**: Episode/Saga candidates are injected into a separate `[NARRATIVE]` context area and only when accepted by the scorer.
- **Narrative-day recall**: Queries such as `第 61 天` / `day 23` now route through `narrative_day` filters, with a backfill script for historical memory events.
- **Flexible date-window recall**: Retrieval now understands phrases such as `前几天`, `过去5天`, `3月8号`, `3月8号那几天`, and `3月中旬`.
- **RESOURCE structured memory**: Added a `RESOURCE` event type for countable inventory/supply/logistics memories with `quantity` and `quantity_unit` payloads.
- **SQL raw-dialogue evidence recall**: Fact-style questions can now retrieve exact source chat messages from SQL history as `[SQL_EVIDENCE]`.
- **SOP response-phase audit tests**: Added regression coverage for response-phase audit errors and writer-internal failure handling.
- **User data isolation**: Added per-user data isolation across chat history, vector memory, Neo4j graph data, and demo access tokens.
- **OpenAI-compatible chat API**: Added `/v1/chat/completions` and `/api/chat/completions` for local external frontends, with non-streaming and SSE streaming responses.

### Changed
- **Evidence-first retrieval**: Fact-style questions now prioritize SQL/Graph/Structured Evidence and strictly exclude zero-budget narrative fallbacks.
- **Graph fallback safety**: Graph recall no longer falls back to unrelated global high-impact events when a query has no entity/time/narrative-day filter.
- **Structured quantity aggregation**: RESOURCE loss/consumption events now subtract from inventory totals while preserving raw totals for audit.
- **Evidence-chain persistence**: Chat turns are persisted through async session writes so SQL message IDs are available for later evidence windows.
- **Token attribution**: Data Monitor now attributes post-response token usage across steps 08-12 instead of letting large memory costs fall through to audit settlement.
- **Middleware audit plumbing**: Response-phase audit callbacks are now attached to `ChatSession`, keeping middleware signatures clean.
- **Embedding monitoring**: Vector query embedding usage is now recorded after successful retrieval and validates collection names explicitly.
- **Startup safety**: EbbingFlow now stops startup when core memory storage cannot initialize, preventing chat sessions from running without persistent memory.

### Removed
- Removed the experimental CRM sync and CRM SQL retrieval path from the open-source edition, including CRM API endpoints, CRM-specific SQL retriever code, config flags, and tests.

### Fixed
- Fixed fact attribution behavior so questions like "where did you know this from?" must rely on SQL/Graph/raw-record evidence instead of narrative summaries.
- Fixed duplicated `inference_turn_count` increments between chat engine and API scheduling.
- Fixed double-application of HybridScorer source multipliers.
- Fixed `budget=0` fallback leakage that could let Episode/Saga appear in fact-only answers.
- Fixed mojibake detection for persona state validation and cleaned corrupted comments/log text.
- Fixed response-phase exception reporting so unfinished SOP steps are marked `error` instead of silent `done`.
- Tightened numeric fact detection to avoid bare numbers and English words such as `3 mood` being routed as factual quantity queries.

## [1.1.0] - 2026-04-27
### Added
- **Demo Data Import**: One-click restoration of the pre-configured demo: *"Reborn as Zhuge Liang: Building an Empire with EbbingFlow"* («重生成为诸葛亮，系统（Ebbingflow）助我成就霸业»).
- **Atomic Runtime Restore**: Backend mechanism (`/maintenance/restore-demo-data`) that safely releases database locks (SQLite, Neo4j, Chroma) to allow data replacement without server restart.
- **Visual Feedback**: Added confirmation dialogs and full-screen loading overlays for maintenance operations.

### Changed
- Replaced "Personality Reassessment" (人格重判) button with "Import Demo Data" (导入演示数据) in the Data Monitor header.

## [0.1.0] - Initial internal development
- Initial internal development and cognitive infrastructure stabilization.
