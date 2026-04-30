# Changelog

All notable changes to the EbbingFlow project will be documented in this file starting from the first official release.

## [Unreleased]
### Added
- **Intent-aware memory priority**: Retrieval now classifies each turn as `fact`, `summary`, `long_term`, or `semantic`, then applies source-specific budgets and multipliers in `HybridScorer`.
- **Narrative prompt lane**: Episode/Saga candidates are injected into a separate `[NARRATIVE]` context area and only when accepted by the scorer.
- **SOP response-phase audit tests**: Added regression coverage for response-phase audit errors and writer-internal failure handling.
- **User data isolation**: Added per-user data isolation across chat history, vector memory, Neo4j graph data, and demo access tokens.

### Changed
- **Evidence-first retrieval**: Fact-style questions now prioritize SQL/Graph/Structured Evidence and strictly exclude zero-budget narrative fallbacks.
- **Token attribution**: Data Monitor now attributes post-response token usage across steps 08-12 instead of letting large memory costs fall through to audit settlement.
- **Middleware audit plumbing**: Response-phase audit callbacks are now attached to `ChatSession`, keeping middleware signatures clean.
- **Embedding monitoring**: Vector query embedding usage is now recorded after successful retrieval and validates collection names explicitly.

### Fixed
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
