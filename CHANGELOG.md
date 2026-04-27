# Changelog

All notable changes to the EbbingFlow project will be documented in this file starting from the first official release.

## [1.1.0] - 2026-04-27
### Added
- **Demo Data Import**: One-click restoration of the pre-configured demo: *"Reborn as Zhuge Liang: Building an Empire with EbbingFlow"* («重生成为诸葛亮，系统（Ebbingflow）助我成就霸业»).
- **Atomic Runtime Restore**: Backend mechanism (`/maintenance/restore-demo-data`) that safely releases database locks (SQLite, Neo4j, Chroma) to allow data replacement without server restart.
- **Visual Feedback**: Added confirmation dialogs and full-screen loading overlays for maintenance operations.

### Changed
- Replaced "Personality Reassessment" (人格重判) button with "Import Demo Data" (导入演示数据) in the Data Monitor header.

## [Unreleased]
- Initial internal development and cognitive infrastructure stabilization.
