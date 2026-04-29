from dataclasses import replace

from memory.knowledge_engine import KnowledgeBaseEngine
from memory.scoring.hybrid_scorer import HybridScorer, ScoredCandidate


def _candidate(content: str, source_type: str) -> ScoredCandidate:
    return ScoredCandidate(
        content=content,
        speaker="tester",
        timestamp="",
        source_type=source_type,
        source_name=source_type,
        semantic_score=1.0,
        graph_hop_score=1.0,
        impact_score=9.0,
    )


def test_query_intent_router_classifies_fact_summary_and_long_term():
    assert KnowledgeBaseEngine.infer_query_intent("我上次说预算多少？") == "fact"
    assert KnowledgeBaseEngine.infer_query_intent("最近我们聊了什么？") == "summary"
    assert KnowledgeBaseEngine.infer_query_intent("opensales 这个项目长期进展如何？") == "long_term"
    assert KnowledgeBaseEngine.infer_query_intent("帮我想想") == "semantic"


def test_query_intent_router_prefers_fact_when_markers_conflict():
    assert KnowledgeBaseEngine.infer_query_intent("最近预算多少？") == "fact"
    assert KnowledgeBaseEngine.infer_query_intent("去年那个长期项目花了多少？") == "fact"


def test_bare_numbers_do_not_force_fact_intent():
    assert KnowledgeBaseEngine.infer_query_intent("我说过 3 次了") == "semantic"
    assert KnowledgeBaseEngine.infer_query_intent("3 mood swings") == "semantic"
    assert KnowledgeBaseEngine.infer_query_intent("这个花了 300 元吗") == "fact"
    assert KnowledgeBaseEngine.infer_query_intent("长度 3 m") == "fact"


def test_fact_queries_prefer_evidence_over_narrative_summaries():
    scorer = HybridScorer()
    candidates = [
        _candidate("graph fact", "graph"),
        _candidate("episode summary", "episode"),
        _candidate("saga summary", "saga"),
    ]

    scored = scorer.score([replace(c) for c in candidates], top_k=2, query_intent="fact")

    assert [c.source_type for c in scored] == ["graph"]


def test_fact_queries_strictly_exclude_zero_budget_narrative_when_no_evidence():
    scorer = HybridScorer()
    candidates = [
        _candidate("episode summary", "episode"),
        _candidate("saga summary", "saga"),
    ]

    scored = scorer.score([replace(c) for c in candidates], top_k=2, query_intent="fact")

    assert scored == []


def test_summary_and_long_term_queries_can_promote_narrative_layers():
    scorer = HybridScorer()
    candidates = [
        _candidate("graph fact", "graph"),
        _candidate("episode summary", "episode"),
        _candidate("saga summary", "saga"),
    ]

    summary_scored = scorer.score([replace(c) for c in candidates], top_k=2, query_intent="summary")
    long_term_scored = scorer.score([replace(c) for c in candidates], top_k=2, query_intent="long_term")

    assert summary_scored[0].source_type == "episode"
    assert long_term_scored[0].source_type == "saga"
