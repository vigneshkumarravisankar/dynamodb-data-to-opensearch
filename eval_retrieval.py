"""
Evaluation script for Bedrock KB retrieval accuracy and citation quality.

Metrics:
  - Source Recall:         Did the KB retrieve the correct document?
  - Content Relevance:     Do retrieved chunks contain expected keywords?
  - Mean Retrieval Score:  Bedrock's confidence in the match (0-1)
  - Answer Faithfulness:   Does the LLM answer contain expected information?
  - Cross-Contamination:   Were wrong framework/control docs retrieved?
  - Purity Score:          % of sources belonging to the correct ID only
  - Answer Contamination:  Does the LLM answer mention wrong IDs?
"""

import json
from query_kb import retrieve, retrieve_and_generate


# ── Ground-truth test cases ─────────────────────────────────────────
EVAL_CASES = [
    {
        "query": "List controls attached to AI-ADF-013",
        "expected_sources": ["AI-ADF-013"],
        "must_contain": ["AI-ADF-013"],
        "must_not_contain_sources": ["AI-ADF-024", "AI-ADF-016", "AI-ADF-015", "AI-ADF-018", "AI-ADF-023"],
    },
    {
        "query": "List controls attached to AI-ADF-024",
        "expected_sources": ["AI-ADF-024"],
        "must_contain": ["AI-ADF-024"],
        "must_not_contain_sources": ["AI-ADF-013", "AI-ADF-016", "AI-ADF-015"],
    },
    {
        "query": "List controls attached to AI-ADF-016",
        "expected_sources": ["AI-ADF-016"],
        "must_contain": ["AI-ADF-016"],
        "must_not_contain_sources": ["AI-ADF-013", "AI-ADF-024", "AI-ADF-015"],
    },
    {
        "query": "What frameworks are available?",
        "expected_sources": ["framework"],
        "must_contain": ["Framework"],
        "must_not_contain_sources": [],
    },
    {
        "query": "Show controls at AI Maturity Level 3",
        "expected_sources": ["controls/"],
        "must_contain": ["Level 3"],
        "must_not_contain_sources": [],
    },
]


def evaluate():
    """Run all eval cases and compute retrieval + generation + contamination metrics."""
    results = []

    for case in EVAL_CASES:
        query = case["query"]
        expected = case["expected_sources"]
        must_contain = case["must_contain"]
        must_not_contain = case.get("must_not_contain_sources", [])

        print(f"\n{'='*60}")
        print(f"Query: {query}")

        # ── Retrieve chunks ────────────────────────────────────────
        chunks = retrieve(query, top_k=10)

        # ── Metric 1: Source Recall ────────────────────────────────
        retrieved_sources = [c["source"] for c in chunks]
        hits = sum(
            1 for exp_src in expected
            if any(exp_src in src for src in retrieved_sources)
        )
        source_recall = hits / len(expected) if expected else 0

        # ── Metric 2: Content Relevance (keyword check) ───────────
        all_content = " ".join(c["content"] for c in chunks)
        keyword_hits = sum(
            1 for kw in must_contain if kw.lower() in all_content.lower()
        )
        content_relevance = keyword_hits / len(must_contain) if must_contain else 0

        # ── Metric 3: Mean Retrieval Score ─────────────────────────
        scores = [c["score"] for c in chunks]
        mean_score = sum(scores) / len(scores) if scores else 0

        # ── Metric 4: Cross-Contamination Check ───────────────────
        contaminated_sources = []
        for bad_id in must_not_contain:
            for src in retrieved_sources:
                if bad_id in src:
                    contaminated_sources.append(src)
        cross_contamination = len(contaminated_sources) > 0
        purity_score = 1.0 - (len(contaminated_sources) / len(retrieved_sources)) if retrieved_sources else 1.0

        # ── Metric 5: Answer Faithfulness ──────────────────────────
        rag_result = retrieve_and_generate(query, top_k=10)
        answer = rag_result["answer"]
        answer_keyword_hits = sum(
            1 for kw in must_contain if kw.lower() in answer.lower()
        )
        answer_faithfulness = answer_keyword_hits / len(must_contain) if must_contain else 0

        # ── Metric 6: Answer Cross-Contamination ──────────────────
        answer_contamination = [
            bad_id for bad_id in must_not_contain
            if bad_id.lower() in answer.lower()
        ]

        result = {
            "query": query,
            "source_recall": source_recall,
            "content_relevance": content_relevance,
            "mean_retrieval_score": round(mean_score, 4),
            "answer_faithfulness": answer_faithfulness,
            "cross_contamination": cross_contamination,
            "purity_score": round(purity_score, 4),
            "contaminated_sources": contaminated_sources,
            "answer_contamination": answer_contamination,
            "rag_retrieval_scores": rag_result.get("retrieval_scores", []),
            "rag_mean_score": rag_result.get("mean_score", 0),
            "rag_source_match": rag_result.get("source_match", False),
            "sources_retrieved": retrieved_sources,
            "chunks_count": len(chunks),
        }
        results.append(result)

        ok = "✅" if not cross_contamination else "❌"
        print(f"  {ok} Source Recall:        {source_recall:.0%}")
        print(f"  {ok} Content Relevance:    {content_relevance:.0%}")
        print(f"     Mean Retrieval Score: {mean_score:.4f}")
        print(f"  {ok} Answer Faithfulness:  {answer_faithfulness:.0%}")
        print(f"  {'✅' if not cross_contamination else '❌'} Cross-Contamination: {'NONE' if not cross_contamination else contaminated_sources}")
        print(f"     Purity Score:        {purity_score:.0%}")
        print(f"  {'✅' if not answer_contamination else '❌'} Answer Contamination: {'NONE' if not answer_contamination else answer_contamination}")
        print(f"     Sources: {retrieved_sources}")

    # ── Summary ────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("EVALUATION SUMMARY")
    print(f"{'='*60}")

    n = len(results)
    avg_recall = sum(r["source_recall"] for r in results) / n
    avg_relevance = sum(r["content_relevance"] for r in results) / n
    avg_score = sum(r["mean_retrieval_score"] for r in results) / n
    avg_faith = sum(r["answer_faithfulness"] for r in results) / n
    avg_purity = sum(r["purity_score"] for r in results) / n
    contamination_rate = sum(1 for r in results if r["cross_contamination"]) / n

    print(f"  Avg Source Recall:        {avg_recall:.0%}")
    print(f"  Avg Content Relevance:    {avg_relevance:.0%}")
    print(f"  Avg Retrieval Score:      {avg_score:.4f}")
    print(f"  Avg Answer Faithfulness:  {avg_faith:.0%}")
    print(f"  Avg Purity Score:         {avg_purity:.0%}")
    print(f"  Contamination Rate:       {contamination_rate:.0%}")

    # ── Save ───────────────────────────────────────────────────────
    with open("eval_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to eval_results.json")


if __name__ == "__main__":
    evaluate()
