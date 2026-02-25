import os
import re
import boto3
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

REGION = os.getenv("REGION", "us-east-1")
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not KNOWLEDGE_BASE_ID:
    raise EnvironmentError(
        "Set KNOWLEDGE_BASE_ID env var. "
        "Find it in the Bedrock console under Knowledge Bases → frameworks-kb."
    )

if not OPENAI_API_KEY:
    raise EnvironmentError("Set OPENAI_API_KEY env var.")

bedrock_agent = boto3.client("bedrock-agent-runtime", region_name=REGION)
openai_client = OpenAI(api_key=OPENAI_API_KEY)


# ───────────────────────────────────────────────────────────────────
# HELPERS — Extract IDs from query
# ───────────────────────────────────────────────────────────────────
def extract_framework_id(query: str) -> str | None:
    """Extract a framework ID pattern like AI-ADF-013 from the query."""
    match = re.search(r'(AI-[A-Z]+-\d+)', query, re.IGNORECASE)
    return match.group(1).upper() if match else None


def extract_control_id(query: str) -> str | None:
    """Extract a control ID pattern like AI-CTRL-00001 from the query."""
    match = re.search(r'(AI-CTRL-\d+)', query, re.IGNORECASE)
    return match.group(1).upper() if match else None


def is_usecase_query(query: str) -> bool:
    """Detect whether the query is about a use case / use-case assessment."""
    usecase_keywords = [
        "use case", "usecase", "use-case", "assessment",
        "model name", "modelname", "ai project", "inventory",
        "design document", "tco", "rollout", "jira stories",
        "bill of material", "bom", "risk and controls",
        "overall risk", "ai category"
    ]
    q_lower = query.lower()
    return any(kw in q_lower for kw in usecase_keywords)


# ── Query-based token budget ────────────────────────────────────────
# Maps query intent to (summary_max_tokens, answer_max_tokens) so we
# spend only the tokens the question actually needs.
_QUERY_TOKEN_PROFILES: list[tuple[list[str], int, int]] = [
    # Narrow / single-field look-ups → small summary, short answer
    (["overall risk", "risk level", "ai category", "status",
      "department", "sector", "vendor", "priority",
      "cloud provider", "platform"], 500, 1000),
    # Medium-depth questions → moderate summary & answer
    (["list controls", "list frameworks", "jira stories",
      "acceptance criteria", "metrics", "rollout",
      "epics", "data labels", "bill of material", "bom",
      "summary", "describe"], 1500, 4000),
    # Deep / multi-section questions → larger budget
    (["design document", "tco", "cost", "architecture",
      "api design", "security", "compliance",
      "risk and controls", "full detail", "everything"], 3000, 8000),
]


def _token_budget_for_query(query: str) -> tuple[int, int]:
    """
    Return (summary_max_tokens, answer_max_tokens) tuned to the query intent.
    Heavier questions get a larger budget; simple look-ups get a small one.
    """
    q_lower = query.lower()
    # Walk profiles from narrowest → widest; last match wins (deepest)
    summary_tok, answer_tok = 1500, 4000          # sensible default
    for keywords, s_tok, a_tok in _QUERY_TOKEN_PROFILES:
        if any(kw in q_lower for kw in keywords):
            summary_tok, answer_tok = s_tok, a_tok  # keep scanning
    return summary_tok, answer_tok


# ───────────────────────────────────────────────────────────────────
# 1. RETRIEVE ONLY — Get relevant chunks from Bedrock KB
# ───────────────────────────────────────────────────────────────────
def retrieve(query: str, top_k: int = 5) -> list[dict]:
    """
    Retrieve relevant chunks from the Knowledge Base.
    Uses HYBRID search (semantic + keyword) for better exact-ID matching.
    Applies STRICT metadata filtering — no cross-contamination.
    When a framework ID is detected, only docs matching that exact
    framework_id or having it in framework_ids_associated are returned.
    """
    retrieval_config = {
        "vectorSearchConfiguration": {
            "numberOfResults": top_k,
            "overrideSearchType": "HYBRID"
        }
    }

    # ── Detect IDs and apply strict metadata filters ──
    fw_id = extract_framework_id(query)
    ctrl_id = extract_control_id(query)

    if fw_id:
        # Strict: only docs where framework_id matches OR framework_ids_associated contains it
        retrieval_config["vectorSearchConfiguration"]["filter"] = {
            "orAll": [
                {"equals": {"key": "framework_id", "value": fw_id}},
                {"listContains": {"key": "framework_ids_associated", "value": fw_id}},
                {"listContains": {"key": "control_ids_associated", "value": fw_id}}
            ]
        }
    elif ctrl_id:
        # Strict: only docs where control_id matches OR control_ids_associated contains it
        retrieval_config["vectorSearchConfiguration"]["filter"] = {
            "orAll": [
                {"equals": {"key": "control_id", "value": ctrl_id}},
                {"listContains": {"key": "control_ids_associated", "value": ctrl_id}}
            ]
        }

    response = bedrock_agent.retrieve(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        retrievalQuery={"text": query},
        retrievalConfiguration=retrieval_config
    )

    results = []
    for item in response.get("retrievalResults", []):
        results.append({
            "content": item.get("content", {}).get("text", ""),
            "score": item.get("score", 0),
            "source": item.get("location", {}).get("s3Location", {}).get("uri", "N/A")
        })

    return results


# ───────────────────────────────────────────────────────────────────
# 2a. SUMMARISE A SINGLE CHUNK — used for large use-case documents
# ───────────────────────────────────────────────────────────────────
def summarize_content(content: str, query: str, max_tokens: int = 1500) -> str:
    """
    Summarise a large retrieved chunk (e.g. a full use-case markdown)
    into a concise, query-focused summary before handing it to the
    final answer-generation step.  Token budget is driven by the query
    complexity so simple look-ups stay cheap.
    """
    response = openai_client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a precise summarisation assistant. "
                    "Given a large AI-governance / use-case document and the "
                    "user's question, produce a **concise summary** that "
                    "retains ONLY the facts and details relevant to "
                    "answering the question.\n\n"
                    "Rules:\n"
                    "- NEVER expose raw internal IDs (e.g. AI-ADF-013, AI-CTRL-00001, "
                    "inventory IDs, Jira issue keys). Instead, refer to items by their "
                    "descriptive name, title, or purpose. For example, say "
                    "'the EU AI Act Compliance framework' instead of 'AI-ADF-013'.\n"
                    "- Keep names, numbers, and dates verbatim.\n"
                    "- When summarising Jira stories, ALWAYS include the associated \n"
                    "  gaps / sub-tasks for each story (gap name and description). \n"
                    "  A story without its gaps is incomplete.\n"
                    "- Drop sections that have no bearing on the question.\n"
                    "- Do NOT add information that is not in the document.\n"
                    "- Output plain text (no markdown headers).\n"
                    "- Be as brief as the question allows."
                )
            },
            {
                "role": "user",
                "content": (
                    f"Question the user will ask next:\n{query}\n\n"
                    f"--- DOCUMENT START ---\n{content}\n--- DOCUMENT END ---"
                )
            }
        ],
        temperature=0.0,
        max_tokens=max_tokens
    )
    return response.choices[0].message.content


# ───────────────────────────────────────────────────────────────────
# 2b. RETRIEVE + OPENAI GENERATE — Full RAG pipeline
# ───────────────────────────────────────────────────────────────────
def retrieve_and_generate(query: str, top_k: int = 5) -> dict:
    """
    Full RAG pipeline: retrieve chunks from Bedrock KB → generate answer via OpenAI.
    Strict metadata filtering ensures zero cross-contamination.
    Returns the generated answer, source citations, and retrieval scores.
    """
    # Step 1: Retrieve chunks (metadata filter already applied in retrieve())
    chunks = retrieve(query, top_k=top_k)

    if not chunks:
        return {
            "answer": "No relevant information found in the knowledge base.",
            "sources": [], "chunks_used": 0,
            "retrieval_scores": [], "mean_score": 0,
            "source_match": False
        }

    # Step 2: Compute query-aware token budgets
    summary_budget, answer_budget = _token_budget_for_query(query)

    # Step 3: Build context — for use-case queries, summarise chunk 1 only
    if is_usecase_query(query):
        top_chunk = chunks[0]
        summarised = summarize_content(
            top_chunk["content"], query, max_tokens=summary_budget
        )
        context = f"[Source: {top_chunk['source']}]\n{summarised}"
        # Narrow sources to the single chunk actually used
        chunks = [top_chunk]
    else:
        context = "\n\n---\n\n".join(
            f"[Source: {chunk['source']}]\n{chunk['content']}" for chunk in chunks
        )

    
    print("Context: ",context)
    # Step 4: Generate answer using OpenAI (budget tuned to query)
    response = openai_client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful AI assistant answering questions about AI governance frameworks, "
                    "risk controls, and AI inventory records.\n\n"
                    "Use ONLY the information from the provided context to answer. "
                    "If the context doesn't contain enough information, say so clearly.\n\n"
                    "CRITICAL FORMATTING RULES (these apply to EVERY answer):\n"
                    "- NEVER show raw internal IDs such as framework IDs (AI-ADF-013), "
                    "control IDs (AI-CTRL-00001), inventory IDs, use-case IDs, "
                    "component IDs, or Jira issue keys.\n"
                    "- Instead, always refer to items by their human-readable name, "
                    "title, or description. For example write "
                    "'the EU AI Act Compliance framework' rather than 'AI-ADF-013', "
                    "or 'the Bias Detection control' rather than 'AI-CTRL-00042'.\n"
                    "- When mentioning Jira stories, ALWAYS include the associated \n"
                    "  gaps / sub-tasks for each story (gap name and description). \n"
                    "  Present each story together with its gaps so the user sees \n"
                    "  what is missing or needs to be addressed.\n"
                    "- Write in clear, complete sentences suitable for business users.\n"
                    "- Do NOT guess or infer information that is not in the provided documents."
                )
            },
            {
                "role": "user",
                "content": f"Context:\n{context}\n\nQuestion: {query}"
            }
        ],
        temperature=0.1,
        max_tokens=answer_budget
    )

    # Compute retrieval accuracy metrics
    retrieval_scores = [chunk["score"] for chunk in chunks]
    mean_score = sum(retrieval_scores) / len(retrieval_scores) if retrieval_scores else 0

    # Check source match — verify only the correct ID appears in sources
    fw_id = extract_framework_id(query)
    ctrl_id = extract_control_id(query)
    target_id = fw_id or ctrl_id or ""
    sources = [chunk["source"] for chunk in chunks]
    source_match = all(target_id in s for s in sources) if target_id else True

    return {
        "answer": response.choices[0].message.content,
        "sources": sources,
        "chunks_used": len(chunks),
        "retrieval_scores": [round(s, 4) for s in retrieval_scores],
        "mean_score": round(mean_score, 4),
        "source_match": source_match
    }


# ───────────────────────────────────────────────────────────────────
# 3. INTERACTIVE CHAT LOOP
# ───────────────────────────────────────────────────────────────────
def chat():
    """Interactive loop to query the Knowledge Base."""
    print("\n" + "=" * 60)
    print("  Bedrock KB + OpenAI RAG Chat")
    print("=" * 60)
    print("Commands: 'quit' to exit | 'retrieve' to toggle retrieve-only mode\n")

    retrieve_only = False

    while True:
        query = input("Your question: ").strip()

        if not query:
            continue
        if query.lower() == "quit":
            print("Goodbye!")
            break
        if query.lower() == "retrieve":
            retrieve_only = not retrieve_only
            mode = "RETRIEVE ONLY" if retrieve_only else "RETRIEVE + GENERATE"
            print(f"  → Switched to {mode} mode\n")
            continue

        try:
            if retrieve_only:
                print("\nRetrieving chunks...\n")
                results = retrieve(query)
                for i, r in enumerate(results, 1):
                    print(f"--- Chunk {i} (score: {r['score']:.4f}) ---")
                    print(f"Source: {r['source']}")
                    print(f"{r['content'][:500]}...")
                    print()
            else:
                print("\nGenerating answer...\n")
                result = retrieve_and_generate(query)
                print(f"Answer:\n{result['answer']}\n")
                print(f"Sources ({result['chunks_used']} chunks used):")
                for src in set(result["sources"]):
                    print(f"  • {src}")
                print()
                # Show retrieval accuracy metrics
                print("--- Retrieval Metrics ---")
                print(f"  Mean Retrieval Score : {result.get('mean_score', 'N/A')}")
                print(f"  Individual Scores    : {result.get('retrieval_scores', [])}")
                print(f"  Source Match         : {'Yes' if result.get('source_match') else 'No'}")
                print()

        except Exception as e:
            print(f"Error: {e}\n")


# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    chat()