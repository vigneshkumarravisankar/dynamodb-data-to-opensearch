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
# 2. RETRIEVE + OPENAI GENERATE — Full RAG pipeline
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

    # Step 2: Build context from retrieved chunks
    context = "\n\n---\n\n".join(
        f"[Source: {chunk['source']}]\n{chunk['content']}" for chunk in chunks
    )

    # Step 3: Generate answer using OpenAI
    response = openai_client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful AI assistant answering questions about AI governance frameworks, "
                    "risk controls, and AI inventory records.\n\n"
                    "Use ONLY the information from the provided context to answer. "
                    "If the context doesn't contain enough information, say so clearly. "
                    "Cite the sources when possible.\n\n"
                    "IMPORTANT: Only reference IDs and data explicitly present in the context. "
                    "Do NOT guess or infer IDs that are not in the provided documents."
                )
            },
            {
                "role": "user",
                "content": f"Context:\n{context}\n\nQuestion: {query}"
            }
        ],
        temperature=0.1,
        max_tokens=10000
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