import os
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
# 1. RETRIEVE ONLY — Get relevant chunks from Bedrock KB
# ───────────────────────────────────────────────────────────────────
def retrieve(query: str, top_k: int = 5) -> list[dict]:
    """
    Retrieve relevant chunks from the Knowledge Base (no LLM answer).
    Useful for inspecting what the KB returns.
    """
    response = bedrock_agent.retrieve(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        retrievalQuery={"text": query},
        retrievalConfiguration={
            "vectorSearchConfiguration": {
                "numberOfResults": top_k
            }
        }
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
    Returns the generated answer and source citations.
    """
    # Step 1: Retrieve relevant chunks from Bedrock KB
    chunks = retrieve(query, top_k)

    if not chunks:
        return {"answer": "No relevant information found in the knowledge base.", "sources": [], "chunks_used": 0}

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
                    "Cite the sources when possible."
                )
            },
            {
                "role": "user",
                "content": f"Context:\n{context}\n\nQuestion: {query}"
            }
        ],
        temperature=0.2,
        max_tokens=10000
    )

    return {
        "answer": response.choices[0].message.content,
        "sources": [chunk["source"] for chunk in chunks],
        "chunks_used": len(chunks)
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

        except Exception as e:
            print(f"Error: {e}\n")


# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    chat()