import os
import json
import gzip
import logging
from typing import Any

import boto3
import openai
from dotenv import load_dotenv
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth
from sentence_transformers import SentenceTransformer
from openai import OpenAI


load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise EnvironmentError("Missing OPENAI_API_KEY in environment variables or .env file.")
openai.api_key = OPENAI_API_KEY



def get_openai_client():
    """
    Create an OpenAI client with API key from Secrets Manager.
    
    Returns:
        OpenAI client or None if key not available
    """
    try:
        api_key = OPENAI_API_KEY
        if api_key:
            return OpenAI(api_key=api_key)
    except Exception as e:
        print(f"⚠️ Failed to get OpenAI API key from Secrets Manager: {e}")
    
    # Fallback to environment variable
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key:
        return OpenAI(api_key=api_key)
    
    return None


# ── Logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────
S3_BUCKET  = os.getenv("S3_BUCKET")
S3_PREFIX  = os.getenv("S3_PREFIX")
OS_HOST    = os.getenv("OS_HOST")
INDEX_NAME = os.getenv("INDEX_NAME")
REGION     = os.getenv("REGION")
PK_FIELD   = os.getenv("PK_FIELD", "id")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "25"))

# ── Validate Required Env Vars ──────────────────────────────────────
REQUIRED_ENV_VARS = ["S3_BUCKET", "S3_PREFIX", "OS_HOST", "INDEX_NAME", "REGION"]
missing_vars = [var for var in REQUIRED_ENV_VARS if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# ── BGE-M3 Embedding Model (lazy loaded) ────────────────────────────
embed_model: SentenceTransformer | None = None


def get_embed_model() -> SentenceTransformer:
    """Lazy load the BGE-M3 embedding model."""
    global embed_model
    if embed_model is None:
        logger.info("Loading BGE-M3 embedding model...")
        embed_model = SentenceTransformer("BAAI/bge-m3")
        logger.info("BGE-M3 model loaded.")
    return embed_model


# ── AWS Clients ─────────────────────────────────────────────────────
s3_client = boto3.client("s3", region_name=REGION)
bedrock_client = boto3.client("bedrock-runtime", region_name=REGION)

session = boto3.Session()
creds = session.get_credentials()
if creds is None:
    raise EnvironmentError("AWS credentials not found. Configure AWS credentials.")
frozen_creds = creds.get_frozen_credentials()

awsauth = AWS4Auth(
    frozen_creds.access_key,
    frozen_creds.secret_key,
    REGION,
    "es",
    session_token=frozen_creds.token
)

os_client = OpenSearch(
    hosts=[{"host": OS_HOST, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=60
)

# ───────────────────────────────────────────────────────────────────
# 1. FLATTEN DYNAMODB JSON
# ───────────────────────────────────────────────────────────────────
def flatten_dynamo_value(value: dict[str, Any]) -> Any:
    """
    Recursively converts a single DynamoDB typed value to Python native type.
    """
    if not value:
        return None
    
    dtype, dval = next(iter(value.items()))
    
    if dtype == "S":  # String
        return dval
    elif dtype == "N":  # Number
        return float(dval) if "." in dval else int(dval)
    elif dtype == "BOOL":  # Boolean
        return dval
    elif dtype == "NULL":  # Null
        return None
    elif dtype == "L":  # List
        return [flatten_dynamo_value(item) for item in dval]
    elif dtype == "M":  # Map
        return {k: flatten_dynamo_value(v) for k, v in dval.items()}
    elif dtype == "SS":  # String Set
        return set(dval)
    elif dtype == "NS":  # Number Set
        return {float(n) if "." in n else int(n) for n in dval}
    elif dtype == "BS":  # Binary Set
        return set(dval)
    elif dtype == "B":  # Binary
        return dval
    else:
        return dval


def flatten_dynamo_item(item: dict[str, Any]) -> dict[str, Any]:
    """
    Converts DynamoDB JSON format:
      {"name": {"S": "John"}, "age": {"N": "30"}}
    To plain dict:
      {"name": "John", "age": 30}
    """
    return {key: flatten_dynamo_value(value) for key, value in item.items()}

# ───────────────────────────────────────────────────────────────────
# 2. BUILD TEXT TO EMBED
# ───────────────────────────────────────────────────────────────────
def build_text(record: dict[str, Any]) -> str:
    """
    Converts record fields into a single string for embedding.
    Customize this based on which fields matter for your RAG search.
    """
    parts = []
    for k, v in record.items():
        if v is not None:
            # Convert non-string values to string representation
            str_val = str(v) if not isinstance(v, str) else v
            parts.append(f"{k}: {str_val}")
    return " | ".join(parts)[:8192]  # BGE-M3 supports up to 8192 tokens

# ───────────────────────────────────────────────────────────────────
# 3. GENERATE EMBEDDINGS
# ───────────────────────────────────────────────────────────────────
def get_embedding(text: str) -> list[float] | None:
    """
    Uses BGE-M3 model to generate embeddings.
    Returns list of floats (1024 dimensions).
    """
    try:
        model = get_embed_model()
        embedding = model.encode(text, normalize_embeddings=True)
        return embedding.tolist()
    except Exception as e:
        logger.warning(f"Embedding error: {e}")
        return None

# ───────────────────────────────────────────────────────────────────
# 4. CREATE OPENSEARCH INDEX
# ───────────────────────────────────────────────────────────────────
def create_index() -> None:
    if os_client.indices.exists(index=INDEX_NAME):
        logger.info(f"Index '{INDEX_NAME}' already exists - skipping creation")
        return

    os_client.indices.create(
        index=INDEX_NAME,
        body={
            "settings": {
                "index.knn": True,
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "id":        {"type": "keyword"},
                    "text":      {"type": "text"},
                    "embedding": {
                        "type":      "knn_vector",
                        "dimension": 1024,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "faiss",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 16
                            }
                        }
                    },
                    "metadata":  {"type": "object", "dynamic": True}
                }
            }
        }
    )
    logger.info(f"Index '{INDEX_NAME}' created")

# ───────────────────────────────────────────────────────────────────
# 5. BULK INDEX TO OPENSEARCH
# ───────────────────────────────────────────────────────────────────
def bulk_index(docs: list[dict[str, Any]]) -> tuple[int, int]:
    actions = [
        {
            "_index":  INDEX_NAME,
            "_id":     doc["id"],
            "_source": {
                "text":      doc["text"],
                "embedding": doc["embedding"],
                "metadata":  doc["metadata"]
            }
        }
        for doc in docs
    ]
    success, failed = helpers.bulk(os_client, actions, raise_on_error=False)
    logger.info(f"Indexed: {success} | Failed: {len(failed)}")
    return success, len(failed)

# ───────────────────────────────────────────────────────────────────
# 6. READ S3 EXPORT & PROCESS
# ───────────────────────────────────────────────────────────────────
def process_and_index() -> None:
    logger.info(f"Reading from s3://{S3_BUCKET}/{S3_PREFIX}")

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX)

    total_files = 0
    total_docs = 0
    batch: list[dict[str, Any]] = []

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json.gz"):
                continue

            total_files += 1
            logger.info(f"Processing file {total_files}: {key}")

            try:
                s3_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)

                with gzip.open(s3_obj["Body"], "rt", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue

                        raw = json.loads(line)
                        item = flatten_dynamo_item(raw.get("Item", {}))

                        if not item:
                            continue

                        text = build_text(item)
                        if not text:
                            continue

                        embedding = get_embedding(text)
                        if not embedding:
                            continue

                        doc_id = str(item.get(PK_FIELD) or total_docs)

                        batch.append({
                            "id":        doc_id,
                            "text":      text,
                            "embedding": embedding,
                            "metadata":  item
                        })

                        # Bulk index when batch is full
                        if len(batch) >= BATCH_SIZE:
                            bulk_index(batch)
                            total_docs += len(batch)
                            logger.info(f"Total indexed: {total_docs}")
                            batch = []

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in {key}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing {key}: {e}")
                continue

    # Index remaining docs
    if batch:
        bulk_index(batch)
        total_docs += len(batch)

    logger.info(f"Indexing complete! Files: {total_files} | Docs: {total_docs}")

# ───────────────────────────────────────────────────────────────────
# 7. RAG SEARCH
# ───────────────────────────────────────────────────────────────────
def rag_search(query: str, top_k: int = 5) -> list[dict[str, Any]]:
    """
    1. Embed the query
    2. Vector search OpenSearch
    3. Return top-k matching chunks
    """
    logger.info(f"Query: {query}")

    query_embedding = get_embedding(query)
    if not query_embedding:
        logger.error("Failed to generate query embedding")
        return []

    response = os_client.search(
        index=INDEX_NAME,
        body={
            "size": top_k,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_embedding,
                        "k": top_k
                    }
                }
            },
            "_source": ["text", "metadata"]
        }
    )

    hits = response["hits"]["hits"]
    logger.info(f"Found {len(hits)} results")

    results = []
    for i, hit in enumerate(hits, 1):
        logger.debug(f"[{i}] Score: {hit['_score']:.4f}")
        results.append({
            "score": hit["_score"],
            "text": hit["_source"]["text"],
            "metadata": hit["_source"]["metadata"]
        })

    return results

# ───────────────────────────────────────────────────────────────────
# 8. GENERATE ANSWER WITH BEDROCK (RAG)
# ───────────────────────────────────────────────────────────────────
def generate_answer(query: str, context_results: list[dict[str, Any]]) -> str | None:
    """
    Pass retrieved chunks as context to Claude via Bedrock
    and generate a final answer.
    """
    if not context_results:
        return "No relevant context found."

    context = "\n\n".join([r["text"] for r in context_results])

    prompt = f"""You are a helpful assistant. Use the following context to answer the question.
If the answer is not in the context, say "I don't have enough information to answer this."

Context:
{context}

Question: {query}

Answer:"""

    try:
        client = get_openai_client()
        response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=2000
            )
            
            
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.error(f"Answer generation error: {e}")
        return None

# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────
def main() -> None:
    # Step 1 — Create index and load data
    logger.info("=" * 60)
    logger.info("STEP 1: Creating Index & Indexing Data")
    logger.info("=" * 60)
    create_index()
    process_and_index()

    # Step 2 — RAG Search + Answer
    logger.info("=" * 60)
    logger.info("STEP 2: RAG Search")
    logger.info("=" * 60)

    # Change this to your actual query
    query = "Can you list the frameworks available in the dataset and their descriptions?"

    results = rag_search(query, top_k=5)
    answer = generate_answer(query, results)

    logger.info("=" * 60)
    logger.info("ANSWER")
    logger.info("=" * 60)
    logger.info(answer)


if __name__ == "__main__":
    main()