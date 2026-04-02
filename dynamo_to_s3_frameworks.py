"""
Pipeline: staging-fusefy-frameworks → S3 (plain text) → Pinecone

Each framework is uploaded as:
  s3://{BUCKET}/frameworks/{record_id}.txt          (plain text for embedding)
  s3://{BUCKET}/frameworks/{record_id}.metadata.json (keyword metadata for Pinecone filtering)

Pinecone approach:
  - First level: keyword-based metadata filtering (framework_id, name, doc_type, categories, regions, etc.)
  - Second level: vector similarity on plain text content for detail matching
  - No markdown formatting — Pinecone embeddings work best with clean plain text
  - Whole field data taken as-is from DynamoDB (no auto-chunking)
"""

import os
import json
import boto3
import time
from dotenv import load_dotenv
from boto3.dynamodb.types import TypeDeserializer

load_dotenv()

REGION = os.getenv("REGION", "us-east-1")
DYNAMODB_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")
S3_BUCKET = os.getenv("S3_BUCKET", "dynamo-to-opensearch-rag-frameworks")
S3_PREFIX = os.getenv("S3_PREFIX", "frameworks")
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID", "GT9B3WZTOE")

dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
bedrock_agent = boto3.client("bedrock-agent", region_name=REGION)
deserializer = TypeDeserializer()


# ───────────────────────────────────────────────────────────────────
# 1. UNMARSHALL DynamoDB JSON → Clean Python dict
# ───────────────────────────────────────────────────────────────────
def unmarshall(dynamo_item: dict) -> dict:
    """Convert DynamoDB typed JSON to plain Python dict."""
    return {key: deserializer.deserialize(value) for key, value in dynamo_item.items()}


# ───────────────────────────────────────────────────────────────────
# 2. BUILD PINECONE METADATA — keyword fields for first-level filtering
# ───────────────────────────────────────────────────────────────────
def build_pinecone_metadata(record: dict) -> dict:
    """
    Build metadata dict for Pinecone filtering.
    These fields enable keyword-based first-level search before
    vector similarity is applied on the text content.
    """
    record_id = record.get("id", "Unknown")
    name = record.get("name", "Unknown")

    # Core keyword fields for first-level filtering
    metadata = {
        "framework_id": record_id,
        "framework_name": name,
        "doc_type": "framework",
    }

    # Owner — filterable keyword
    if record.get("owner"):
        metadata["owner"] = record["owner"]

    # Count
    if record.get("count") is not None:
        metadata["control_count"] = int(record["count"]) if isinstance(record["count"], (int, float)) else str(record["count"])

    # Assessment categories — list of keywords for filtering
    if record.get("assessmentCategory"):
        cats = record["assessmentCategory"]
        if isinstance(cats, list):
            metadata["assessment_categories"] = [str(c) for c in cats]
        else:
            metadata["assessment_categories"] = [str(cats)]

    # Regions — list of keywords for filtering
    if record.get("region"):
        regions = record["region"]
        if isinstance(regions, list):
            metadata["regions"] = [str(r) for r in regions]
        else:
            metadata["regions"] = [str(regions)]

    # Verticals — list of keywords for filtering
    if record.get("verticals"):
        verticals = record["verticals"]
        if isinstance(verticals, list):
            metadata["verticals"] = [str(v) for v in verticals]
        else:
            metadata["verticals"] = [str(verticals)]

    # Search keywords — flattened for keyword matching
    if record.get("searchAttributesAsJson"):
        metadata["search_keywords"] = str(record["searchAttributesAsJson"])

    # Policy documents — presence flag + count for filtering
    if record.get("policyDocuments"):
        docs = record["policyDocuments"]
        if isinstance(docs, list) and docs:
            metadata["has_policy_documents"] = True
            metadata["policy_document_count"] = len(docs)

    # Policy links — presence flag + count for filtering
    if record.get("policyLinks"):
        links = record["policyLinks"]
        if isinstance(links, list) and links:
            metadata["has_policy_links"] = True
            metadata["policy_link_count"] = len(links)

    return metadata


# ───────────────────────────────────────────────────────────────────
# 3. FLATTEN record into PLAIN TEXT for Pinecone embedding
# ───────────────────────────────────────────────────────────────────
def flatten_for_rag(record: dict) -> str:
    """
    Convert an unmarshalled framework record into plain text.
    No markdown formatting — Pinecone works best with clean readable text.
    Each field is presented as 'Label: Value' on its own line.
    """
    lines = []
    record_id = record.get("id", "Unknown")
    name = record.get("name", "Unknown")

    lines.append(f"Framework: {name}")
    lines.append(f"Framework ID: {record_id}")
    lines.append("")

    # Core Fields
    if record.get("description"):
        lines.append(f"Description: {record['description']}")

    if record.get("owner"):
        lines.append(f"Owner: {record['owner']}")

    if record.get("name"):
        lines.append(f"Name: {record['name']}")

    if record.get("count") is not None:
        lines.append(f"Count: {record['count']}")

    # Assessment Categories
    if record.get("assessmentCategory"):
        cats = record["assessmentCategory"]
        if isinstance(cats, list):
            lines.append(f"Assessment Categories: {', '.join(str(c) for c in cats)}")
        else:
            lines.append(f"Assessment Categories: {cats}")

    # Regions
    if record.get("region"):
        regions = record["region"]
        if isinstance(regions, list):
            lines.append(f"Regions: {', '.join(str(r) for r in regions)}")
        else:
            lines.append(f"Regions: {regions}")

    # Verticals
    if record.get("verticals"):
        verticals = record["verticals"]
        if isinstance(verticals, list):
            lines.append(f"Verticals: {', '.join(str(v) for v in verticals)}")
        else:
            lines.append(f"Verticals: {verticals}")

    # Search Attributes
    if record.get("searchAttributesAsJson"):
        lines.append(f"Search Keywords: {record['searchAttributesAsJson']}")

    # Framework Image
    if record.get("frameWorkImgUrl"):
        lines.append(f"Framework Image URL: {record['frameWorkImgUrl']}")

    # Policy Documents
    if record.get("policyDocuments"):
        docs = record["policyDocuments"]
        if isinstance(docs, list) and docs:
            lines.append("")
            lines.append("Policy Documents:")
            for i, doc in enumerate(docs, 1):
                lines.append(f"  {i}. {doc}")

    # Policy Links
    if record.get("policyLinks"):
        links = record["policyLinks"]
        if isinstance(links, list) and links:
            lines.append("")
            lines.append("Policy Links:")
            for link in links:
                lines.append(f"  - {link}")

    # Catch-all for any other fields not explicitly handled
    handled_keys = {
        "id", "name", "description", "owner", "count",
        "assessmentCategory", "region", "verticals",
        "searchAttributesAsJson", "frameWorkImgUrl",
        "policyDocuments", "policyLinks", "createdDate", "updatedDate"
    }
    extra_fields = {k: v for k, v in record.items() if k not in handled_keys and v is not None}
    if extra_fields:
        lines.append("")
        lines.append("Additional Information:")
        for key, val in extra_fields.items():
            if isinstance(val, (list, dict)):
                lines.append(f"  {key}: {json.dumps(val, default=str)}")
            else:
                lines.append(f"  {key}: {val}")

    return "\n".join(lines)


# ───────────────────────────────────────────────────────────────────
# 4. SCAN DynamoDB → Upload to S3
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    """Scan all items from DynamoDB, unmarshall, flatten to plain text, and upload to S3."""
    print(f"\n📖 Scanning DynamoDB table: {DYNAMODB_TABLE}")

    all_items = []
    params = {"TableName": DYNAMODB_TABLE}

    while True:
        response = dynamodb.scan(**params)
        items = response.get("Items", [])
        all_items.extend(items)
        print(f"  Scanned {len(all_items)} items so far...")

        if "LastEvaluatedKey" in response:
            params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break

    print(f"\n✅ Total items scanned: {len(all_items)}")

    if not all_items:
        print("⚠️  No items found in the table. Exiting.")
        return 0

    # Clear existing framework files in S3 prefix (keep other prefixes intact)
    print(f"\n🧹 Clearing existing files in s3://{S3_BUCKET}/{S3_PREFIX}/")
    paginator = s3.get_paginator("list_objects_v2")
    delete_objects = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            delete_objects.append({"Key": obj["Key"]})

    if delete_objects:
        for i in range(0, len(delete_objects), 1000):
            batch = delete_objects[i:i + 1000]
            s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": batch})
        print(f"  Deleted {len(delete_objects)} existing files.")
    else:
        print("  No existing files to delete.")

    # Upload each record as plain text + metadata
    print(f"\n📤 Uploading to s3://{S3_BUCKET}/{S3_PREFIX}/")
    uploaded = 0
    for item in all_items:
        try:
            clean_record = unmarshall(item)
            record_id = clean_record.get("id", f"unknown-{uploaded}")

            # Upload as plain text (optimized for Pinecone vector embedding)
            text_content = flatten_for_rag(clean_record)
            text_key = f"{S3_PREFIX}/{record_id}.txt"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=text_key,
                Body=text_content.encode("utf-8"),
                ContentType="text/plain"
            )

            # Upload metadata file for Pinecone keyword-based filtering
            metadata = build_pinecone_metadata(clean_record)
            metadata_key = f"{S3_PREFIX}/{record_id}.metadata.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2).encode("utf-8"),
                ContentType="application/json"
            )

            fw_name = clean_record.get("name", "Unknown")
            uploaded += 1
            print(f"  ✅ {record_id} — {fw_name} ({len(text_content)} chars) — metadata uploaded")

        except Exception as e:
            record_id = "unknown"
            try:
                record_id = item.get("id", {}).get("S", "unknown")
            except Exception:
                pass
            print(f"  ❌ Failed: {record_id} — {e}")

    print(f"\n✅ Uploaded {uploaded}/{len(all_items)} records to S3.")
    return uploaded


# ───────────────────────────────────────────────────────────────────
# 5. SYNC Bedrock Knowledge Base
# ───────────────────────────────────────────────────────────────────
def sync_knowledge_base():
    """Start ingestion job and wait for completion."""
    print(f"\n🔄 Starting Bedrock KB sync (KB: {KNOWLEDGE_BASE_ID}, DS: {DATA_SOURCE_ID})")

    response = bedrock_agent.start_ingestion_job(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        dataSourceId=DATA_SOURCE_ID
    )

    job_id = response["ingestionJob"]["ingestionJobId"]
    print(f"  Ingestion Job ID: {job_id}")

    while True:
        job = bedrock_agent.get_ingestion_job(
            knowledgeBaseId=KNOWLEDGE_BASE_ID,
            dataSourceId=DATA_SOURCE_ID,
            ingestionJobId=job_id
        )

        status = job["ingestionJob"]["status"]
        stats = job["ingestionJob"]["statistics"]
        print(
            f"  Status: {status} | "
            f"Scanned: {stats['numberOfDocumentsScanned']} | "
            f"Indexed: {stats['numberOfNewDocumentsIndexed']} | "
            f"Modified: {stats['numberOfModifiedDocumentsIndexed']} | "
            f"Failed: {stats['numberOfDocumentsFailed']}"
        )

        if status in ["COMPLETE", "FAILED", "STOPPED"]:
            break

        time.sleep(5)

    if status == "COMPLETE":
        total = stats["numberOfNewDocumentsIndexed"] + stats["numberOfModifiedDocumentsIndexed"]
        print(f"\n✅ KB sync complete! {total} documents indexed.")
    else:
        print(f"\n❌ KB sync {status}. Check the Bedrock console for details.")

    return status


# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  DynamoDB → S3 (Plain Text) → Pinecone Pipeline")
    print(f"  Table: {DYNAMODB_TABLE}")
    print(f"  S3:    s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"  KB:    {KNOWLEDGE_BASE_ID}")
    print("=" * 60)

    count = scan_and_upload()

    if count and count > 0:
        status = sync_knowledge_base()

        if status == "COMPLETE":
            print("\n🎉 Pipeline complete! Run 'python query_kb.py' to query.")
        else:
            print("\n⚠️  Sync did not complete successfully. Check AWS console.")
    else:
        print("\n⚠️  No records to sync.")
