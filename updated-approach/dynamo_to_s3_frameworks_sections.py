"""
Pipeline: staging-fusefy-frameworks → S3 (plain text, section-based) → Pinecone

Each framework is split into individual section files:

  s3://{BUCKET}/frameworks-v2/{frameworkId}/
      ├── fw_01_overview.txt              + .metadata.json
      └── fw_02_policy_references.txt     + .metadata.json

Pinecone approach:
  - First level: keyword-based metadata filtering (framework_id, name, doc_type, etc.)
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

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Config ──────────────────────────────────────────────────────────
REGION = os.getenv("REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dynamo-to-opensearch-rag-frameworks")
S3_PREFIX = "frameworks-v2"

KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID_V2") or os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID_V2", "")

FRAMEWORKS_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")
CLOUD_ID = os.getenv("CLOUD_ID", "")

# ── Clients ─────────────────────────────────────────────────────────
dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
bedrock_agent = boto3.client("bedrock-agent", region_name=REGION)
deserializer = TypeDeserializer()


# ───────────────────────────────────────────────────────────────────
# HELPERS
# ───────────────────────────────────────────────────────────────────
def unmarshall(dynamo_item: dict) -> dict:
    return {k: deserializer.deserialize(v) for k, v in dynamo_item.items()}


def scan_table(table_name: str) -> list[dict]:
    print(f"  📖 Scanning table: {table_name}")
    all_items, params = [], {"TableName": table_name}
    while True:
        resp = dynamodb.scan(**params)
        all_items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" in resp:
            params["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        else:
            break
    clean = [unmarshall(i) for i in all_items]
    print(f"    ✅ {len(clean)} records from {table_name}")
    return clean


def upload_section(bucket: str, folder: str, filename: str, content: str, metadata: dict):
    """Upload a single .txt + .metadata.json to S3."""
    s3_key = f"{folder}/{filename}.txt"
    meta_key = f"{folder}/{filename}.txt.metadata.json"
    s3.put_object(Bucket=bucket, Key=s3_key, Body=content.encode("utf-8"), ContentType="text/plain")
    s3.put_object(Bucket=bucket, Key=meta_key, Body=json.dumps(metadata, indent=2).encode("utf-8"), ContentType="application/json")
    print(f"    ✓ {s3_key} ({len(content)} chars)")


# ───────────────────────────────────────────────────────────────────
# PINECONE METADATA BUILDER — keyword fields for first-level filtering
# ───────────────────────────────────────────────────────────────────
def build_section_metadata(record: dict, section_name: str, extra: dict = None) -> dict:
    fw_id = record.get("id", "unknown")
    fw_name = record.get("name", "Unknown")
    meta = {
        "metadataAttributes": {
            "cloudId": CLOUD_ID,
            "framework_id": fw_id,
            "framework_name": fw_name,
            "section": section_name,
            "doc_type": "framework",
        }
    }

    # Add rich keyword fields for Pinecone first-level filtering
    if record.get("owner"):
        meta["metadataAttributes"]["owner"] = record["owner"]
    if record.get("assessmentCategory"):
        cats = record["assessmentCategory"]
        meta["metadataAttributes"]["assessment_categories"] = ", ".join(str(c) for c in cats) if isinstance(cats, list) else str(cats)
    if record.get("region"):
        regions = record["region"]
        meta["metadataAttributes"]["regions"] = ", ".join(str(r) for r in regions) if isinstance(regions, list) else str(regions)
    if record.get("verticals"):
        verticals = record["verticals"]
        meta["metadataAttributes"]["verticals"] = ", ".join(str(v) for v in verticals) if isinstance(verticals, list) else str(verticals)
    if record.get("searchAttributesAsJson"):
        meta["metadataAttributes"]["search_keywords"] = str(record["searchAttributesAsJson"])

    if extra:
        meta["metadataAttributes"].update(extra)
    return meta


# ───────────────────────────────────────────────────────────────────
# FLATTEN FUNCTIONS — plain text output, one per section
# ───────────────────────────────────────────────────────────────────
def flatten_fw_overview(record: dict) -> list[str]:
    """Section fw_01_overview — core framework details in plain text."""
    lines = []
    fw_id = record.get("id", "Unknown")
    name = record.get("name", "Unknown")

    lines.append("Framework Overview")
    lines.append("")

    if record.get("description"):
        lines.append(f"Description: {record['description']}")
    if record.get("owner"):
        lines.append(f"Owner: {record['owner']}")
    if record.get("count") is not None:
        lines.append(f"Total Controls: {record['count']}")

    if record.get("assessmentCategory"):
        cats = record["assessmentCategory"]
        if isinstance(cats, list):
            lines.append(f"Assessment Categories: {', '.join(str(c) for c in cats)}")
        else:
            lines.append(f"Assessment Categories: {cats}")

    if record.get("region"):
        regions = record["region"]
        if isinstance(regions, list):
            lines.append(f"Regions: {', '.join(str(r) for r in regions)}")
        else:
            lines.append(f"Regions: {regions}")

    if record.get("verticals"):
        verticals = record["verticals"]
        if isinstance(verticals, list):
            lines.append(f"Verticals: {', '.join(str(v) for v in verticals)}")
        else:
            lines.append(f"Verticals: {verticals}")

    if record.get("searchAttributesAsJson"):
        lines.append(f"Search Keywords: {record['searchAttributesAsJson']}")

    if record.get("frameWorkImgUrl"):
        lines.append(f"Framework Image URL: {record['frameWorkImgUrl']}")

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

    return lines


def flatten_fw_policy_references(record: dict) -> list[str]:
    """Section fw_02_policy_references — policy documents and links in plain text."""
    lines = []
    has_content = False

    if record.get("policyDocuments"):
        docs = record["policyDocuments"]
        if isinstance(docs, list) and docs:
            lines.append("Policy Documents:")
            lines.append("")
            for i, doc in enumerate(docs, 1):
                lines.append(f"  {i}. {doc}")
            lines.append("")
            has_content = True

    if record.get("policyLinks"):
        links = record["policyLinks"]
        if isinstance(links, list) and links:
            lines.append("Policy Links:")
            lines.append("")
            for i, link in enumerate(links, 1):
                lines.append(f"  {i}. {link}")
            lines.append("")
            has_content = True

    if not has_content:
        return []

    return lines


# ───────────────────────────────────────────────────────────────────
# UPLOAD ALL SECTIONS FOR ONE FRAMEWORK
# ───────────────────────────────────────────────────────────────────
def upload_framework_sections(record: dict) -> int:
    fw_id = record.get("id", "unknown")
    fw_name = record.get("name", "Unknown")
    folder = f"{S3_PREFIX}/{fw_id}"
    uploaded = 0

    # Header prepended to every section — plain text
    fw_header = (
        f"Framework: {fw_name}\n"
        f"Framework ID: {fw_id}\n\n"
    )

    section_defs = [
        ("fw_01_overview", flatten_fw_overview),
        ("fw_02_policy_references", flatten_fw_policy_references),
    ]

    for section_name, flatten_fn in section_defs:
        try:
            lines = flatten_fn(record)
            if not lines:
                continue
            content = "\n".join(lines) if isinstance(lines, list) else str(lines)
            if not content.strip():
                continue
            content = fw_header + content
            metadata = build_section_metadata(record, section_name)
            upload_section(S3_BUCKET, folder, section_name, content, metadata)
            uploaded += 1
        except Exception as e:
            print(f"    ✗ {section_name} failed: {e}")

    print(f"  → {fw_id}: uploaded {uploaded} section files")
    return uploaded


# ───────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    print(f"\n{'='*60}")
    print(f"  Frameworks — Plain Text Pipeline (Pinecone)")
    print(f"  Table:  {FRAMEWORKS_TABLE}")
    print(f"  S3:     s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"{'='*60}")

    frameworks = scan_table(FRAMEWORKS_TABLE)

    if not frameworks:
        print("⚠️  No records found. Exiting.")
        return 0

    print(f"\n📊 Found {len(frameworks)} frameworks:")
    for fw in frameworks:
        print(f"  • {fw.get('name', 'Unknown')} ({fw.get('id', '?')})")

    # Clear existing files
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

    # Upload sections
    print(f"\n📤 Uploading sections to s3://{S3_BUCKET}/{S3_PREFIX}/")
    total_sections = 0
    for record in frameworks:
        try:
            count = upload_framework_sections(record)
            total_sections += count
        except Exception as e:
            print(f"  ❌ Failed: {record.get('id', 'unknown')} — {e}")

    print(f"\n✅ Uploaded {total_sections} total section files for {len(frameworks)} frameworks.")
    return total_sections


def sync_knowledge_base():
    if not DATA_SOURCE_ID:
        print("\n⚠️  DATA_SOURCE_ID_FRAMEWORKS_V2 not set. Run setup_new_data_sources.py first.")
        return "SKIPPED"

    print(f"\n🔄 Starting Bedrock KB sync (KB: {KNOWLEDGE_BASE_ID}, DS: {DATA_SOURCE_ID})")
    response = bedrock_agent.start_ingestion_job(knowledgeBaseId=KNOWLEDGE_BASE_ID, dataSourceId=DATA_SOURCE_ID)
    job_id = response["ingestionJob"]["ingestionJobId"]
    print(f"  Ingestion Job ID: {job_id}")

    while True:
        job = bedrock_agent.get_ingestion_job(knowledgeBaseId=KNOWLEDGE_BASE_ID, dataSourceId=DATA_SOURCE_ID, ingestionJobId=job_id)
        status = job["ingestionJob"]["status"]
        stats = job["ingestionJob"]["statistics"]
        print(f"  Status: {status} | Scanned: {stats['numberOfDocumentsScanned']} | Indexed: {stats['numberOfNewDocumentsIndexed']} | Modified: {stats['numberOfModifiedDocumentsIndexed']} | Failed: {stats['numberOfDocumentsFailed']}")
        if status in ["COMPLETE", "FAILED", "STOPPED"]:
            break
        time.sleep(5)

    if status == "COMPLETE":
        total = stats["numberOfNewDocumentsIndexed"] + stats["numberOfModifiedDocumentsIndexed"]
        print(f"\n✅ KB sync complete! {total} documents indexed.")
    else:
        print(f"\n❌ KB sync {status}. Check the Bedrock console for details.")
    return status


if __name__ == "__main__":
    print("=" * 60)
    print("  Frameworks → S3 (Plain Text) → Pinecone")
    print(f"  Table: {FRAMEWORKS_TABLE}")
    print(f"  S3:    s3://{S3_BUCKET}/{S3_PREFIX}/")
    print("=" * 60)

    count = scan_and_upload()
    if count and count > 0:
        status = sync_knowledge_base()
        if status == "COMPLETE":
            print("\n🎉 Pipeline complete!")
        elif status == "SKIPPED":
            print("\n⚠️  Upload done. Run setup_new_data_sources.py first to create data source.")
        else:
            print("\n⚠️  Sync did not complete. Check AWS console.")
    else:
        print("\n⚠️  No sections to sync.")
