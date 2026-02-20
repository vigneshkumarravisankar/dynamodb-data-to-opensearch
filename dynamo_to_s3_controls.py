"""
Pipeline: staging-fusefy-controls → S3 → Bedrock KB

Standalone controls pipeline — each control gets its own enriched markdown
document with full details including AI maturity level context.

The Level 1-6 fields map to AI Maturity Levels:
  Level 1: AI Discovery
  Level 2: AI Pilot Projects
  Level 3: AI Strategic Applications
  Level 4: AI Business Integration
  Level 5: AI Optimization
  Level 6: AI Autonomy

Each control document includes the maturity level name and description
so RAG can answer queries like "which controls are at Level 3?" or
"show me all controls for AI Strategic Applications maturity".
"""

import os
import json
import boto3
import time
from dotenv import load_dotenv
from boto3.dynamodb.types import TypeDeserializer

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────
REGION = os.getenv("REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dynamo-to-opensearch-rag-frameworks")
S3_PREFIX = "controls"
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID", "GT9B3WZTOE")

CONTROLS_TABLE = os.getenv("DYNAMODB_CONTROLS_TABLE", "staging-fusefy-controls")
FRAMEWORKS_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")

# ── Clients ─────────────────────────────────────────────────────────
dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
bedrock_agent = boto3.client("bedrock-agent", region_name=REGION)
deserializer = TypeDeserializer()


# ── AI Maturity Level Definitions ───────────────────────────────────
MATURITY_LEVELS = {
    "Level 1": {
        "name": "AI Discovery",
        "description": (
            "Organizations are beginning to experiment with AI technologies. "
            "Focus on foundational aspects: initial data governance policies, "
            "experimenting with basic data sources, basic security measures, "
            "fixed model training, basic prompt engineering, manual deployments, "
            "and general-purpose copilots."
        ),
    },
    "Level 2": {
        "name": "AI Pilot Projects",
        "description": (
            "Organizations are running pilot AI projects to test feasibility and value. "
            "Incorporates structured data sources, initial feature stores, feedback mechanisms, "
            "on-demand training environments, automated deployments, model registries, "
            "model drift monitoring, RAG techniques, and enhanced prompt engineering."
        ),
    },
    "Level 3": {
        "name": "AI Strategic Applications",
        "description": (
            "AI becomes strategic, supporting key business functions. "
            "Integrates additional data sources, refines feature stores, implements "
            "advanced feedback mechanisms, AI/ML risk committees, bias detection, "
            "explainability controls, knowledge distillation, adapter models, "
            "contextual RAG, agents, and multi-agent routing."
        ),
    },
    "Level 4": {
        "name": "AI Business Integration",
        "description": (
            "AI is fully integrated into business processes, enhancing operations "
            "and decision-making. Advanced feature stores, real-time feature extraction, "
            "automated model retraining, RLHF, PII/data leakage protection, "
            "prompt injection detection, profanity guardrails, multi-region deployments, "
            "and proactive monitoring."
        ),
    },
    "Level 5": {
        "name": "AI Optimization",
        "description": (
            "Focus on optimizing AI performance and scalability. Optimized data pipelines, "
            "continuous reinforcement learning, knowledge distillation from LLMs to SLMs, "
            "domain-specific LLM fine-tuning, dynamic RAG, Graph RAG, multi-agent systems, "
            "multi-modal models, and comprehensive security/bias monitoring."
        ),
    },
    "Level 6": {
        "name": "AI Autonomy",
        "description": (
            "AI systems operate autonomously, making decisions and adapting without "
            "human intervention. Real-time feature extraction, self-learning models, "
            "autonomous model training/tuning/deployment, autonomous RAG updates, "
            "multi-source RAG systems, proactive incident response, and fully "
            "autonomous agents for end-to-end processes."
        ),
    },
}


# ───────────────────────────────────────────────────────────────────
# 1. UNMARSHALL DynamoDB JSON → Clean Python dict
# ───────────────────────────────────────────────────────────────────
def unmarshall(dynamo_item: dict) -> dict:
    """Convert DynamoDB typed JSON to plain Python dict."""
    return {key: deserializer.deserialize(value) for key, value in dynamo_item.items()}


# ───────────────────────────────────────────────────────────────────
# 2. SCAN DynamoDB table
# ───────────────────────────────────────────────────────────────────
def scan_table(table_name: str) -> list[dict]:
    """Scan all items from a DynamoDB table and return unmarshalled records."""
    print(f"  📖 Scanning table: {table_name}")
    all_items = []
    params = {"TableName": table_name}

    while True:
        response = dynamodb.scan(**params)
        items = response.get("Items", [])
        all_items.extend(items)

        if "LastEvaluatedKey" in response:
            params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break

    clean_items = [unmarshall(item) for item in all_items]
    print(f"    ✅ {len(clean_items)} records from {table_name}")
    return clean_items


# ───────────────────────────────────────────────────────────────────
# 3. FLATTEN a control record into enriched markdown for RAG
# ───────────────────────────────────────────────────────────────────
def flatten_control_for_rag(record: dict, framework_lookup: dict = None) -> str:
    """
    Convert an unmarshalled control record into a rich markdown document
    with AI maturity level context and full framework details embedded
    for optimal RAG retrieval.
    """
    if framework_lookup is None:
        framework_lookup = {}
    lines = []
    ctrl_id = record.get("id", "Unknown")

    # ── Name: hierarchical list → display name + hierarchy ──
    name_field = record.get("name", [])
    if isinstance(name_field, list) and name_field:
        display_name = name_field[-1]
        hierarchy = " > ".join(str(n) for n in name_field)
    else:
        display_name = str(name_field) if name_field else "Unknown"
        hierarchy = None

    lines.append(f"# Control: {display_name}")
    lines.append(f"**Control ID:** {ctrl_id}")
    lines.append("")

    if hierarchy:
        lines.append(f"**Control Hierarchy:** {hierarchy}")

    # ── Core Fields ──
    if record.get("questionaire"):
        lines.append(f"**Assessment Question:** {record['questionaire']}")

    if record.get("aiLifecycleStage"):
        lines.append(f"**AI Lifecycle Stage:** {record['aiLifecycleStage']}")

    if record.get("trustworthyAiControl"):
        lines.append(f"**Trustworthy AI Control Category:** {record['trustworthyAiControl']}")

    if record.get("assessmentCategory"):
        cats = record["assessmentCategory"]
        if isinstance(cats, list):
            lines.append(f"**Assessment Categories:** {', '.join(str(c) for c in cats)}")
        else:
            lines.append(f"**Assessment Categories:** {cats}")

    if record.get("gradingTypesFormat"):
        lines.append(f"**Grading Format:** {record['gradingTypesFormat']}")

    # ── AI Maturity Level (the key enrichment) ──
    active_levels = []
    for lvl_key in ["Level 1", "Level 2", "Level 3", "Level 4", "Level 5", "Level 6"]:
        val = record.get(lvl_key, "")
        if val and val.strip():
            level_info = MATURITY_LEVELS.get(lvl_key, {})
            active_levels.append({
                "key": lvl_key,
                "name": level_info.get("name", "Unknown"),
                "description": level_info.get("description", ""),
            })

    if active_levels:
        lines.append("")
        lines.append(f"## AI Maturity Level")
        lines.append("")
        for lvl in active_levels:
            lines.append(f"**{lvl['key']}: {lvl['name']}**")
            lines.append(f"{lvl['description']}")
            lines.append("")
        lines.append(
            f"This control is applicable at the **{', '.join(l['name'] for l in active_levels)}** "
            f"maturity stage(s) of an organization's AI adoption journey."
        )
    else:
        lines.append("")
        lines.append("**AI Maturity Level:** Not assigned")

    # ── Framework Associations (enriched from staging-fusefy-frameworks) ──
    if record.get("frameworkControlIds"):
        fc_ids = record["frameworkControlIds"]
        if isinstance(fc_ids, list) and fc_ids:
            lines.append("")
            lines.append("## Associated Frameworks")
            lines.append("")
            for item in fc_ids:
                if isinstance(item, dict):
                    for fw_id, domain in item.items():
                        fw = framework_lookup.get(fw_id)
                        if fw:
                            fw_name = fw.get("name", "Unknown")
                            lines.append(f"### {fw_name} ({fw_id})")
                            lines.append(f"- **Domain:** {domain}")
                            if fw.get("description"):
                                lines.append(f"- **Description:** {fw['description']}")
                            if fw.get("owner"):
                                lines.append(f"- **Owner:** {fw['owner']}")
                            if fw.get("region"):
                                regions = fw["region"]
                                if isinstance(regions, list):
                                    lines.append(f"- **Regions:** {', '.join(str(r) for r in regions)}")
                                else:
                                    lines.append(f"- **Regions:** {regions}")
                            if fw.get("verticals"):
                                verts = fw["verticals"]
                                if isinstance(verts, list):
                                    lines.append(f"- **Verticals:** {', '.join(str(v) for v in verts)}")
                                else:
                                    lines.append(f"- **Verticals:** {verts}")
                            if fw.get("assessmentCategory"):
                                cats = fw["assessmentCategory"]
                                if isinstance(cats, list):
                                    lines.append(f"- **Assessment Categories:** {', '.join(str(c) for c in cats)}")
                                else:
                                    lines.append(f"- **Assessment Categories:** {cats}")
                            lines.append("")
                        else:
                            lines.append(f"- **{fw_id}** — Domain: {domain}")
                else:
                    lines.append(f"- {item}")

    # ── Search Keywords ──
    if record.get("searchAttributesAsJson"):
        lines.append("")
        lines.append(f"**Search Keywords:** {record['searchAttributesAsJson']}")

    # ── Catch-all for any other fields ──
    handled_keys = {
        "id", "name", "questionaire", "aiLifecycleStage",
        "trustworthyAiControl", "assessmentCategory",
        "gradingTypesFormat", "frameworkControlIds",
        "searchAttributesAsJson", "count",
        "Level 1", "Level 2", "Level 3", "Level 4", "Level 5", "Level 6",
        "createdDate", "updatedDate"
    }
    extra = {k: v for k, v in record.items() if k not in handled_keys and v is not None}
    if extra:
        lines.append("")
        lines.append("## Additional Information")
        for key, val in extra.items():
            if isinstance(val, (list, dict)):
                lines.append(f"- **{key}:** {json.dumps(val, default=str)}")
            else:
                lines.append(f"- **{key}:** {val}")

    return "\n".join(lines)


# ───────────────────────────────────────────────────────────────────
# 4. MAIN PIPELINE: Scan → Flatten → Upload to S3
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    """Scan all controls, flatten with maturity context, upload to S3."""
    print(f"\n{'='*60}")
    print(f"  Controls Pipeline")
    print(f"  Table:  {CONTROLS_TABLE}")
    print(f"  S3:     s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"{'='*60}")

    controls = scan_table(CONTROLS_TABLE)
    frameworks = scan_table(FRAMEWORKS_TABLE)

    if not controls:
        print("⚠️  No records found in controls table. Exiting.")
        return 0

    # Build framework lookup by ID for enriching frameworkControlIds
    framework_lookup = {}
    for fw in frameworks:
        fw_id = fw.get("id")
        if fw_id:
            framework_lookup[fw_id] = fw
    print(f"  📚 Built framework lookup: {len(framework_lookup)} frameworks")

    # Summary by maturity level
    level_counts = {f"Level {i}": 0 for i in range(1, 7)}
    for ctrl in controls:
        for lvl in range(1, 7):
            key = f"Level {lvl}"
            val = ctrl.get(key, "")
            if val and val.strip():
                level_counts[key] += 1

    print(f"\n📊 Controls by AI Maturity Level:")
    for lvl_key, count in level_counts.items():
        lvl_name = MATURITY_LEVELS.get(lvl_key, {}).get("name", "")
        print(f"  • {lvl_key} ({lvl_name}): {count} controls")

    # Clear existing files in S3 prefix
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

    # Upload each control as a separate markdown file
    print(f"\n📤 Uploading to s3://{S3_BUCKET}/{S3_PREFIX}/")
    uploaded = 0
    for record in controls:
        try:
            ctrl_id = record.get("id", f"unknown-{uploaded}")

            content = flatten_control_for_rag(record, framework_lookup)
            s3_key = f"{S3_PREFIX}/{ctrl_id}.md"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=content.encode("utf-8"),
                ContentType="text/markdown"
            )

            # Extract display name for logging
            name_field = record.get("name", [])
            if isinstance(name_field, list) and name_field:
                display = name_field[-1]
            else:
                display = str(name_field)

            uploaded += 1
            print(f"  ✅ {ctrl_id} — {display} ({len(content)} chars)")

        except Exception as e:
            record_id = record.get("id", "unknown")
            print(f"  ❌ Failed: {record_id} — {e}")

    print(f"\n✅ Uploaded {uploaded}/{len(controls)} control documents to S3.")
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
    print("  Controls → S3 → Bedrock KB Pipeline")
    print(f"  Table: {CONTROLS_TABLE}")
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
