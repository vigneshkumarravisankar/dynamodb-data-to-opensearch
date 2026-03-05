"""
Pipeline: staging-fusefy-controls → S3 (section-based) → Bedrock KB

Each control is split into individual section files:

  s3://{BUCKET}/controls-v2/{controlId}/
      ├── ctrl_01_overview.md                + .metadata.json
      ├── ctrl_02_maturity_levels.md         + .metadata.json
      └── ctrl_03_framework_associations.md  + .metadata.json

Each section has its own .metadata.json with:
  - control_id, control_name, section, doc_type, framework_ids_associated
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
S3_PREFIX = "controls-v2"

KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID_V2") or os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID_CONTROLS_V2", "")

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
    s3_key = f"{folder}/{filename}.md"
    meta_key = f"{folder}/{filename}.md.metadata.json"
    s3.put_object(Bucket=bucket, Key=s3_key, Body=content.encode("utf-8"), ContentType="text/markdown")
    s3.put_object(Bucket=bucket, Key=meta_key, Body=json.dumps(metadata, indent=2).encode("utf-8"), ContentType="application/json")
    print(f"    ✓ {s3_key} ({len(content)} chars)")


def get_display_name(record: dict) -> str:
    name_field = record.get("name", [])
    if isinstance(name_field, list) and name_field:
        return str(name_field[-1])
    return str(name_field) if name_field else "Unknown"


def get_hierarchy(record: dict) -> str | None:
    name_field = record.get("name", [])
    if isinstance(name_field, list) and len(name_field) > 1:
        return " > ".join(str(n) for n in name_field)
    return None


def get_associated_framework_ids(record: dict) -> list[str]:
    fc_ids = record.get("frameworkControlIds", [])
    fw_ids = []
    if isinstance(fc_ids, list):
        for item in fc_ids:
            if isinstance(item, dict):
                for fid in item.keys():
                    if fid not in fw_ids:
                        fw_ids.append(fid)
    return fw_ids


# ───────────────────────────────────────────────────────────────────
# METADATA BUILDER
# ───────────────────────────────────────────────────────────────────
def build_section_metadata(record: dict, section_name: str, extra: dict = None) -> dict:
    ctrl_id = record.get("id", "unknown")
    display_name = get_display_name(record)
    fw_ids = get_associated_framework_ids(record)

    meta = {
        "metadataAttributes": {
            "control_id": ctrl_id,
            "control_name": display_name,
            "section": section_name,
            "doc_type": "control",
            "framework_ids_associated": fw_ids,
        }
    }
    if extra:
        meta["metadataAttributes"].update(extra)
    return meta


# ───────────────────────────────────────────────────────────────────
# FLATTEN FUNCTIONS — one per section
# ───────────────────────────────────────────────────────────────────
def flatten_ctrl_overview(record: dict) -> list[str]:
    """Section ctrl_01_overview — core control details."""
    lines = []
    ctrl_id = record.get("id", "Unknown")
    display_name = get_display_name(record)
    hierarchy = get_hierarchy(record)

    lines.append("## Control Overview")
    lines.append("")

    if hierarchy:
        lines.append(f"**Hierarchy:** {hierarchy}")

    if record.get("description"):
        lines.append(f"**Description:** {record['description']}")

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

    if record.get("searchAttributesAsJson"):
        lines.append(f"**Search Keywords:** {record['searchAttributesAsJson']}")

    if record.get("tcoIds"):
        lines.append(f"**TCO ID:** {record['tcoIds']}")

    # Catch-all
    handled_keys = {
        "id", "name", "description", "questionaire", "aiLifecycleStage",
        "trustworthyAiControl", "assessmentCategory",
        "gradingTypesFormat", "frameworkControlIds",
        "searchAttributesAsJson", "count", "tcoIds",
        "Level 1", "Level 2", "Level 3", "Level 4", "Level 5", "Level 6",
        "createdDate", "updatedDate"
    }
    extra = {k: v for k, v in record.items() if k not in handled_keys and v is not None}
    if extra:
        lines.append("")
        lines.append("### Additional Information")
        for key, val in extra.items():
            if isinstance(val, (list, dict)):
                lines.append(f"- **{key}:** {json.dumps(val, default=str)}")
            else:
                lines.append(f"- **{key}:** {val}")

    return lines


def flatten_ctrl_maturity_levels(record: dict) -> list[str]:
    """Section ctrl_02_maturity_levels — AI maturity level detail."""
    active_levels = []
    for lvl_key in ["Level 1", "Level 2", "Level 3", "Level 4", "Level 5", "Level 6"]:
        val = record.get(lvl_key, "")
        if val and str(val).strip():
            level_info = MATURITY_LEVELS.get(lvl_key, {})
            active_levels.append({
                "key": lvl_key,
                "name": level_info.get("name", "Unknown"),
                "description": level_info.get("description", ""),
            })

    if not active_levels:
        return []

    lines = []
    lines.append("## AI Maturity Levels")
    lines.append("")

    for lvl in active_levels:
        lines.append(f"### {lvl['key']}: {lvl['name']}")
        lines.append(f"{lvl['description']}")
        lines.append("")

    lines.append(
        f"This control is applicable at the **{', '.join(l['name'] for l in active_levels)}** "
        f"maturity stage(s) of an organization's AI adoption journey."
    )

    return lines


def flatten_ctrl_framework_associations(record: dict, framework_lookup: dict) -> list[str]:
    """Section ctrl_03_framework_associations — which frameworks this control belongs to."""
    fc_ids = record.get("frameworkControlIds", [])
    if not isinstance(fc_ids, list) or not fc_ids:
        return []

    lines = []
    lines.append("## Associated Frameworks")
    lines.append("")

    count = 0
    for item in fc_ids:
        if isinstance(item, dict):
            for fw_id, domain in item.items():
                count += 1
                fw = framework_lookup.get(fw_id)
                if fw:
                    fw_name = fw.get("name", "Unknown")
                    lines.append(f"### {count}. {fw_name} ({fw_id})")
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
                    lines.append(f"### {count}. {fw_id}")
                    lines.append(f"- **Domain:** {domain}")
                    lines.append("")

    if count == 0:
        return []

    return lines


# ───────────────────────────────────────────────────────────────────
# UPLOAD ALL SECTIONS FOR ONE CONTROL
# ───────────────────────────────────────────────────────────────────
def upload_control_sections(record: dict, framework_lookup: dict) -> int:
    ctrl_id = record.get("id", "unknown")
    display_name = get_display_name(record)
    folder = f"{S3_PREFIX}/{ctrl_id}"
    uploaded = 0

    # Header prepended to every section
    ctrl_header = (
        f"**Control:** {display_name}\n"
        f"**Control ID:** {ctrl_id}\n\n"
    )

    # Standard sections (no extra args)
    simple_sections = [
        ("ctrl_01_overview", flatten_ctrl_overview),
        ("ctrl_02_maturity_levels", flatten_ctrl_maturity_levels),
    ]

    for section_name, flatten_fn in simple_sections:
        try:
            lines = flatten_fn(record)
            if not lines:
                continue
            content = "\n".join(lines) if isinstance(lines, list) else str(lines)
            if not content.strip():
                continue
            content = ctrl_header + content
            metadata = build_section_metadata(record, section_name)
            upload_section(S3_BUCKET, folder, section_name, content, metadata)
            uploaded += 1
        except Exception as e:
            print(f"    ✗ {section_name} failed: {e}")

    # Framework associations (needs framework_lookup)
    try:
        lines = flatten_ctrl_framework_associations(record, framework_lookup)
        if lines:
            content = "\n".join(lines)
            if content.strip():
                content = ctrl_header + content
                metadata = build_section_metadata(record, "ctrl_03_framework_associations")
                upload_section(S3_BUCKET, folder, "ctrl_03_framework_associations", content, metadata)
                uploaded += 1
    except Exception as e:
        print(f"    ✗ ctrl_03_framework_associations failed: {e}")

    return uploaded


# ───────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    print(f"\n{'='*60}")
    print(f"  Controls — Section-Based Pipeline")
    print(f"  Table:  {CONTROLS_TABLE}")
    print(f"  S3:     s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"{'='*60}")

    controls = scan_table(CONTROLS_TABLE)
    frameworks = scan_table(FRAMEWORKS_TABLE)

    if not controls:
        print("⚠️  No records found. Exiting.")
        return 0

    framework_lookup = {fw["id"]: fw for fw in frameworks if "id" in fw}
    print(f"  📚 Built framework lookup: {len(framework_lookup)} frameworks")

    # Maturity level stats
    level_counts = {f"Level {i}": 0 for i in range(1, 7)}
    for ctrl in controls:
        for lvl in range(1, 7):
            key = f"Level {lvl}"
            val = ctrl.get(key, "")
            if val and str(val).strip():
                level_counts[key] += 1

    print(f"\n📊 Controls by AI Maturity Level:")
    for lvl_key, count in level_counts.items():
        lvl_name = MATURITY_LEVELS.get(lvl_key, {}).get("name", "")
        print(f"  • {lvl_key} ({lvl_name}): {count} controls")

    print(f"\n📊 Found {len(controls)} controls")

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
    for i, record in enumerate(controls, 1):
        try:
            count = upload_control_sections(record, framework_lookup)
            total_sections += count
            if i % 100 == 0:
                print(f"  ... processed {i}/{len(controls)} controls ({total_sections} sections)")
        except Exception as e:
            print(f"  ❌ Failed: {record.get('id', 'unknown')} — {e}")

    print(f"\n✅ Uploaded {total_sections} total section files for {len(controls)} controls.")
    return total_sections


def sync_knowledge_base():
    if not DATA_SOURCE_ID:
        print("\n⚠️  DATA_SOURCE_ID_CONTROLS_V2 not set. Run setup_new_data_sources.py first.")
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
    print("  Controls → S3 (Section-Based) → Bedrock KB")
    print(f"  Table: {CONTROLS_TABLE}")
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
