"""
Pipeline: staging-fusefy-frameworkControls → S3 (section-based) → Bedrock KB

Each framework's controls are split into section files:

  s3://{BUCKET}/framework-controls-v2/{frameworkId}/
      ├── fc_01_framework_summary.md     + .metadata.json
      └── fc_02_attached_controls.md     + .metadata.json

Each section has its own .metadata.json with:
  - framework_id, framework_name, section, doc_type, control_ids_associated
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
S3_PREFIX = "framework-controls-v2"

KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID_V2") or os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID_FC_V2", "")

FRAMEWORK_CONTROLS_TABLE = os.getenv("DYNAMODB_FRAMEWORK_CONTROLS_TABLE", "staging-fusefy-frameworkControls")
FRAMEWORKS_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")
CONTROLS_TABLE = os.getenv("DYNAMODB_CONTROLS_TABLE", "staging-fusefy-controls")

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
    s3_key = f"{folder}/{filename}.md"
    meta_key = f"{folder}/{filename}.md.metadata.json"
    s3.put_object(Bucket=bucket, Key=s3_key, Body=content.encode("utf-8"), ContentType="text/markdown")
    s3.put_object(Bucket=bucket, Key=meta_key, Body=json.dumps(metadata, indent=2).encode("utf-8"), ContentType="application/json")
    print(f"    ✓ {s3_key} ({len(content)} chars)")


# ───────────────────────────────────────────────────────────────────
# METADATA BUILDER
# ───────────────────────────────────────────────────────────────────
def build_section_metadata(framework: dict, section_name: str, control_ids: list[str] = None, extra: dict = None) -> dict:
    fw_id = framework.get("id", "unknown")
    fw_name = framework.get("name", "Unknown")
    meta = {
        "metadataAttributes": {
            "framework_id": fw_id,
            "framework_name": fw_name,
            "section": section_name,
            "doc_type": "framework-controls",
            "control_ids_associated": control_ids or [],
        }
    }
    if extra:
        meta["metadataAttributes"].update(extra)
    return meta


# ───────────────────────────────────────────────────────────────────
# FLATTEN FUNCTIONS — one per section
# ───────────────────────────────────────────────────────────────────
def flatten_fc_framework_summary(framework: dict, num_controls: int) -> list[str]:
    """Section fc_01_framework_summary — brief framework overview."""
    lines = []
    lines.append("## Framework Summary")
    lines.append("")

    if framework.get("description"):
        lines.append(f"**Description:** {framework['description']}")
    if framework.get("owner"):
        lines.append(f"**Owner:** {framework['owner']}")
    lines.append(f"**Total Attached Controls:** {num_controls}")

    if framework.get("assessmentCategory"):
        cats = framework["assessmentCategory"]
        if isinstance(cats, list):
            lines.append(f"**Assessment Categories:** {', '.join(str(c) for c in cats)}")
        else:
            lines.append(f"**Assessment Categories:** {cats}")

    if framework.get("region"):
        regions = framework["region"]
        if isinstance(regions, list):
            lines.append(f"**Regions:** {', '.join(str(r) for r in regions)}")
        else:
            lines.append(f"**Regions:** {regions}")

    if framework.get("verticals"):
        verticals = framework["verticals"]
        if isinstance(verticals, list):
            lines.append(f"**Verticals:** {', '.join(str(v) for v in verticals)}")
        else:
            lines.append(f"**Verticals:** {verticals}")

    if framework.get("searchAttributesAsJson"):
        lines.append(f"**Search Keywords:** {framework['searchAttributesAsJson']}")

    return lines


def flatten_fc_attached_controls(framework: dict, control_ids: list[str], control_lookup: dict) -> list[str]:
    """Section fc_02_attached_controls — full enriched list of all controls."""
    if not control_ids:
        return []

    fw_name = framework.get("name", "Unknown")
    lines = []
    lines.append(f"## Attached Controls ({len(control_ids)} controls)")
    lines.append("")

    for i, ctrl_id in enumerate(control_ids, 1):
        ctrl = control_lookup.get(ctrl_id)

        if ctrl:
            ctrl_name_field = ctrl.get("name", ctrl_id)
            if isinstance(ctrl_name_field, list) and ctrl_name_field:
                ctrl_display_name = ctrl_name_field[-1]
                ctrl_hierarchy = " > ".join(str(n) for n in ctrl_name_field)
            else:
                ctrl_display_name = str(ctrl_name_field)
                ctrl_hierarchy = None

            ctrl_code = ctrl.get("id", ctrl_id)

            lines.append(f"### {i}. {ctrl_display_name}")
            lines.append(f"- **Control ID:** {ctrl_code}")

            if ctrl.get("description"):
                lines.append(f"- **Description:** {ctrl['description']}")

            if ctrl_hierarchy:
                lines.append(f"- **Hierarchy:** {ctrl_hierarchy}")

            if ctrl.get("questionaire"):
                lines.append(f"- **Question:** {ctrl['questionaire']}")

            if ctrl.get("aiLifecycleStage"):
                lines.append(f"- **AI Lifecycle Stage:** {ctrl['aiLifecycleStage']}")

            if ctrl.get("trustworthyAiControl"):
                lines.append(f"- **Trustworthy AI Control:** {ctrl['trustworthyAiControl']}")

            if ctrl.get("assessmentCategory"):
                cats = ctrl["assessmentCategory"]
                if isinstance(cats, list):
                    lines.append(f"- **Assessment Categories:** {', '.join(str(c) for c in cats)}")
                else:
                    lines.append(f"- **Assessment Categories:** {cats}")

            if ctrl.get("gradingTypesFormat"):
                lines.append(f"- **Grading Format:** {ctrl['gradingTypesFormat']}")

            # Maturity Levels (Level 1 through Level 6)
            levels = []
            for lvl in range(1, 7):
                key = f"Level {lvl}"
                val = ctrl.get(key, "")
                if val and str(val).strip():
                    levels.append(f"L{lvl}: ✓")
            if levels:
                lines.append(f"- **Maturity Levels:** {', '.join(levels)}")

            if ctrl.get("searchAttributesAsJson"):
                lines.append(f"- **Search Keywords:** {ctrl['searchAttributesAsJson']}")

            # Catch-all for extra fields
            handled_ctrl_keys = {
                "id", "name", "description", "questionaire", "aiLifecycleStage",
                "trustworthyAiControl", "assessmentCategory",
                "gradingTypesFormat", "frameworkControlIds",
                "searchAttributesAsJson", "count", "tcoIds",
                "Level 1", "Level 2", "Level 3", "Level 4", "Level 5", "Level 6",
                "createdDate", "updatedDate"
            }
            extra = {k: v for k, v in ctrl.items() if k not in handled_ctrl_keys and v is not None}
            for key, val in extra.items():
                if isinstance(val, (list, dict)):
                    lines.append(f"- **{key}:** {json.dumps(val, default=str)}")
                else:
                    lines.append(f"- **{key}:** {val}")
        else:
            lines.append(f"### {i}. Control (not found)")
            lines.append(f"- **Control ID:** {ctrl_id}")

        lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# UPLOAD ALL SECTIONS FOR ONE FRAMEWORK
# ───────────────────────────────────────────────────────────────────
def upload_framework_control_sections(
    framework: dict,
    control_ids: list[str],
    control_lookup: dict,
) -> int:
    fw_id = framework.get("id", "unknown")
    fw_name = framework.get("name", "Unknown")
    folder = f"{S3_PREFIX}/{fw_id}"
    uploaded = 0

    # Header prepended to every section
    fc_header = (
        f"**Framework:** {fw_name}\n"
        f"**Framework ID:** {fw_id}\n"
        f"**Total Controls:** {len(control_ids)}\n\n"
    )

    # Section 1: Framework summary
    try:
        lines = flatten_fc_framework_summary(framework, len(control_ids))
        if lines:
            content = "\n".join(lines)
            if content.strip():
                content = fc_header + content
                metadata = build_section_metadata(framework, "fc_01_framework_summary", control_ids)
                upload_section(S3_BUCKET, folder, "fc_01_framework_summary", content, metadata)
                uploaded += 1
    except Exception as e:
        print(f"    ✗ fc_01_framework_summary failed: {e}")

    # Section 2: Attached controls
    try:
        lines = flatten_fc_attached_controls(framework, control_ids, control_lookup)
        if lines:
            content = "\n".join(lines)
            if content.strip():
                content = fc_header + content
                metadata = build_section_metadata(framework, "fc_02_attached_controls", control_ids)
                upload_section(S3_BUCKET, folder, "fc_02_attached_controls", content, metadata)
                uploaded += 1
    except Exception as e:
        print(f"    ✗ fc_02_attached_controls failed: {e}")

    print(f"  → {fw_id} ({fw_name}): uploaded {uploaded} section files")
    return uploaded


# ───────────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    print(f"\n{'='*60}")
    print(f"  Framework-Controls — Section-Based Pipeline")
    print(f"  S3:     s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"{'='*60}")

    # Scan all three tables
    mappings = scan_table(FRAMEWORK_CONTROLS_TABLE)
    frameworks = scan_table(FRAMEWORKS_TABLE)
    controls = scan_table(CONTROLS_TABLE)

    if not mappings:
        print("⚠️  No records found in frameworkControls table. Exiting.")
        return 0

    # Build lookups
    framework_lookup = {fw["id"]: fw for fw in frameworks if "id" in fw}
    control_lookup = {ctrl["id"]: ctrl for ctrl in controls if "id" in ctrl}

    # Group mappings by framework
    grouped = {}
    for mapping in mappings:
        fw_id = mapping.get("frameworkId")
        ctrl_id = mapping.get("controlId")
        if fw_id and ctrl_id:
            grouped.setdefault(fw_id, []).append(ctrl_id)

    print(f"\n📊 Found {len(grouped)} frameworks with attached controls:")
    for fw_id, ctrl_ids in grouped.items():
        fw_name = framework_lookup.get(fw_id, {}).get("name", "Unknown")
        print(f"  • {fw_name} ({fw_id}) → {len(ctrl_ids)} controls")

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
    for fw_id, ctrl_ids in grouped.items():
        try:
            framework = framework_lookup.get(fw_id)
            if not framework:
                print(f"  ⚠️  Framework {fw_id} not found — skipping.")
                continue
            count = upload_framework_control_sections(framework, ctrl_ids, control_lookup)
            total_sections += count
        except Exception as e:
            print(f"  ❌ Failed: {fw_id} — {e}")

    print(f"\n✅ Uploaded {total_sections} total section files for {len(grouped)} frameworks.")
    return total_sections


def sync_knowledge_base():
    if not DATA_SOURCE_ID:
        print("\n⚠️  DATA_SOURCE_ID_FC_V2 not set. Run setup_new_data_sources.py first.")
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
    print("  Framework-Controls → S3 (Section-Based) → Bedrock KB")
    print(f"  Mapping: {FRAMEWORK_CONTROLS_TABLE}")
    print(f"  S3:      s3://{S3_BUCKET}/{S3_PREFIX}/")
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
