"""
Pipeline: staging-fusefy-frameworkControls → S3 → Bedrock KB

This table is a junction/mapping table linking frameworks to controls.
Each record has: id, frameworkId, controlId.

A single framework (e.g. AI-ADF-024) can have multiple controls attached.

This pipeline:
  1. Scans staging-fusefy-frameworkControls to get all mappings
  2. Scans staging-fusefy-frameworks to get framework details (name, description, etc.)
  3. Scans staging-fusefy-controls to get control details
  4. Groups mappings by frameworkId
  5. Produces one enriched markdown document per framework showing:
     - Framework details
     - All attached controls with their details
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
S3_PREFIX = "framework-controls"
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID", "GT9B3WZTOE")

# Table names
FRAMEWORK_CONTROLS_TABLE = os.getenv("DYNAMODB_FRAMEWORK_CONTROLS_TABLE", "staging-fusefy-frameworkControls")
FRAMEWORKS_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")
CONTROLS_TABLE = os.getenv("DYNAMODB_CONTROLS_TABLE", "staging-fusefy-controls")

# ── Clients ─────────────────────────────────────────────────────────
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
# 2. SCAN an entire DynamoDB table
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

    # Unmarshall all items
    clean_items = [unmarshall(item) for item in all_items]
    print(f"    ✅ {len(clean_items)} records from {table_name}")
    return clean_items


# ───────────────────────────────────────────────────────────────────
# 3. BUILD LOOKUP DICTS for frameworks and controls
# ───────────────────────────────────────────────────────────────────
def build_framework_lookup(frameworks: list[dict]) -> dict:
    """Build a dict keyed by framework 'id' for quick lookup."""
    lookup = {}
    for fw in frameworks:
        fw_id = fw.get("id")
        if fw_id:
            lookup[fw_id] = fw
    return lookup


def build_control_lookup(controls: list[dict]) -> dict:
    """Build a dict keyed by control 'id' for quick lookup."""
    lookup = {}
    for ctrl in controls:
        ctrl_id = ctrl.get("id")
        if ctrl_id:
            lookup[ctrl_id] = ctrl
    return lookup


# ───────────────────────────────────────────────────────────────────
# 4. GROUP frameworkControls by frameworkId
# ───────────────────────────────────────────────────────────────────
def group_by_framework(mappings: list[dict]) -> dict:
    """
    Group frameworkControl mappings by frameworkId.
    Returns: { frameworkId: [controlId1, controlId2, ...] }
    """
    grouped = {}
    for mapping in mappings:
        fw_id = mapping.get("frameworkId")
        ctrl_id = mapping.get("controlId")
        if fw_id and ctrl_id:
            grouped.setdefault(fw_id, []).append(ctrl_id)

    return grouped


# ───────────────────────────────────────────────────────────────────
# 5. FLATTEN into enriched markdown for RAG
# ───────────────────────────────────────────────────────────────────
def flatten_framework_with_controls(
    framework: dict,
    control_ids: list[str],
    control_lookup: dict
) -> str:
    """
    Create an enriched markdown document for a framework with all its
    attached controls resolved from the controls table.
    """
    lines = []
    fw_id = framework.get("id", "Unknown")
    fw_name = framework.get("name", "Unknown")

    lines.append(f"# Framework: {fw_name}")
    lines.append(f"**Framework ID:** {fw_id}")
    lines.append("")

    # ── Framework Details ──
    if framework.get("description"):
        lines.append(f"**Description:** {framework['description']}")
    if framework.get("owner"):
        lines.append(f"**Owner:** {framework['owner']}")
    if framework.get("count") is not None:
        lines.append(f"**Control Count:** {framework['count']}")

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

    # ── Attached Controls ──
    lines.append("")
    lines.append(f"## Attached Controls ({len(control_ids)} controls)")
    lines.append("")

    for i, ctrl_id in enumerate(control_ids, 1):
        ctrl = control_lookup.get(ctrl_id)

        if ctrl:
            # name is a hierarchical list: [lifecycle, platform, category, subcategory, control]
            ctrl_name_field = ctrl.get("name", ctrl_id)
            if isinstance(ctrl_name_field, list) and ctrl_name_field:
                ctrl_display_name = ctrl_name_field[-1]  # last element = actual control name
                ctrl_hierarchy = " > ".join(str(n) for n in ctrl_name_field)
            else:
                ctrl_display_name = str(ctrl_name_field)
                ctrl_hierarchy = None

            ctrl_code = ctrl.get("id", ctrl_id)

            lines.append(f"### {i}. {ctrl_display_name}")
            lines.append(f"- **Control ID:** {ctrl_code}")

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
                if val and val.strip():
                    levels.append(f"L{lvl}: ✓")
            if levels:
                lines.append(f"- **Maturity Levels:** {', '.join(levels)}")

            if ctrl.get("frameworkControlIds"):
                fc_ids = ctrl["frameworkControlIds"]
                if isinstance(fc_ids, list):
                    fc_parts = []
                    for item in fc_ids:
                        if isinstance(item, dict):
                            for fid, domain in item.items():
                                fc_parts.append(f"{fid} ({domain})")
                        else:
                            fc_parts.append(str(item))
                    lines.append(f"- **Framework Associations:** {', '.join(fc_parts)}")

            if ctrl.get("searchAttributesAsJson"):
                lines.append(f"- **Search Keywords:** {ctrl['searchAttributesAsJson']}")

            # Catch-all for any other fields not explicitly handled
            handled_ctrl_keys = {
                "id", "name", "questionaire", "aiLifecycleStage",
                "trustworthyAiControl", "assessmentCategory",
                "gradingTypesFormat", "frameworkControlIds",
                "searchAttributesAsJson", "count",
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
            # Control not found in controls table — still record the ID
            lines.append(f"### {i}. Control (not found in controls table)")
            lines.append(f"- **Control ID:** {ctrl_id}")

        lines.append("")

    return "\n".join(lines)


# ───────────────────────────────────────────────────────────────────
# 6. MAIN PIPELINE: Scan → Enrich → Upload → Sync
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    """
    1. Scan all three tables (frameworkControls, frameworks, controls)
    2. Group mappings by framework
    3. Enrich each framework with its attached controls
    4. Upload one markdown file per framework to S3
    """
    print(f"\n{'='*60}")
    print("  Scanning all required tables...")
    print(f"{'='*60}")

    # Scan all three tables
    mappings = scan_table(FRAMEWORK_CONTROLS_TABLE)
    frameworks = scan_table(FRAMEWORKS_TABLE)
    controls = scan_table(CONTROLS_TABLE)

    if not mappings:
        print("⚠️  No records found in frameworkControls table. Exiting.")
        return 0

    # Build lookup dicts
    framework_lookup = build_framework_lookup(frameworks)
    control_lookup = build_control_lookup(controls)

    # Group mappings by frameworkId
    grouped = group_by_framework(mappings)
    print(f"\n📊 Found {len(grouped)} frameworks with attached controls")

    # Show summary
    for fw_id, ctrl_ids in grouped.items():
        fw_name = framework_lookup.get(fw_id, {}).get("name", "Unknown")
        print(f"  • {fw_name} ({fw_id}) → {len(ctrl_ids)} controls")

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

    # Upload one enriched markdown file per framework
    print(f"\n📤 Uploading to s3://{S3_BUCKET}/{S3_PREFIX}/")
    uploaded = 0

    for fw_id, ctrl_ids in grouped.items():
        try:
            framework = framework_lookup.get(fw_id)

            if not framework:
                print(f"  ⚠️  Framework {fw_id} not found in frameworks table — skipping.")
                continue

            fw_name = framework.get("name", "Unknown")

            # Build enriched document
            content = flatten_framework_with_controls(framework, ctrl_ids, control_lookup)

            # Upload to S3
            s3_key = f"{S3_PREFIX}/{fw_id}.md"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=content.encode("utf-8"),
                ContentType="text/markdown"
            )

            uploaded += 1
            print(f"  ✅ {fw_name} ({fw_id}) — {len(ctrl_ids)} controls — {len(content)} chars")

        except Exception as e:
            print(f"  ❌ Failed: {fw_id} — {e}")

    print(f"\n✅ Uploaded {uploaded}/{len(grouped)} framework-control documents to S3.")
    return uploaded


# ───────────────────────────────────────────────────────────────────
# 7. SYNC Bedrock Knowledge Base
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
    print("  FrameworkControls → S3 → Bedrock KB Pipeline")
    print(f"  Mapping Table:  {FRAMEWORK_CONTROLS_TABLE}")
    print(f"  Frameworks:     {FRAMEWORKS_TABLE}")
    print(f"  Controls:       {CONTROLS_TABLE}")
    print(f"  S3:             s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"  KB:             {KNOWLEDGE_BASE_ID}")
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
