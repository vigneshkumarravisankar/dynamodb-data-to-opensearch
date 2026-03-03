"""
Pipeline: staging-fusefy-usecaseAssessments (tenant) → S3 (section-based) → Bedrock KB

UPDATED APPROACH — Instead of one giant .md per use case, each use case is
split into individual section files stored under:

  s3://{BUCKET}/usecase-assessments-v2/{usecaseId}/
      ├── 01_overview.md                 + .metadata.json
      ├── 02_document_summary.md         + .metadata.json
      ├── 03_ai_bom.md                   + .metadata.json
      ├── 04_data_bom.md                 + .metadata.json
      ├── 05_metrics.md                  + .metadata.json
      ├── 06_jira_stories.md             + .metadata.json
      ├── 07_risk_and_controls.md        + .metadata.json
      ├── 08_design_document.md          + .metadata.json
      ├── 09_rollout_and_epics.md        + .metadata.json
      ├── 10_tco.md                      + .metadata.json
      ├── 11_model_validation.md         + .metadata.json
      └── 12_framework_kcis.md           + .metadata.json

Each section has its own .metadata.json with:
  - usecase_id, model_name, section, doc_type, etc.

Benefits:
  - Bedrock chunks each section cleanly (no cross-section bleeding)
  - Queries can filter by `section` metadata → only relevant data retrieved
  - Smaller context → faster LLM responses, lower cost
  - top_k can stay small (5-10) and still cover the right data
"""

import os
import sys
import json
import boto3
import time
from dotenv import load_dotenv
from boto3.dynamodb.types import TypeDeserializer

# ── Add parent dir to path so we can import flatten functions ──
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dynamo_to_s3_usecase_assessments import (
    flatten_overview,
    flatten_document_summary,
    flatten_ai_bom,
    flatten_data_bom,
    flatten_metrics,
    flatten_jira_stories,
    flatten_risk_and_controls,
    flatten_design_document,
    flatten_rollout_and_epics,
    flatten_tco,
    flatten_model_validation,
    flatten_framework_kcis,
    scan_table,
    unmarshall,
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Config ──────────────────────────────────────────────────────────
REGION = os.getenv("REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dynamo-to-opensearch-rag-frameworks")
S3_PREFIX = "usecase-assessments-v2"

# KB for section-based approach (will be set after creation or via env)
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID_V2", "")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID_V2", "")

USECASE_TABLE = os.getenv(
    "DYNAMODB_USECASE_ASSESSMENTS_TENANT_TABLE",
    "staging-fusefy-usecaseAssessments-d66cb7c7-04ac-4634-927f-06d91afa39bf"
)
FRAMEWORKS_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")
CONTROLS_TABLE = os.getenv("DYNAMODB_CONTROLS_TABLE", "staging-fusefy-controls")
FRAMEWORK_CONTROLS_TABLE = os.getenv(
    "DYNAMODB_FRAMEWORK_CONTROLS_TABLE", "staging-fusefy-frameworkControls"
)
MODEL_VALIDATION_TABLE = os.getenv(
    "DYNAMODB_MODEL_VALIDATION_TABLE",
    "staging-fusefy-modelValidation-d66cb7c7-04ac-4634-927f-06d91afa39bf"
)

# ── Clients ─────────────────────────────────────────────────────────
dynamodb = boto3.client("dynamodb", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
bedrock_agent = boto3.client("bedrock-agent", region_name=REGION)
deserializer = TypeDeserializer()


# ───────────────────────────────────────────────────────────────────
# SECTION DEFINITIONS
# ───────────────────────────────────────────────────────────────────
# Each entry: (section_filename, flatten_function, needs_extra_args)
# needs_extra_args: False = fn(record), True = handled separately

SECTION_DEFS = [
    ("01_overview",            flatten_overview,          "framework_lookup"),
    ("02_document_summary",    flatten_document_summary,  None),
    ("03_ai_bom",              flatten_ai_bom,            None),
    ("04_data_bom",            flatten_data_bom,          None),
    ("05_metrics",             flatten_metrics,            None),
    ("06_jira_stories",        flatten_jira_stories,       None),
    ("07_risk_and_controls",   flatten_risk_and_controls,  None),
    ("08_design_document",     flatten_design_document,    None),
    ("09_rollout_and_epics",   flatten_rollout_and_epics,  None),
    ("10_tco",                 flatten_tco,                None),
    # model_validation is special — handled separately
]


# ───────────────────────────────────────────────────────────────────
# METADATA BUILDER
# ───────────────────────────────────────────────────────────────────
def build_section_metadata(record: dict, section_name: str, extra: dict = None) -> dict:
    """Build .metadata.json for a single section file."""
    uc_id = record.get("id", "unknown")
    model_name = record.get("modelName", "Unknown")
    risk_fw_id = record.get("riskframeworkid", "")

    meta = {
        "metadataAttributes": {
            "usecase_id": uc_id,
            "section": section_name,
            "model_name": model_name,
            "doc_type": "usecase-assessment",
            "ai_category": record.get("aiCategory", ""),
            "overall_risk": record.get("overallRisk", ""),
            "department": record.get("department", ""),
            "status": record.get("status", ""),
            "framework_ids_associated": [risk_fw_id] if risk_fw_id else [],
        }
    }
    if extra:
        meta["metadataAttributes"].update(extra)
    return meta


# ───────────────────────────────────────────────────────────────────
# UPLOAD ONE SECTION
# ───────────────────────────────────────────────────────────────────
def upload_section(bucket: str, folder: str, filename: str, content: str, metadata: dict):
    """Upload a single .md + .metadata.json to S3."""
    s3_key = f"{folder}/{filename}.md"
    meta_key = f"{folder}/{filename}.md.metadata.json"

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=content.encode("utf-8"),
        ContentType="text/markdown"
    )
    s3.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=json.dumps(metadata, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"    ✓ {s3_key} ({len(content)} chars)")


# ───────────────────────────────────────────────────────────────────
# SPLIT & UPLOAD ALL SECTIONS FOR ONE USE CASE
# ───────────────────────────────────────────────────────────────────
def upload_usecase_sections(
    record: dict,
    framework_lookup: dict,
    model_validation_lookup: dict,
    controls_lookup: dict,
    framework_controls_grouped: dict = None,
) -> int:
    """Split one use case record into section files and upload each."""
    uc_id = record.get("id", "unknown")
    model_name = record.get("modelName", "Unknown")
    folder = f"{S3_PREFIX}/{uc_id}"
    uploaded = 0

    # Header prepended to EVERY section so the LLM always knows which use case
    uc_header = (
        f"**Use Case:** {model_name}\n"
        f"**Use Case ID:** {uc_id}\n\n"
    )

    # ── Standard sections ──
    for section_name, flatten_fn, extra_arg in SECTION_DEFS:
        try:
            if extra_arg == "framework_lookup":
                lines = flatten_fn(record, framework_lookup)
            else:
                lines = flatten_fn(record)

            if not lines:
                continue

            content = "\n".join(lines) if isinstance(lines, list) else str(lines)
            if not content.strip():
                continue

            content = uc_header + content

            metadata = build_section_metadata(record, section_name)
            upload_section(S3_BUCKET, folder, section_name, content, metadata)
            uploaded += 1

        except Exception as e:
            print(f"    ✗ {section_name} failed: {e}")

    # ── Model Validation (needs extra lookups) ──
    try:
        mv_lines = flatten_model_validation(
            record, model_validation_lookup, controls_lookup,
            framework_lookup, framework_controls_grouped or {}
        )
        if mv_lines:
            content = "\n".join(mv_lines) if isinstance(mv_lines, list) else str(mv_lines)
            if content.strip():
                content = uc_header + content
                mv_assessment_id = record.get("modelValidationAssessmentId", "")
                extra_meta = {}
                if mv_assessment_id and mv_assessment_id in model_validation_lookup:
                    mv_rec = model_validation_lookup[mv_assessment_id]
                    extra_meta = {
                        "model_validation_id": mv_assessment_id,
                        "model_validation_framework_name": mv_rec.get("frameworkName", ""),
                    }
                metadata = build_section_metadata(record, "11_model_validation", extra_meta)
                upload_section(S3_BUCKET, folder, "11_model_validation", content, metadata)
                uploaded += 1
    except Exception as e:
        print(f"    ✗ 11_model_validation failed: {e}")

    # ── Framework KCIs (Risk Framework Controls as Key Control Indicators) ──
    try:
        kci_lines = flatten_framework_kcis(
            record, framework_lookup,
            framework_controls_grouped or {},
            controls_lookup
        )
        if kci_lines:
            content = "\n".join(kci_lines) if isinstance(kci_lines, list) else str(kci_lines)
            if content.strip():
                content = uc_header + content
                risk_fw_id = record.get("riskframeworkid", "")
                extra_meta = {}
                if risk_fw_id:
                    fw = framework_lookup.get(risk_fw_id, {})
                    extra_meta = {
                        "framework_id": risk_fw_id,
                        "framework_name": fw.get("name", ""),
                    }
                metadata = build_section_metadata(record, "12_framework_kcis", extra_meta)
                upload_section(S3_BUCKET, folder, "12_framework_kcis", content, metadata)
                uploaded += 1
    except Exception as e:
        print(f"    ✗ 12_framework_kcis failed: {e}")

    print(f"  → {uc_id}: uploaded {uploaded} section files")
    return uploaded


# ───────────────────────────────────────────────────────────────────
# MAIN PIPELINE: Scan → Split → Upload to S3
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    """Scan all use case assessments, split into sections, upload to S3."""
    print(f"\n{'='*60}")
    print(f"  Use Case Assessments — Section-Based Pipeline")
    print(f"  Table:  {USECASE_TABLE}")
    print(f"  S3:     s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"{'='*60}")

    records = scan_table(USECASE_TABLE)

    # Load lookup tables
    print(f"\n📖 Loading frameworks...")
    frameworks = scan_table(FRAMEWORKS_TABLE)
    framework_lookup = {fw["id"]: fw for fw in frameworks if "id" in fw}
    print(f"  ✅ {len(framework_lookup)} frameworks")

    print(f"\n📖 Loading model validations...")
    model_validations = scan_table(MODEL_VALIDATION_TABLE)
    model_validation_lookup = {mv["id"]: mv for mv in model_validations if "id" in mv}
    print(f"  ✅ {len(model_validation_lookup)} model validations")

    print(f"\n📖 Loading controls...")
    controls = scan_table(CONTROLS_TABLE)
    controls_lookup = {ctrl["id"]: ctrl for ctrl in controls if "id" in ctrl}
    print(f"  ✅ {len(controls_lookup)} controls")

    print(f"\n📖 Loading framework-control mappings...")
    fc_mappings = scan_table(FRAMEWORK_CONTROLS_TABLE)
    framework_controls_grouped = {}
    for mapping in fc_mappings:
        fw_id = mapping.get("frameworkId")
        ctrl_id = mapping.get("controlId")
        if fw_id and ctrl_id:
            framework_controls_grouped.setdefault(fw_id, []).append(ctrl_id)
    print(f"  ✅ {len(framework_controls_grouped)} frameworks with controls")

    if not records:
        print("⚠️  No records found. Exiting.")
        return 0

    # Summary
    print(f"\n📊 Found {len(records)} use cases:")
    for r in records:
        uc_id = r.get("id", "Unknown")
        name = r.get("modelName", "Unknown")
        print(f"  • {name} ({uc_id})")

    # Clear existing section files
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
    for record in records:
        try:
            count = upload_usecase_sections(
                record, framework_lookup, model_validation_lookup,
                controls_lookup, framework_controls_grouped
            )
            total_sections += count
        except Exception as e:
            print(f"  ❌ Failed: {record.get('id', 'unknown')} — {e}")

    print(f"\n✅ Uploaded {total_sections} total section files for {len(records)} use cases.")
    return total_sections


# ───────────────────────────────────────────────────────────────────
# SYNC Bedrock Knowledge Base
# ───────────────────────────────────────────────────────────────────
def sync_knowledge_base():
    """Start ingestion job and wait for completion."""
    if not KNOWLEDGE_BASE_ID or not DATA_SOURCE_ID:
        print("\n⚠️  KNOWLEDGE_BASE_ID_V2 and DATA_SOURCE_ID_V2 not set.")
        print("   Run setup_bedrock_kb.py first, then add the IDs to .env")
        return "SKIPPED"

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
    print("  Use Case Assessments → S3 (Section-Based) → Bedrock KB")
    print(f"  Table: {USECASE_TABLE}")
    print(f"  S3:    s3://{S3_BUCKET}/{S3_PREFIX}/")
    print("=" * 60)

    count = scan_and_upload()

    if count and count > 0:
        status = sync_knowledge_base()
        if status == "COMPLETE":
            print("\n🎉 Pipeline complete! Run 'python query_kb_v2.py' to query.")
        elif status == "SKIPPED":
            print("\n⚠️  Upload done. Set up KB with: python setup_bedrock_kb.py")
        else:
            print("\n⚠️  Sync did not complete. Check AWS console.")
    else:
        print("\n⚠️  No sections to sync.")
