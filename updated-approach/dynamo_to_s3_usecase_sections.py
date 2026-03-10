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
      ├── 07b_threat_assessment.md       + .metadata.json
      ├── 08_design_document.md          + .metadata.json
      ├── 09_rollout_and_epics.md        + .metadata.json
      ├── 10_tco.md                      + .metadata.json
      ├── 11_model_validation.md         + .metadata.json
      ├── 12_framework_kcis.md           + .metadata.json
      ├── 13_ai_model_info.md            + .metadata.json   (from validation-results)
      ├── 14_ai_eval_metrics.md          + .metadata.json   (from validation-results)
      ├── 15_ai_sbom.md                  + .metadata.json   (from validation-results)
      ├── 16_ai_cspm.md                  + .metadata.json   (from validation-results)
      ├── 17_ai_security_threats.md       + .metadata.json   (from validation-results)
      ├── 18_ai_chart_data.md            + .metadata.json   (from validation-results)
      ├── 19_ai_agent_evaluators.md      + .metadata.json   (from validation-results)
      ├── 20_monitoring_day_1.md          + .metadata.json   (from monitoring-results/day1.json)
      ├── 20_monitoring_day_2.md          + .metadata.json   (from monitoring-results/day2.json)
      └── 20_monitoring_day_N.md          + .metadata.json   (from monitoring-results/dayN.json)

Each section has its own .metadata.json with:
  - usecase_id, model_name, section, doc_type, etc.

Benefits:
  - Bedrock chunks each section cleanly (no cross-section bleeding)
  - Queries can filter by `section` metadata → only relevant data retrieved
  - Smaller context → faster LLM responses, lower cost
  - top_k can stay small (5-10) and still cover the right data
"""

import os
import re
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
    flatten_threat_assessment,
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

VALIDATION_RESULTS_BUCKET = os.getenv(
    "VALIDATION_RESULTS_BUCKET", "fusefy-staging-fa39bf"
)

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
# FETCH VALIDATION RESULTS FROM S3
# ───────────────────────────────────────────────────────────────────
def fetch_validation_results(usecase_id: str) -> dict | None:
    """Fetch the latest validation-results JSON for a use case from S3.

    Path pattern: s3://{VALIDATION_RESULTS_BUCKET}/{usecaseId}/validation-results/*.json
    Returns the parsed JSON dict, or None if not found.
    """
    prefix = f"{usecase_id}/validation-results/"
    try:
        resp = s3.list_objects_v2(
            Bucket=VALIDATION_RESULTS_BUCKET, Prefix=prefix
        )
        contents = resp.get("Contents", [])
        # Exclude day*.json monitoring files — those are handled separately
        json_files = [
            obj for obj in contents
            if obj["Key"].endswith(".json")
            and not obj["Key"].rsplit("/", 1)[-1].startswith("day")
        ]
        if not json_files:
            print(f"    ℹ No validation-results found for {usecase_id}")
            return None

        # Pick the most recently modified file
        latest = max(json_files, key=lambda o: o["LastModified"])
        print(f"    📥 Fetching validation-results: {latest['Key']}")

        obj = s3.get_object(Bucket=VALIDATION_RESULTS_BUCKET, Key=latest["Key"])
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data
    except Exception as e:
        print(f"    ⚠️  Could not fetch validation-results for {usecase_id}: {e}")
        return None


# ───────────────────────────────────────────────────────────────────
# FETCH MONITORING RESULTS (day*.json) FROM S3
# ───────────────────────────────────────────────────────────────────
def fetch_monitoring_results(usecase_id: str) -> list[tuple[str, dict]]:
    """Fetch all day-wise monitoring JSON files for a use case from S3.

    Path pattern: s3://{VALIDATION_RESULTS_BUCKET}/{usecaseId}/monitoring-results/day{i}.json
    Returns a sorted list of (day_label, parsed_dict) tuples.
    """
    prefix = f"{usecase_id}/monitoring-results/"
    try:
        results = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=VALIDATION_RESULTS_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.rsplit("/", 1)[-1]
                # Match day*.json files (day1.json, day2.json, day_1.json, etc.)
                if filename.startswith("day") and filename.endswith(".json") and filename != "day.json":
                    try:
                        resp = s3.get_object(Bucket=VALIDATION_RESULTS_BUCKET, Key=key)
                        data = json.loads(resp["Body"].read().decode("utf-8"))
                        # Normalise to day_N: "day1" → "day_1", "day_1" stays "day_1"
                        raw_label = filename.replace(".json", "")
                        if "_" not in raw_label:          # day1 → day_1
                            day_label = re.sub(r"^day(\d+)$", r"day_\1", raw_label)
                        else:
                            day_label = raw_label          # already day_1
                        results.append((day_label, data))
                    except Exception as e:
                        print(f"    ⚠️  Failed to read {key}: {e}")

        # Sort by day number (day_1, day_2, ... day_10, ...)
        def _day_sort_key(item):
            try:
                return int(item[0].split("_", 1)[1])
            except (ValueError, IndexError):
                return 999999

        results.sort(key=_day_sort_key)

        if results:
            print(f"    📥 Found {len(results)} monitoring day files for {usecase_id}")
        else:
            print(f"    ℹ No monitoring day files found for {usecase_id}")
        return results

    except Exception as e:
        print(f"    ⚠️  Could not fetch monitoring results for {usecase_id}: {e}")
        return []


# ───────────────────────────────────────────────────────────────────
# FLATTEN MONITORING RESULTS (day-wise)
# ───────────────────────────────────────────────────────────────────
def flatten_monitoring_day(day_label: str, data: dict) -> list[str]:
    """Flatten a single day monitoring JSON into markdown sections."""
    day_num = day_label.replace("day_", "Day ")
    lines = [f"## AI Monitoring Results — {day_num}\n"]

    # ── Run Info ──
    lines.append("### Run Information")
    lines.append(f"- **Run ID:** {data.get('run_id', 'N/A')}")
    lines.append(f"- **Job ID:** {data.get('job_id', 'N/A')}")
    lines.append(f"- **Status:** {data.get('status', 'N/A')}")
    lines.append("")

    # ── Input Dataset ──
    lines.append("### Input Dataset")
    lines.append(f"- **Dataset ID:** {data.get('input_dataset', 'N/A')}")
    lines.append("")

    # ── Model Info ──
    lines.append("### Model")
    lines.append(f"- **Model ID:** {data.get('model_id', 'N/A')}")
    lines.append(f"- **Model Type:** {data.get('model_type', 'N/A')}")
    lines.append(f"- **Is Baseline:** {data.get('is_baseline', 'N/A')}")
    lines.append("")

    # ── Monitoring Window ──
    mw = data.get("monitoring_window", {})
    if mw:
        lines.append("### Monitoring Window")
        lines.append(f"- **Start Time:** {mw.get('start_time', 'N/A')}")
        lines.append(f"- **End Time:** {mw.get('end_time', 'N/A')}")
        lines.append("")

    # ── Metrics ──
    metrics = data.get("metrics", {})
    if metrics:
        lines.append("### Metrics\n")
        lines.append("| Metric | Value | Explanation |")
        lines.append("|--------|-------|-------------|")
        metric_keys = [k for k in metrics if not k.endswith("_explanation")]
        for key in metric_keys:
            value = metrics[key]
            explanation = metrics.get(f"{key}_explanation", "")
            lines.append(f"| {key} | {value} | {explanation} |")
        lines.append("")

    # ── Drift ──
    drift = data.get("drift", {})
    if drift:
        lines.append("### Drift Analysis")
        lines.append(f"- **Has Drift:** {drift.get('has_drift', 'N/A')}")
        lines.append(f"- **Drift Magnitude:** {drift.get('drift_magnitude', 'N/A')}")
        lines.append(f"- **Threshold:** {drift.get('threshold', 'N/A')}")
        lines.append(f"- **Drift Share:** {drift.get('drift_share', 'N/A')}")
        drifted = drift.get("drifted_features", [])
        if drifted:
            lines.append("- **Drifted Features:**")
            for f in drifted:
                lines.append(f"  - {f}")
        else:
            lines.append("- **Drifted Features:** None")
        lines.append("")

    # ── Data Quality ──
    dq = data.get("data_quality", {})
    if dq:
        lines.append("### Data Quality")
        lines.append(f"- **Missing Values:** {dq.get('missing_values', 'N/A')}")
        constant_cols = dq.get("constant_columns", [])
        if constant_cols:
            lines.append(f"- **Constant Columns:** {', '.join(str(c) for c in constant_cols)}")
        else:
            lines.append("- **Constant Columns:** None")
        lines.append("")

    # ── Alerts ──
    alerts = data.get("alerts", [])
    if alerts:
        lines.append("### Alerts\n")
        lines.append("| Type | Severity | Message |")
        lines.append("|------|----------|---------|")
        for alert in alerts:
            lines.append(
                f"| {alert.get('type', '')} "
                f"| {alert.get('severity', '')} "
                f"| {alert.get('message', '')} |"
            )
        lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# FLATTEN VALIDATION-RESULTS SECTIONS
# ───────────────────────────────────────────────────────────────────
def flatten_vr_model_info(vr: dict) -> list[str]:
    """Flatten modelInfo from validation results."""
    mi = vr.get("modelInfo")
    if not mi:
        return []
    lines = ["## AI Model Information (Validation Results)\n"]
    lines.append(f"**Model Name:** {mi.get('name', 'N/A')}")
    lines.append(f"**Version:** {mi.get('version', 'N/A')}")
    lines.append(f"**Status:** {mi.get('status', 'N/A')}")
    lines.append(f"**Last Updated:** {mi.get('lastUpdated', 'N/A')}")
    lines.append(f"**Approved By:** {mi.get('approvedBy', 'N/A')}")
    lines.append(f"**Approved Date:** {mi.get('approvedDate', 'N/A')}")
    return lines


def flatten_vr_metrics(vr: dict) -> list[str]:
    """Flatten metrics from validation results."""
    metrics = vr.get("metrics")
    if not metrics:
        return []
    lines = ["## AI Evaluation Metrics (Validation Results)\n"]
    lines.append("| Metric | Value | Explanation |")
    lines.append("|--------|-------|-------------|")

    # Separate metric values from explanations
    metric_keys = [k for k in metrics if not k.endswith("_explanation")]
    for key in metric_keys:
        value = metrics[key]
        explanation = metrics.get(f"{key}_explanation", "")
        lines.append(f"| {key} | {value} | {explanation} |")
    return lines


def flatten_vr_sbom(vr: dict) -> list[str]:
    """Flatten SBOM (Software Bill of Materials) from validation results."""
    sbom = vr.get("sbom")
    if not sbom:
        return []
    lines = ["## AI Software Bill of Materials — SBOM (Validation Results)\n"]
    lines.append(f"**Total Components:** {sbom.get('totalComponents', 'N/A')}")
    lines.append(f"**Critical Vulnerabilities:** {sbom.get('criticalVulnerabilities', 0)}")
    lines.append(f"**High Vulnerabilities:** {sbom.get('highVulnerabilities', 0)}")
    lines.append(f"**Medium Vulnerabilities:** {sbom.get('mediumVulnerabilities', 0)}")
    lines.append(f"**Low Vulnerabilities:** {sbom.get('lowVulnerabilities', 0)}")
    lines.append("")

    components = sbom.get("components", [])
    if components:
        lines.append("### Components\n")
        lines.append("| Name | Version | License | Vulnerabilities | Severity | Type |")
        lines.append("|------|---------|---------|-----------------|----------|------|")
        for comp in components:
            lines.append(
                f"| {comp.get('name', '')} "
                f"| {comp.get('version', '')} "
                f"| {comp.get('license', '')} "
                f"| {comp.get('vulnerabilities', 0)} "
                f"| {comp.get('severity', 'none')} "
                f"| {comp.get('type', '')} |"
            )
    return lines


def flatten_vr_cspm(vr: dict) -> list[str]:
    """Flatten CSPM (Cloud Security Posture Management) from validation results."""
    cspm = vr.get("cspm")
    if not cspm:
        return []
    lines = ["## AI Cloud Security Posture — CSPM (Validation Results)\n"]
    lines.append(f"**Overall Security Score:** {cspm.get('overallScore', 'N/A')}")
    lines.append("")

    policies = cspm.get("policies", [])
    if policies:
        lines.append("### Security Policies\n")
        lines.append("| Policy | Status | Score | Issues |")
        lines.append("|--------|--------|-------|--------|")
        for p in policies:
            lines.append(
                f"| {p.get('name', '')} "
                f"| {p.get('status', '')} "
                f"| {p.get('score', '')} "
                f"| {p.get('issues', 0)} |"
            )
    return lines


def flatten_vr_security_threats(vr: dict) -> list[str]:
    """Flatten securityThreats from validation results."""
    st = vr.get("securityThreats")
    if not st:
        return []
    lines = ["## AI Security Threats (Validation Results)\n"]

    # Prompt Injection
    pi = st.get("promptInjection", {})
    if pi:
        lines.append("### Prompt Injection")
        lines.append(f"- **Risk Level:** {pi.get('riskLevel', 'N/A')}")
        lines.append(f"- **Detected Attempts:** {pi.get('detectedAttempts', 0)}")
        lines.append(f"- **Blocked Attempts:** {pi.get('blockedAttempts', 0)}")
        lines.append(f"- **Success Rate:** {pi.get('successRate', 0)}%")
        patterns = pi.get("commonPatterns", [])
        if patterns:
            lines.append("- **Common Patterns:**")
            for p in patterns:
                lines.append(f"  - {p}")
        lines.append("")

    # Tool Abuse
    ta = st.get("toolAbuse", {})
    if ta:
        lines.append("### Tool Abuse")
        lines.append(f"- **Risk Level:** {ta.get('riskLevel', 'N/A')}")
        lines.append(f"- **Detected Attempts:** {ta.get('detectedAttempts', 0)}")
        lines.append(f"- **Blocked Attempts:** {ta.get('blockedAttempts', 0)}")
        lines.append(f"- **Success Rate:** {ta.get('successRate', 0)}%")
        patterns = ta.get("commonPatterns", [])
        if patterns:
            lines.append("- **Common Patterns:**")
            for p in patterns:
                lines.append(f"  - {p}")
        lines.append("")

    # Threat Detection Summary
    td = st.get("threatDetection", [])
    if td:
        lines.append("### Threat Detection Summary\n")
        lines.append("| Threat Type | Detected | Blocked |")
        lines.append("|-------------|----------|---------|")
        for t in td:
            lines.append(
                f"| {t.get('type', '')} "
                f"| {t.get('detected', 0)} "
                f"| {t.get('blocked', 0)} |"
            )
    return lines


def flatten_vr_chart_data(vr: dict) -> list[str]:
    """Flatten chartData from validation results."""
    cd = vr.get("chartData")
    if not cd:
        return []
    lines = ["## AI Monitoring Chart Data (Validation Results)\n"]

    # Metrics over time
    mot = cd.get("metricsOverTime", [])
    if mot:
        lines.append("### Metrics Over Time\n")
        if mot:
            # Get all metric keys (excluding 'date')
            metric_keys = [k for k in mot[0].keys() if k != "date"]
            header = "| Date | " + " | ".join(metric_keys) + " |"
            separator = "|------| " + " | ".join(["---"] * len(metric_keys)) + " |"
            lines.append(header)
            lines.append(separator)
            for entry in mot:
                row = f"| {entry.get('date', '')} | "
                row += " | ".join(str(entry.get(k, "")) for k in metric_keys)
                row += " |"
                lines.append(row)
        lines.append("")

    # Threat detection chart data
    td = cd.get("threatDetection", [])
    if td:
        lines.append("### Threat Detection Chart Data\n")
        lines.append("| Threat Type | Detected | Blocked |")
        lines.append("|-------------|----------|---------|")
        for t in td:
            lines.append(
                f"| {t.get('type', '')} "
                f"| {t.get('detected', 0)} "
                f"| {t.get('blocked', 0)} |"
            )
    return lines


def flatten_vr_agent_evaluators(vr: dict) -> list[str]:
    """Flatten agentEvaluators from validation results."""
    evals = vr.get("agentEvaluators", [])
    if not evals:
        return []
    lines = ["## AI Agent Evaluators (Validation Results)\n"]
    lines.append("| Evaluator | Signal | Definition | Judging Method |")
    lines.append("|-----------|--------|------------|----------------|")
    for ev in evals:
        lines.append(
            f"| {ev.get('name', '')} "
            f"| {ev.get('signal', '')} "
            f"| {ev.get('definition', '')} "
            f"| {ev.get('judging_method', '')} |"
        )
    return lines


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
    ("07b_threat_assessment",  flatten_threat_assessment,  "threat_assessment"),
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

            # Build extra metadata for threat assessment section
            extra_meta = None
            if extra_arg == "threat_assessment":
                ta = record.get("threatAssessment", {})
                if ta and isinstance(ta, dict):
                    ta_summary = ta.get("summary", {})
                    sb = ta_summary.get("statusBreakdown", {}) if isinstance(ta_summary, dict) else {}
                    rp = ta.get("riskPosture", {})
                    extra_meta = {
                        "threat_framework_name": ta.get("frameworkName", ""),
                        "threat_risk_level": rp.get("riskLevel", "") if isinstance(rp, dict) else "",
                        "threat_total_applicable_controls": int(ta_summary.get("totalApplicableControls", 0)) if isinstance(ta_summary, dict) else 0,
                        "threat_total_framework_controls": int(ta_summary.get("totalFrameworkControls", 0)) if isinstance(ta_summary, dict) else 0,
                        "threat_status_met": int(sb.get("Met", 0)),
                        "threat_status_implemented": int(sb.get("Implemented", 0)),
                        "threat_status_not_met": int(sb.get("Not Met", 0)),
                        "threat_status_risk_accepted": int(sb.get("Risk Accepted", 0)),
                        "threat_status_not_applicable": int(sb.get("Not Applicable", 0)),
                    }

            metadata = build_section_metadata(record, section_name, extra=extra_meta)
            upload_section(S3_BUCKET, folder, section_name, content, metadata)
            uploaded += 1

        except Exception as e:
            print(f"    ✗ {section_name} failed: {e}")

    # ── Validation Results sections (from S3 — AI Eval & Monitoring) ──
    validation_data = fetch_validation_results(uc_id)
    if validation_data:
        VR_SECTION_DEFS = [
            ("13_ai_model_info",        flatten_vr_model_info),
            ("14_ai_eval_metrics",      flatten_vr_metrics),
            ("15_ai_sbom",              flatten_vr_sbom),
            ("16_ai_cspm",              flatten_vr_cspm),
            ("17_ai_security_threats",   flatten_vr_security_threats),
            ("18_ai_chart_data",        flatten_vr_chart_data),
            ("19_ai_agent_evaluators",  flatten_vr_agent_evaluators),
        ]
        for section_name, flatten_fn in VR_SECTION_DEFS:
            try:
                vr_lines = flatten_fn(validation_data)
                if not vr_lines:
                    continue
                content = "\n".join(vr_lines) if isinstance(vr_lines, list) else str(vr_lines)
                if not content.strip():
                    continue
                content = uc_header + content
                metadata = build_section_metadata(
                    record, section_name,
                    extra={"data_source": "validation-results"},
                )
                upload_section(S3_BUCKET, folder, section_name, content, metadata)
                uploaded += 1
            except Exception as e:
                print(f"    ✗ {section_name} failed: {e}")
    else:
        print(f"    ℹ Skipping validation-results sections (no data) for {uc_id}")

    # ── Monitoring Results — day-wise (from S3) ──
    monitoring_days = fetch_monitoring_results(uc_id)
    if monitoring_days:
        for day_label, day_data in monitoring_days:
            try:
                mon_lines = flatten_monitoring_day(day_label, day_data)
                if not mon_lines:
                    continue
                content = "\n".join(mon_lines) if isinstance(mon_lines, list) else str(mon_lines)
                if not content.strip():
                    continue
                content = uc_header + content

                section_name = f"20_monitoring_{day_label}"
                # Pull monitoring window dates for metadata
                mw = day_data.get("monitoring_window", {})
                extra_meta = {
                    "data_source": "monitoring-results",
                    "monitoring_day": day_label,
                    "run_id": day_data.get("run_id", ""),
                    "model_id": day_data.get("model_id", ""),
                    "monitoring_start": mw.get("start_time", ""),
                    "monitoring_end": mw.get("end_time", ""),
                    "has_drift": str(day_data.get("drift", {}).get("has_drift", "")),
                    "monitoring_status": day_data.get("status", ""),
                }
                metadata = build_section_metadata(record, section_name, extra=extra_meta)
                upload_section(S3_BUCKET, folder, section_name, content, metadata)
                uploaded += 1
            except Exception as e:
                print(f"    ✗ 20_monitoring_{day_label} failed: {e}")
    else:
        print(f"    ℹ Skipping monitoring day sections (no data) for {uc_id}")

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
