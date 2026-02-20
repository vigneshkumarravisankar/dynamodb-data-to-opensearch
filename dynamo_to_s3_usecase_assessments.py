"""
Pipeline: staging-fusefy-usecaseAssessments (tenant) → S3 → Bedrock KB

This is the AI use case assessment table — each record is a full AI project
assessment with deeply nested data including:
  - Use case overview (model name, purpose, approach, risk level)
  - AI Bill of Material (frameworks, platforms, hardware, inputs/outputs)
  - Data Bill of Material (datasets, lineage, validation)
  - Assessment Result / Jira Stories (missing components to implement vs valid)
  - Risk & Controls analysis (risk categories, control coverage, insights)
  - Design Document (architecture, APIs, agents, data model, security)
  - TCO (Total Cost of Ownership breakdown)
  - Metrics (performance thresholds)
  - Rollout Plan (phased deployment)

Each use case gets its own enriched markdown document in S3.
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
S3_PREFIX = "usecase-assessments"
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")
DATA_SOURCE_ID = os.getenv("DATA_SOURCE_ID", "GT9B3WZTOE")

USECASE_TABLE = os.getenv(
    "DYNAMODB_USECASE_ASSESSMENTS_TENANT_TABLE",
    "staging-fusefy-usecaseAssessments-d66cb7c7-04ac-4634-927f-06d91afa39bf"
)
FRAMEWORKS_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")

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
# 3. HELPER: safely get nested values
# ───────────────────────────────────────────────────────────────────
def safe_get(d, *keys, default=""):
    """Safely traverse nested dicts."""
    current = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
    return current if current is not None else default


def format_list(items, bullet="- "):
    """Format a list of items as bullet points."""
    if not items:
        return ""
    if isinstance(items, list):
        return "\n".join(f"{bullet}{item}" for item in items if item)
    return str(items)


# ───────────────────────────────────────────────────────────────────
# 4. FLATTEN: Use Case Overview
# ───────────────────────────────────────────────────────────────────
def flatten_overview(record: dict, framework_lookup: dict = None) -> list[str]:
    """Flatten the top-level use case fields."""
    if framework_lookup is None:
        framework_lookup = {}
    lines = []
    uc_id = record.get("id", "Unknown")
    model_name = record.get("modelName", "Unknown")

    lines.append(f"# AI Use Case: {model_name}")
    lines.append(f"**Use Case ID:** {uc_id}")
    lines.append(f"**Inventory ID:** {record.get('inventoryId', uc_id)}")
    lines.append("")

    # Core details
    if record.get("modelSummary"):
        lines.append(f"**Summary:** {record['modelSummary']}")
    if record.get("modelDescription"):
        lines.append(f"**Description:** {record['modelDescription']}")
    if record.get("modelPurpose"):
        lines.append(f"**Purpose:** {record['modelPurpose']}")
    if record.get("modelUsage"):
        lines.append(f"**Usage:** {record['modelUsage']}")
    if record.get("modelInput"):
        lines.append(f"**Model Input:** {record['modelInput']}")
    if record.get("modelOutput"):
        lines.append(f"**Model Output:** {record['modelOutput']}")

    lines.append("")

    # Business context
    if record.get("businessUsage"):
        lines.append(f"**Business Usage:** {record['businessUsage']}")
    if record.get("currentBusinessUsage"):
        lines.append(f"**Current Business Usage (Before AI):** {record['currentBusinessUsage']}")
    if record.get("keyActivity"):
        lines.append(f"**Key Activity:** {record['keyActivity']}")

    lines.append("")

    # Classification
    if record.get("aiApproach"):
        lines.append(f"**AI Approach:** {record['aiApproach']}")
    if record.get("aiCategory"):
        lines.append(f"**AI Category:** {record['aiCategory']}")
    if record.get("AIMethodologyType"):
        lines.append(f"**AI Methodology:** {record['AIMethodologyType']}")
    if record.get("baseModelName"):
        lines.append(f"**Base LLM Model:** {record['baseModelName']}")
    if record.get("aiCloudProvider"):
        lines.append(f"**Cloud Provider:** {record['aiCloudProvider']}")
    if record.get("platform"):
        lines.append(f"**Platform/Tech Stack:** {record['platform']}")
    if record.get("development"):
        lines.append(f"**Development:** {record['development']}")

    lines.append("")

    # Risk & classification
    if record.get("overallRisk"):
        lines.append(f"**Overall Risk:** {record['overallRisk']}")
    if record.get("impact"):
        lines.append(f"**Impact:** {record['impact']}")
    if record.get("priorityType"):
        lines.append(f"**Priority Type:** {record['priorityType']}")
    if record.get("level") is not None:
        lines.append(f"**AI Maturity Level:** {record['level']}")

    lines.append("")

    # Organization
    if record.get("department"):
        lines.append(f"**Department:** {record['department']}")
    if record.get("sector"):
        lines.append(f"**Sector:** {record['sector']}")
    if record.get("targetDivision"):
        lines.append(f"**Target Division:** {record['targetDivision']}")
    if record.get("primaryContact"):
        lines.append(f"**Primary Contact:** {record['primaryContact']}")
    if record.get("useFrequency"):
        lines.append(f"**Use Frequency:** {record['useFrequency']}")
    if record.get("status"):
        lines.append(f"**Jira Stories Status:** {record['status']}")
    if record.get("processStatus"):
        lines.append(f"**TCO Generation Status:** {record['processStatus']}")
    if record.get("aiArchitectureGeneratingStatus"):
        lines.append(f"**Architecture Diagram Generation Status:** {record['aiArchitectureGeneratingStatus']}")
    if record.get("aiFeatureGeneratingStatus"):
        lines.append(f"**Jira Stories Generation Status:** {record['aiFeatureGeneratingStatus']}")
    if record.get("aiProgressGeneratingStatus"):
        lines.append(f"**Design Document Generation Status:** {record['aiProgressGeneratingStatus']}")
    if record.get("vendorName"):
        lines.append(f"**Vendor:** {record['vendorName']}")

    # Data labels
    if record.get("dataLabels"):
        labels = record["dataLabels"]
        if isinstance(labels, list):
            lines.append(f"**Data Labels:** {', '.join(str(l) for l in labels)}")

    if record.get("dataLineage"):
        lines.append(f"**Data Lineage:** {record['dataLineage']}")

    if record.get("processCategories"):
        cats = record["processCategories"]
        if isinstance(cats, list):
            lines.append(f"**Process Categories:** {', '.join(str(c) for c in cats)}")

    if record.get("riskframeworkid"):
        fw_id = record["riskframeworkid"]
        fw = framework_lookup.get(fw_id)
        if fw:
            lines.append("")
            lines.append(f"### Risk Framework")
            lines.append(f"**Framework ID:** {fw_id}")
            if fw.get("name"):
                lines.append(f"**Framework Name:** {fw['name']}")
            if fw.get("description"):
                lines.append(f"**Framework Description:** {fw['description']}")
            if fw.get("owner"):
                lines.append(f"**Framework Owner:** {fw['owner']}")
            if fw.get("regions"):
                regions = fw["regions"]
                if isinstance(regions, list):
                    lines.append(f"**Framework Regions:** {', '.join(str(r) for r in regions)}")
                else:
                    lines.append(f"**Framework Regions:** {regions}")
            if fw.get("verticals"):
                verticals = fw["verticals"]
                if isinstance(verticals, list):
                    lines.append(f"**Framework Verticals:** {', '.join(str(v) for v in verticals)}")
                else:
                    lines.append(f"**Framework Verticals:** {verticals}")
            if fw.get("assessmentCategories"):
                cats = fw["assessmentCategories"]
                if isinstance(cats, list):
                    lines.append(f"**Assessment Categories:** {', '.join(str(c) for c in cats)}")
                else:
                    lines.append(f"**Assessment Categories:** {cats}")
        else:
            lines.append(f"**Risk Framework ID:** {fw_id}")

    return lines


# ───────────────────────────────────────────────────────────────────
# 5. FLATTEN: AI Bill of Material
# ───────────────────────────────────────────────────────────────────
def flatten_ai_bom(record: dict) -> list[str]:
    """Flatten the AI Bill of Material."""
    bom = record.get("aiBillOfMaterial")
    if not bom or not isinstance(bom, list):
        return []

    lines = []
    lines.append("")
    lines.append("## AI Bill of Material")
    lines.append("")

    for i, item in enumerate(bom, 1):
        if not isinstance(item, dict):
            continue

        lines.append(f"### Component Set {i}")
        if item.get("aiFramework"):
            lines.append(f"- **AI Framework:** {item['aiFramework']}")
        if item.get("aiPlatform"):
            lines.append(f"- **AI Platform:** {item['aiPlatform']}")
        if item.get("foundationalLLMProvider"):
            lines.append(f"- **LLM Provider:** {item['foundationalLLMProvider']}")
        if item.get("fineTunedFrom"):
            lines.append(f"- **Fine-Tuned From:** {item['fineTunedFrom']}")
        if item.get("components"):
            lines.append(f"- **Components:** {item['components']}")
        if item.get("hardware"):
            lines.append(f"- **Hardware:** {item['hardware']}")
        if item.get("software"):
            lines.append(f"- **Software:** {item['software']}")
        if item.get("hostingType"):
            lines.append(f"- **Hosting:** {item['hostingType']}")
        if item.get("input"):
            lines.append(f"- **Input:** {item['input']}")
        if item.get("output"):
            lines.append(f"- **Output:** {item['output']}")
        if item.get("piiDataProcessed"):
            lines.append(f"- **PII Data Processed:** {item['piiDataProcessed']}")
        if item.get("humanInLoopRequired"):
            lines.append(f"- **Human-in-Loop Required:** {item['humanInLoopRequired']}")
        if item.get("thirdParty"):
            lines.append(f"- **Third Party:** {item['thirdParty']}")
        if item.get("softwareRequiredForExecution"):
            lines.append(f"- **Software Required for Execution:** {item['softwareRequiredForExecution']}")
        lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 6. FLATTEN: Data Bill of Material
# ───────────────────────────────────────────────────────────────────
def flatten_data_bom(record: dict) -> list[str]:
    """Flatten the Data Bill of Material."""
    bom = record.get("dataBillOfMaterial")
    if not bom or not isinstance(bom, list):
        return []

    lines = []
    lines.append("")
    lines.append("## Data Bill of Material")
    lines.append("")

    for item in bom:
        if not isinstance(item, dict):
            continue
        name = item.get("datasetName", "Unknown Dataset")
        lines.append(f"### {name}")
        if item.get("version"):
            lines.append(f"- **Version:** {item['version']}")
        if item.get("dataValidation"):
            lines.append(f"- **Data Validation:** {item['dataValidation']}")
        if item.get("lineage"):
            lines.append(f"- **Lineage:** {item['lineage']}")
        lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 7. FLATTEN: Metrics
# ───────────────────────────────────────────────────────────────────
def flatten_metrics(record: dict) -> list[str]:
    """Flatten performance metrics."""
    metrics = record.get("metrics")
    if not metrics or not isinstance(metrics, list):
        return []

    lines = []
    lines.append("")
    lines.append("## Performance Metrics")
    lines.append("")

    for m in metrics:
        if not isinstance(m, dict):
            continue
        name = m.get("metricName", "Unknown")
        lines.append(f"### {name}")
        if m.get("metricDescription"):
            lines.append(f"- **Description:** {m['metricDescription']}")
        if m.get("threshold") is not None:
            unit = m.get("thresholdUnit", "")
            eval_type = m.get("thresholdEvaluation", "")
            lines.append(f"- **Threshold:** {m['threshold']} {unit} ({eval_type})")
        if m.get("relatedAiComponents"):
            comps = m["relatedAiComponents"]
            if isinstance(comps, list):
                lines.append(f"- **AI Components:** {', '.join(str(c) for c in comps)}")
        if m.get("relatedDatasets"):
            ds = m["relatedDatasets"]
            if isinstance(ds, list):
                lines.append(f"- **Datasets:** {', '.join(str(d) for d in ds)}")
        lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 8. FLATTEN: Jira Stories (assessmentResult)
# ───────────────────────────────────────────────────────────────────
def flatten_jira_stories(record: dict) -> list[str]:
    """
    Flatten assessmentResult into Jira Stories.
    - missingComponents = features/stories to implement
    - validComponents = already implemented
    """
    assessment = record.get("assessmentResult")
    if not assessment or not isinstance(assessment, list):
        return []

    lines = []
    lines.append("")
    lines.append("## Jira Stories (Assessment Result)")
    lines.append("")

    for result_set in assessment:
        if not isinstance(result_set, dict):
            continue

        # ── Missing Components (to implement) ──
        missing = result_set.get("missingComponents", [])
        if missing and isinstance(missing, list):
            lines.append(f"### Stories to Implement ({len(missing)} items)")
            lines.append("")

            for mc in missing:
                if not isinstance(mc, dict):
                    continue

                mc_id = mc.get("id", "")
                feature = mc.get("feature", "Unknown Feature")
                issue_id = mc.get("issueId", "")
                days = mc.get("numberOfDays", "")
                lifecycle = mc.get("aiLifecycleStage", "")
                desc = mc.get("featureDescription", "")

                lines.append(f"#### {feature}")
                if issue_id:
                    lines.append(f"- **Jira Issue:** {issue_id}")
                if mc_id:
                    lines.append(f"- **Component ID:** {mc_id}")
                if lifecycle:
                    lines.append(f"- **AI Lifecycle Stage:** {lifecycle}")
                if days:
                    lines.append(f"- **Estimated Days:** {days}")
                if desc:
                    lines.append(f"- **Description:** {desc}")

                # Acceptance Criteria
                criteria = mc.get("acceptanceCriteria", [])
                if criteria and isinstance(criteria, list):
                    lines.append(f"- **Acceptance Criteria:**")
                    for ac in criteria:
                        lines.append(f"  - {ac}")

                # Associated Controls with Deployment Stages
                ctrl_stages = mc.get("associatedControlIdStages", {})
                if ctrl_stages and isinstance(ctrl_stages, dict):
                    lines.append(f"- **Associated Controls:**")
                    for ctrl_id, ctrl_info in ctrl_stages.items():
                        if isinstance(ctrl_info, dict):
                            ctrl_name_list = ctrl_info.get("name", [])
                            if isinstance(ctrl_name_list, list) and ctrl_name_list:
                                ctrl_display = ctrl_name_list[-1]
                                ctrl_hierarchy = " > ".join(str(n) for n in ctrl_name_list)
                            else:
                                ctrl_display = str(ctrl_name_list)
                                ctrl_hierarchy = None

                            deploy_stages = ctrl_info.get("deploymentStages", [])
                            if isinstance(deploy_stages, list):
                                stages_str = ", ".join(str(s) for s in deploy_stages)
                            else:
                                stages_str = str(deploy_stages)

                            lines.append(f"  - **{ctrl_id}** — {ctrl_display}")
                            if ctrl_hierarchy:
                                lines.append(f"    - Hierarchy: {ctrl_hierarchy}")
                            lines.append(f"    - Deployment Stages: {stages_str}")
                        else:
                            lines.append(f"  - {ctrl_id}")

                # Gap / Sub-tasks
                gaps = mc.get("gap", [])
                if gaps and isinstance(gaps, list):
                    lines.append(f"- **Sub-tasks (Gaps):**")
                    for gap in gaps:
                        if isinstance(gap, dict):
                            gap_name = gap.get("gapName", "")
                            task_id = gap.get("taskid", "")
                            if task_id:
                                lines.append(f"  - [{task_id}] {gap_name}")
                            else:
                                lines.append(f"  - {gap_name}")

                lines.append("")

        # ── Valid Components (already implemented) ──
        valid = result_set.get("validComponents", [])
        if valid and isinstance(valid, list) and len(valid) > 0:
            lines.append(f"### Already Implemented ({len(valid)} items)")
            lines.append("")
            for vc in valid:
                if isinstance(vc, dict):
                    feature = vc.get("feature", "Unknown")
                    lines.append(f"- ✅ **{feature}**")
                    if vc.get("featureDescription"):
                        lines.append(f"  - {vc['featureDescription']}")
                else:
                    lines.append(f"- ✅ {vc}")
            lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 9. FLATTEN: Risk & Controls
# ───────────────────────────────────────────────────────────────────
def flatten_risk_and_controls(record: dict) -> list[str]:
    """Flatten the risk and controls analysis."""
    rac = record.get("riskAndControls")
    if not rac or not isinstance(rac, dict):
        return []

    lines = []
    lines.append("")
    lines.append("## Risk & Controls Analysis")
    lines.append("")

    # Summary
    summary = rac.get("summary", {})
    if summary and isinstance(summary, dict):
        lines.append(f"**Overall Risk Posture:** {summary.get('overallRiskPosture', 'N/A')}")
        lines.append(f"**Risks Identified:** {summary.get('risksIdentified', 'N/A')}")
        lines.append(f"**Applicable Controls:** {summary.get('applicableControls', 'N/A')}")
        lines.append(f"**Total Framework Controls:** {summary.get('totalFrameworkControls', 'N/A')}")

        sev = summary.get("severityBreakdown", {})
        if sev and isinstance(sev, dict):
            parts = []
            for level in ["critical", "high", "medium", "low"]:
                if sev.get(level) is not None:
                    parts.append(f"{level.capitalize()}: {sev[level]}")
            if parts:
                lines.append(f"**Severity Breakdown:** {', '.join(parts)}")
        lines.append("")

    # Framework context
    fw_ctx = rac.get("frameworkContext", {})
    if fw_ctx and isinstance(fw_ctx, dict):
        if fw_ctx.get("name"):
            lines.append(f"**Framework:** {fw_ctx['name']}")
        if fw_ctx.get("description"):
            lines.append(f"**Framework Description:** {fw_ctx['description']}")
        lines.append("")

    # Assessment Insights
    insights = rac.get("assessmentInsights", {})
    if insights and isinstance(insights, dict):
        lines.append("### Assessment Insights")
        if insights.get("riskApplicabilitySummary"):
            lines.append(f"- **Risk Applicability:** {insights['riskApplicabilitySummary']}")
        if insights.get("residualRiskOverview"):
            lines.append(f"- **Residual Risk:** {insights['residualRiskOverview']}")
        if insights.get("keyTakeaways"):
            lines.append(f"- **Key Takeaways:** {insights['keyTakeaways']}")
        lines.append("")

    # Control Coverage
    coverage = rac.get("controlCoverage", {})
    if coverage and isinstance(coverage, dict):
        lines.append("### Control Coverage")
        covered = coverage.get("covered", [])
        if covered and isinstance(covered, list):
            lines.append(f"- **Covered ({len(covered)}):** {', '.join(str(c) for c in covered)}")
        partial = coverage.get("partial", [])
        if partial and isinstance(partial, list):
            lines.append(f"- **Partial ({len(partial)}):** {', '.join(str(c) for c in partial)}")
        gap = coverage.get("gap", [])
        if gap and isinstance(gap, list):
            lines.append(f"- **Gap ({len(gap)}):** {', '.join(str(c) for c in gap)}")
        lines.append("")

    # Risk Categories
    risk_cats = rac.get("riskCategories", [])
    if risk_cats and isinstance(risk_cats, list):
        lines.append("### Risk Categories")
        lines.append("")
        for cat in risk_cats:
            if not isinstance(cat, dict):
                continue
            cat_name = cat.get("categoryName", "Unknown")
            lines.append(f"#### {cat_name}")

            risks = cat.get("risks", [])
            if isinstance(risks, list):
                for risk in risks:
                    if not isinstance(risk, dict):
                        continue
                    risk_id = risk.get("riskId", "")
                    severity = risk.get("severity", "")
                    reason = risk.get("applicabilityReason", "")

                    lines.append(f"- **{risk_id}** (Severity: {severity})")
                    if reason:
                        lines.append(f"  - Reason: {reason}")

                    mapped = risk.get("mappedControlIds", [])
                    if isinstance(mapped, list):
                        for ctrl in mapped:
                            if isinstance(ctrl, dict):
                                ctrl_id = ctrl.get("controlId", "")
                                ctrl_name = ctrl.get("controlName", "")
                                ctrl_desc = ctrl.get("controlDescription", "")
                                lines.append(f"  - Control: **{ctrl_id}** — {ctrl_name}")
                                if ctrl_desc:
                                    lines.append(f"    - {ctrl_desc}")
            lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 10. FLATTEN: Design Document
# ───────────────────────────────────────────────────────────────────
def flatten_design_document(record: dict) -> list[str]:
    """Flatten the design document into readable sections."""
    dd = record.get("designDocument")
    if not dd or not isinstance(dd, dict):
        return []

    lines = []
    lines.append("")
    lines.append("## Technical Design Document")
    lines.append("")

    # Intro
    if dd.get("intro"):
        lines.append(f"**Overview:** {dd['intro']}")
        lines.append("")

    # Context
    ctx = dd.get("context", {})
    if ctx and isinstance(ctx, dict):
        if ctx.get("platform"):
            lines.append(f"**Platform:** {ctx['platform']}")
        if ctx.get("riskFrameworkLabel"):
            lines.append(f"**Risk Framework:** {ctx['riskFrameworkLabel']}")
        hints = ctx.get("domainHints", [])
        if isinstance(hints, list) and hints:
            lines.append(f"**Domain:** {', '.join(str(h) for h in hints)}")
        lines.append("")

    # Cloud Architecture
    cloud = dd.get("cloudArchitecture", {})
    if cloud and isinstance(cloud, dict):
        lines.append("### Cloud Architecture")
        if cloud.get("architectureSketch"):
            lines.append(f"{cloud['architectureSketch']}")
        comps = cloud.get("cloudComponents", {})
        if isinstance(comps, dict) and comps:
            lines.append("")
            for comp_name, comp_val in comps.items():
                lines.append(f"- **{comp_name}:** {comp_val}")
        lines.append("")

    # Domain & Entities
    domain = dd.get("domainAndEntities", {})
    if domain and isinstance(domain, dict):
        lines.append("### Domain & Entities")
        if domain.get("coreDomain"):
            lines.append(f"**Core Domain:** {domain['coreDomain']}")
        bcs = domain.get("boundedContexts", [])
        if isinstance(bcs, list):
            for bc in bcs:
                if isinstance(bc, dict):
                    lines.append(f"\n**{bc.get('name', 'Unknown')}:** {bc.get('description', '')}")
                    entities = bc.get("entities", [])
                    if isinstance(entities, list):
                        for ent in entities:
                            if isinstance(ent, dict):
                                attrs = ent.get("keyAttributes", [])
                                attrs_str = ", ".join(str(a) for a in attrs) if isinstance(attrs, list) else str(attrs)
                                lines.append(f"- Entity: **{ent.get('name', '')}** — Attributes: {attrs_str}")
        lines.append("")

    # API Design
    api = dd.get("apiDesign", {})
    if api and isinstance(api, dict):
        lines.append("### API Design")
        if api.get("apiNotes"):
            lines.append(f"{api['apiNotes']}")
        services = api.get("services", [])
        if isinstance(services, list):
            for svc in services:
                if isinstance(svc, dict):
                    lines.append(f"\n**Service: {svc.get('serviceName', 'Unknown')}**")
                    if svc.get("description"):
                        lines.append(f"{svc['description']}")
                    endpoints = svc.get("endpoints", [])
                    if isinstance(endpoints, list):
                        for ep in endpoints:
                            if isinstance(ep, dict):
                                method = ep.get("method", "")
                                path = ep.get("path", "")
                                summary = ep.get("summary", "")
                                lines.append(f"- `{method} {path}` — {summary}")
                                roles = ep.get("requiredRoles", [])
                                if isinstance(roles, list) and roles:
                                    lines.append(f"  - Required Roles: {', '.join(str(r) for r in roles)}")
        lines.append("")

    # Agent Integration
    agent = dd.get("agentIntegration", {})
    if agent and isinstance(agent, dict):
        lines.append("### Agent Integration")
        persona = agent.get("agentPersona", {})
        if isinstance(persona, dict):
            if persona.get("name"):
                lines.append(f"**Agent Name:** {persona['name']}")
            if persona.get("primaryGoal"):
                lines.append(f"**Primary Goal:** {persona['primaryGoal']}")
            if persona.get("personaDescription"):
                lines.append(f"**Description:** {persona['personaDescription']}")
        if agent.get("agentSecurityNotes"):
            lines.append(f"**Security Notes:** {agent['agentSecurityNotes']}")

        # MCP Design
        mcp = agent.get("mcpDesign", {})
        if isinstance(mcp, dict):
            if mcp.get("inputValidationAndSanitization"):
                lines.append(f"**Input Validation:** {mcp['inputValidationAndSanitization']}")
            if mcp.get("runtimePolicyEnforcement"):
                lines.append(f"**Runtime Policy:** {mcp['runtimePolicyEnforcement']}")
            mcp_comps = mcp.get("components", [])
            if isinstance(mcp_comps, list):
                for comp in mcp_comps:
                    if isinstance(comp, dict):
                        lines.append(f"- **{comp.get('name', '')}:** {comp.get('purpose', '')} — {comp.get('details', '')}")

        # Tools Mapping
        tools = agent.get("toolsMapping", [])
        if isinstance(tools, list):
            lines.append("\n**Agent Tools:**")
            for tool in tools:
                if isinstance(tool, dict):
                    lines.append(f"- **{tool.get('toolName', '')}** → `{tool.get('mappedApiEndpoint', '')}` — {tool.get('description', '')}")
        lines.append("")

    # Data Model
    dm = dd.get("dataModel", {})
    if dm and isinstance(dm, dict):
        lines.append("### Data Model")
        if dm.get("schemaNotes"):
            lines.append(f"{dm['schemaNotes']}")
        if dm.get("relationshipSketch"):
            lines.append(f"**Relationships:** {dm['relationshipSketch']}")
        tables = dm.get("priorityTablesDDL", [])
        if isinstance(tables, list):
            for tbl in tables:
                if isinstance(tbl, dict):
                    lines.append(f"\n**Table: {tbl.get('tableName', 'Unknown')}**")
                    if tbl.get("ddl"):
                        lines.append(f"```sql\n{tbl['ddl']}\n```")
        lines.append("")

    # Security & Compliance
    sec = dd.get("securityCompliance", {})
    if sec and isinstance(sec, dict):
        lines.append("### Security & Compliance")
        controls_map = sec.get("controlsMapping", [])
        if isinstance(controls_map, list):
            for ctrl in controls_map:
                if isinstance(ctrl, dict):
                    lines.append(
                        f"- **{ctrl.get('controlId', '')}** ({ctrl.get('controlName', '')}) "
                        f"→ {ctrl.get('mappedComponent', '')} — {ctrl.get('justification', '')}"
                    )
        supply = sec.get("supplyChainAndCloudPosture", {})
        if isinstance(supply, dict):
            if supply.get("sbomStrategy"):
                lines.append(f"- **SBOM Strategy:** {supply['sbomStrategy']}")
            if supply.get("cspmStrategy"):
                lines.append(f"- **CSPM Strategy:** {supply['cspmStrategy']}")
        lines.append("")

    # Model Process / Lifecycle Stages
    mp = dd.get("modelProcess", {})
    if mp and isinstance(mp, dict):
        stages = mp.get("stages", [])
        if isinstance(stages, list) and stages:
            lines.append("### AI Lifecycle Stages")
            for stage in stages:
                if isinstance(stage, dict):
                    lines.append(f"\n**{stage.get('name', 'Unknown')}**")
                    if stage.get("businessContext"):
                        lines.append(f"- Business: {stage['businessContext']}")
                    if stage.get("technicalContext"):
                        lines.append(f"- Technical: {stage['technicalContext']}")
                    if stage.get("implementationSteps"):
                        lines.append(f"- Implementation: {stage['implementationSteps']}")
                    if stage.get("integration"):
                        lines.append(f"- Integration: {stage['integration']}")
            lines.append("")

    # Delivery Plan
    dp = dd.get("deliveryPlan", [])
    if dp and isinstance(dp, list):
        lines.append("### Delivery Plan")
        for phase in dp:
            if isinstance(phase, dict):
                lines.append(f"- **{phase.get('phase', 'Unknown')}:** {phase.get('description', '')}")
        lines.append("")

    # Frontend & UX
    fux = dd.get("frontendAndUx", {})
    if fux and isinstance(fux, dict):
        pages = fux.get("pages", [])
        if isinstance(pages, list) and pages:
            lines.append("### Frontend & UX")
            for page in pages:
                if isinstance(page, dict):
                    lines.append(f"\n**Page: {page.get('pageName', 'Unknown')}**")
                    if page.get("purpose"):
                        lines.append(f"- Purpose: {page['purpose']}")
                    ui_elems = page.get("keyUIElements", [])
                    if isinstance(ui_elems, list):
                        lines.append(f"- UI Elements: {', '.join(str(e) for e in ui_elems)}")
                    trigger = page.get("agentTriggerElement", {})
                    if isinstance(trigger, dict) and trigger.get("elementName"):
                        lines.append(f"- Agent Trigger: **{trigger['elementName']}** — {trigger.get('behavior', '')}")
            lines.append("")

    # Third-party
    tpi = dd.get("thirdPartyIntegration", {})
    if tpi and isinstance(tpi, dict):
        tools_list = tpi.get("tools", [])
        if isinstance(tools_list, list) and tools_list:
            lines.append("### Third-Party Integrations")
            for tool in tools_list:
                if isinstance(tool, dict):
                    lines.append(f"- **{tool.get('toolName', '')}:** {tool.get('purpose', '')} — {tool.get('notes', '')}")
            lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 11. FLATTEN: Rollout Plan & Epics
# ───────────────────────────────────────────────────────────────────
def flatten_rollout_and_epics(record: dict) -> list[str]:
    """Flatten rollout plan and epic list."""
    lines = []

    # Epic List
    epics = record.get("epicList", [])
    if epics and isinstance(epics, list):
        lines.append("")
        lines.append("## Jira Epics")
        lines.append("")
        for epic in epics:
            if isinstance(epic, dict):
                lines.append(f"- **{epic.get('epicKey', '')}:** {epic.get('epicName', '')}")
        lines.append("")

    # Rollout Plan
    rollout = record.get("rolloutPlan", [])
    if rollout and isinstance(rollout, list):
        lines.append("## Rollout Plan")
        lines.append("")
        for phase in rollout:
            if isinstance(phase, dict):
                phase_name = phase.get("phase", "Unknown")
                group = phase.get("group", "")
                users = phase.get("users", "")
                duration = phase.get("durationMonths")
                dur_str = f", {duration} months" if duration else ""
                lines.append(f"- **{phase_name}** ({group}) — {users} users{dur_str}")
        lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 12. FLATTEN: TCO Summary
# ───────────────────────────────────────────────────────────────────
def flatten_tco(record: dict) -> list[str]:
    """Flatten Total Cost of Ownership into a readable summary."""
    tco = record.get("tco")
    if not tco or not isinstance(tco, dict):
        return []

    lines = []
    lines.append("")
    lines.append("## Total Cost of Ownership (TCO)")
    lines.append("")

    # High-level cost factors
    factor_fields = {
        "technologyFactor": "Technology Factor",
        "peopleFactor": "People Factor",
        "operationsFactor": "Operations Factor",
        "businessFactor": "Business Factor",
        "usabilityFactor": "Usability Factor",
    }
    factors = []
    for key, label in factor_fields.items():
        val = tco.get(key)
        if val is not None:
            factors.append(f"- **{label}:** ${val:,}" if isinstance(val, (int, float)) else f"- **{label}:** {val}")
    if factors:
        lines.append("### Cost Factors")
        lines.extend(factors)
        lines.append("")

    # People
    fte = tco.get("resource_fte")
    ftc = tco.get("resource_ftc")
    if fte is not None or ftc is not None:
        lines.append("### People")
        if fte is not None:
            lines.append(f"- **FTE Count:** {fte} (Budget: ${tco.get('fte_budget', 'N/A')})")
        if ftc is not None:
            lines.append(f"- **Contractor Count:** {ftc} (Budget: ${tco.get('ftc_budget', 'N/A')})")
        if tco.get("training"):
            lines.append(f"- **Training:** ${tco['training']}")
        if tco.get("end_user_training"):
            lines.append(f"- **End User Training:** ${tco['end_user_training']}")
        lines.append("")

    # Compute
    compute = tco.get("compute", [])
    if compute and isinstance(compute, list):
        lines.append("### Compute")
        for c in compute:
            if isinstance(c, dict):
                lines.append(
                    f"- {c.get('service', '')} ({c.get('type', '')}, "
                    f"{c.get('size', '')}) × {c.get('quantity', 1)} = ${c.get('cost', 'N/A')}/mo"
                )
        lines.append("")

    # AI/ML
    aiml = tco.get("aiMl", [])
    if aiml and isinstance(aiml, list):
        lines.append("### AI/ML Services")
        for item in aiml:
            if isinstance(item, dict):
                lines.append(
                    f"- {item.get('service', '')} — "
                    f"{item.get('token', 'N/A')} tokens = ${item.get('cost', 'N/A')}/mo"
                )
        lines.append("")

    # Token Analysis
    tokens = tco.get("tokenAnalysis", [])
    if tokens and isinstance(tokens, list):
        lines.append("### Token Analysis")
        for t in tokens:
            if isinstance(t, dict):
                lines.append(f"- **{t.get('name', '')}** ({t.get('provider', '')}) — Role: {t.get('role', '')}")
                if t.get("section1Content"):
                    lines.append(f"  - {t.get('section1Title', '')}: {t['section1Content']}")
                if t.get("section2Content"):
                    lines.append(f"  - {t.get('section2Title', '')}: {t['section2Content']}")
        lines.append("")

    return lines


# ───────────────────────────────────────────────────────────────────
# 13. FLATTEN: Document Summary
# ───────────────────────────────────────────────────────────────────
def flatten_document_summary(record: dict) -> list[str]:
    """Flatten the document summary."""
    summary = record.get("documentSummary")
    if not summary:
        return []

    lines = []
    lines.append("")
    lines.append("## Document Summary")
    lines.append(f"{summary}")
    lines.append("")
    return lines


# ───────────────────────────────────────────────────────────────────
# 14. MASTER FLATTEN: Assemble full markdown
# ───────────────────────────────────────────────────────────────────
def flatten_usecase_for_rag(record: dict, framework_lookup: dict = None) -> str:
    """Assemble all sections into a complete RAG document."""
    all_lines = []

    all_lines.extend(flatten_overview(record, framework_lookup))
    all_lines.extend(flatten_document_summary(record))
    all_lines.extend(flatten_ai_bom(record))
    all_lines.extend(flatten_data_bom(record))
    all_lines.extend(flatten_metrics(record))
    all_lines.extend(flatten_jira_stories(record))
    all_lines.extend(flatten_risk_and_controls(record))
    all_lines.extend(flatten_design_document(record))
    all_lines.extend(flatten_rollout_and_epics(record))
    all_lines.extend(flatten_tco(record))

    # Search keywords at the end for embedding
    if record.get("searchAttributesAsJson"):
        all_lines.append("")
        all_lines.append(f"**Search Keywords:** {record['searchAttributesAsJson']}")

    return "\n".join(all_lines)


# ───────────────────────────────────────────────────────────────────
# 15. MAIN PIPELINE: Scan → Flatten → Upload to S3
# ───────────────────────────────────────────────────────────────────
def scan_and_upload():
    """Scan all use case assessments, flatten, upload to S3."""
    print(f"\n{'='*60}")
    print(f"  Use Case Assessments Pipeline")
    print(f"  Table:  {USECASE_TABLE}")
    print(f"  S3:     s3://{S3_BUCKET}/{S3_PREFIX}/")
    print(f"{'='*60}")

    records = scan_table(USECASE_TABLE)

    # Scan frameworks table for riskframeworkid resolution
    print(f"\n📖 Loading frameworks for risk framework resolution...")
    frameworks = scan_table(FRAMEWORKS_TABLE)
    framework_lookup = {fw["id"]: fw for fw in frameworks if "id" in fw}
    print(f"  ✅ {len(framework_lookup)} frameworks loaded for lookup")

    if not records:
        print("⚠️  No records found. Exiting.")
        return 0

    # Summary
    print(f"\n📊 Use Case Summary:")
    for r in records:
        uc_id = r.get("id", "Unknown")
        name = r.get("modelName", "Unknown")
        risk = r.get("overallRisk", "N/A")
        category = r.get("aiCategory", "N/A")
        print(f"  • {name} ({uc_id}) — Risk: {risk}, Category: {category}")

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

    # Upload
    print(f"\n📤 Uploading to s3://{S3_BUCKET}/{S3_PREFIX}/")
    uploaded = 0
    for record in records:
        try:
            uc_id = record.get("id", f"unknown-{uploaded}")
            model_name = record.get("modelName", "Unknown")

            content = flatten_usecase_for_rag(record, framework_lookup)
            s3_key = f"{S3_PREFIX}/{uc_id}.md"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=content.encode("utf-8"),
                ContentType="text/markdown"
            )

            uploaded += 1
            print(f"  ✅ {uc_id} — {model_name} ({len(content)} chars)")

        except Exception as e:
            record_id = record.get("id", "unknown")
            print(f"  ❌ Failed: {record_id} — {e}")

    print(f"\n✅ Uploaded {uploaded}/{len(records)} use case documents to S3.")
    return uploaded


# ───────────────────────────────────────────────────────────────────
# 16. SYNC Bedrock Knowledge Base
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
    print("  Use Case Assessments → S3 → Bedrock KB Pipeline")
    print(f"  Table: {USECASE_TABLE}")
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
