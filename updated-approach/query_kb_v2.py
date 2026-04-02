"""
Query KB v2 — Section-aware RAG pipeline for the updated approach.

Key differences from the original query_kb.py:
  - Detects which SECTION the query is about (overview, jira, tco, etc.)
  - Adds a `section` metadata filter so Bedrock only returns relevant chunks
  - No summarisation needed — sections are already small & clean
  - top_k stays small (10) since each section file is focused
  - Returns data verbatim — no rephrasing
"""

import os
import re
import sys
import json
import boto3
from dotenv import load_dotenv
from openai import OpenAI
from boto3.dynamodb.types import TypeDeserializer

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

REGION = os.getenv("REGION", "us-east-1")
KNOWLEDGE_BASE_ID = os.getenv("KNOWLEDGE_BASE_ID_V2") or os.getenv("KNOWLEDGE_BASE_ID")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# DynamoDB table names for entity catalog
USECASE_TABLE = os.getenv(
    "DYNAMODB_USECASE_ASSESSMENTS_TENANT_TABLE",
    "staging-fusefy-usecaseAssessments-d66cb7c7-04ac-4634-927f-06d91afa39bf"
)
FRAMEWORKS_TABLE = os.getenv("DYNAMODB_TABLE", "staging-fusefy-frameworks")
CONTROLS_TABLE = os.getenv("DYNAMODB_CONTROLS_TABLE", "staging-fusefy-controls")

if not KNOWLEDGE_BASE_ID:
    raise EnvironmentError("Set KNOWLEDGE_BASE_ID_V2 (or KNOWLEDGE_BASE_ID) in .env")

if not OPENAI_API_KEY:
    raise EnvironmentError("Set OPENAI_API_KEY in .env")

bedrock_agent = boto3.client("bedrock-agent-runtime", region_name=REGION)
openai_client = OpenAI(api_key=OPENAI_API_KEY)
_dynamodb = boto3.client("dynamodb", region_name=REGION)
_deserializer = TypeDeserializer()

# Regex to extract cloudId from natural language queries
_CLOUD_ID_RE = re.compile(
    r'(?:with\s+)?cloud\s*id\s*[-\u2013\u2014:=]*\s*([a-f0-9\-]{36})',
    re.IGNORECASE,
)


def _extract_cloud_id(query: str) -> tuple[str, str]:
    """Extract cloudId from query text and return (cloud_id, cleaned_query).

    Supports patterns like:
      - "... with cloudId - d66cb7c7-..."
      - "... cloudId: d66cb7c7-..."
      - "... cloudId d66cb7c7-..."
    Returns ("", original_query) if no cloudId found.
    """
    m = _CLOUD_ID_RE.search(query)
    if not m:
        return "", query
    cloud_id = m.group(1)
    cleaned = query[:m.start()].rstrip(" ,;-\u2013\u2014") + query[m.end():]
    return cloud_id, cleaned.strip()


# ───────────────────────────────────────────────────────────────────
# ENTITY CATALOG — loaded once from DynamoDB
# ───────────────────────────────────────────────────────────────────
_CATALOG: dict | None = None


def _scan_table_light(table_name: str) -> list[dict]:
    """Scan all items from a DynamoDB table (lightweight, for catalog only)."""
    all_items = []
    params = {"TableName": table_name}
    while True:
        response = _dynamodb.scan(**params)
        all_items.extend(response.get("Items", []))
        if "LastEvaluatedKey" in response:
            params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        else:
            break
    return [
        {k: _deserializer.deserialize(v) for k, v in item.items()}
        for item in all_items
    ]


def _load_catalog() -> dict:
    """Load entity catalog from DynamoDB (cached after first call)."""
    global _CATALOG
    if _CATALOG is not None:
        return _CATALOG

    print("  [catalog] Loading entities from DynamoDB...")

    # Use cases
    uc_records = _scan_table_light(USECASE_TABLE)
    usecases = [
        {"id": r.get("id", ""), "name": r.get("modelName", "")}
        for r in uc_records if r.get("id")
    ]

    # Frameworks
    fw_records = _scan_table_light(FRAMEWORKS_TABLE)
    frameworks = [
        {"id": r.get("id", ""), "name": r.get("name", r.get("id", ""))}
        for r in sorted(fw_records, key=lambda x: x.get("id", "")) if r.get("id")
    ]

    # Controls
    ctrl_records = _scan_table_light(CONTROLS_TABLE)
    controls = []
    for r in sorted(ctrl_records, key=lambda x: x.get("id", "")):
        cid = r.get("id")
        if not cid:
            continue
        name_field = r.get("name", cid)
        if isinstance(name_field, list) and name_field:
            display_name = str(name_field[-1])
        else:
            display_name = str(name_field)
        controls.append({"id": cid, "name": display_name})

    _CATALOG = {
        "usecases": usecases,
        "frameworks": frameworks,
        "controls": controls,
    }

    print(
        f"  [catalog] Loaded: {len(usecases)} usecases, "
        f"{len(frameworks)} frameworks, {len(controls)} controls"
    )
    return _CATALOG


# ───────────────────────────────────────────────────────────────────
# LLM-BASED QUERY CLASSIFIER
# ───────────────────────────────────────────────────────────────────

# Section descriptions are the SINGLE SOURCE OF TRUTH.
# _VALID_SECTIONS is auto-derived from these descriptions, so adding
# a new section only requires updating _SECTION_DESCRIPTIONS.
_SECTION_DESCRIPTIONS = """01_overview: Use case overview — model name, AI category, department, vendor, risk level, status, platform, data labels, inventory
02_document_summary: Document summary and highlights
03_ai_bom: AI Bill of Materials — LLM provider, AI frameworks, hardware, hosting, PII, AI platform
04_data_bom: Data Bill of Materials — datasets, data validation
05_metrics: KPIs, performance metrics, thresholds (from the use case assessment definition)
06_jira_stories: Jira stories, epics, gaps, acceptance criteria, assessment results, missing/valid components
07_risk_and_controls: Risk POSTURE summary — risk categories, severity breakdown, control coverage percentages, risk applicability (NOT individual control details)
07b_threat_assessment: Threat assessment — threat controls status (Met, Implemented, Risk Accepted, Not Met, Not Applicable), evidence attachments (comments, uploaded documents) for addressed controls, Jira stories for pending controls, risk posture, risk level, framework name, identified threats, mapped components, remediation
08_design_document: Architecture, API design, cloud architecture, security compliance, data model, delivery plan, frontend, third party integrations
09_rollout_and_epics: Rollout plan, deployment phases, epic list
10_tco: Total Cost of Ownership — compute costs, token analysis, FTE, contractors
11_model_validation: Model validation assessment — KCI grading, remediation, justification, implementation status, not-met counts
12_framework_kcis: Individual controls and Key Control Indicators — LIST of attached controls, control IDs, control hierarchy, control names, control questions, "controls attached", "list controls"
13_ai_model_info: AI model info from validation results — model name, version, production status, approval info, approved by, approved date
14_ai_eval_metrics: AI evaluation metrics from validation results — actual metric scores/values with explanations, fraud detection rate, processing time, accuracy, latency, compliance scores
15_ai_sbom: AI Software Bill of Materials (SBOM) from validation results — component inventory, vulnerabilities count, licenses, severity levels, package versions
16_ai_cspm: AI Cloud Security Posture Management (CSPM) from validation results — overall security score, policy compliance status, security policy issues
17_ai_security_threats: AI security threats from validation results — prompt injection attempts, tool abuse, threat detection, blocked attempts, risk levels, adversarial attacks
18_ai_chart_data: AI monitoring chart/trend data from validation results — metrics over time, historical trends, threat detection charts
19_ai_agent_evaluators: AI agent evaluators from validation results — evaluator definitions, signals, judging methods, evaluation criteria
20_monitoring_day: Day-wise AI monitoring results — daily run info, run_id, job_id, input dataset, model info, monitoring window, daily metric values, drift analysis, data quality, alerts, monitoring status
fw_01_overview: Framework overview — framework name, description, owner, category
fw_02_policy_references: Framework policy references — policy documents, links, regulatory references
ctrl_overview: Control overview, maturity levels, and framework associations — control name, description, hierarchy, classification, AI maturity level definitions, and which frameworks a control belongs to
fc_01_framework_summary: Framework-controls summary — framework with attached control count
fc_02_attached_controls: Framework-controls list — all controls attached to a framework""".strip()

# Auto-derive _VALID_SECTIONS from _SECTION_DESCRIPTIONS (single source of truth)
_VALID_SECTIONS = [
    line.split(":")[0].strip()
    for line in _SECTION_DESCRIPTIONS.splitlines()
    if ":" in line
]

# Sections that warrant a high token budget
_HEAVY_SECTIONS = {
    "08_design_document", "10_tco", "07_risk_and_controls",
    "07b_threat_assessment",
    "11_model_validation", "12_framework_kcis",
    "15_ai_sbom", "17_ai_security_threats", "20_monitoring_day",
    "fc_02_attached_controls",
    "ctrl_overview",
}
_MEDIUM_SECTIONS = {
    "06_jira_stories", "05_metrics", "09_rollout_and_epics",
    "03_ai_bom", "04_data_bom", "02_document_summary",
    "13_ai_model_info", "14_ai_eval_metrics", "16_ai_cspm",
    "18_ai_chart_data", "19_ai_agent_evaluators",
    "fw_01_overview", "fw_02_policy_references",
    "fc_01_framework_summary",
}


def _resolve_entities(query: str) -> dict:
    """Resolve entity IDs from the query using Python string matching.

    Strategy (in order):
      1. Regex match explicit IDs  (AI-UC-AST-*, AI-ADF-*, AI-CTRL-*)
      2. Exact case-insensitive name match against the catalog
      3. Substring match (catalog name found inside query)

    Returns dict with usecase_id, framework_id, control_id (str | None).
    """
    catalog = _load_catalog()
    q_lower = query.lower()

    # 1. Regex — explicit IDs in the query
    uc_id = fw_id = ctrl_id = None

    m = re.search(r'(AI-UC-AST-\d+)', query, re.IGNORECASE)
    if m:
        uc_id = m.group(1).upper()
    m = re.search(r'(AI-ADF-\d+)', query, re.IGNORECASE)
    if m:
        fw_id = m.group(1).upper()
    m = re.search(r'(AI-CTRL-\d+)', query, re.IGNORECASE)
    if m:
        ctrl_id = m.group(1).upper()

    # 2 & 3. Name matching against catalog (skip if already resolved by regex)
    def _match(items: list[dict], resolved_id: str | None) -> str | None:
        if resolved_id:
            # Validate the regex-extracted ID exists in catalog
            valid = {it["id"] for it in items}
            if resolved_id in valid:
                return resolved_id
            # Try numeric-suffix correction (e.g. AI-CTRL-0045 → AI-CTRL-00045)
            suffix = resolved_id.rsplit("-", 1)[-1].lstrip("0") or "0"
            prefix_parts = resolved_id.split("-")[0:2]
            for vid in sorted(valid):
                vid_suffix = vid.rsplit("-", 1)[-1].lstrip("0") or "0"
                if vid_suffix == suffix and vid.split("-")[0:2] == prefix_parts:
                    return vid
            return resolved_id  # pass through even if not in catalog
        # Name match: longest match wins (avoids "ISO" matching before "ISO 42001")
        best_id, best_len = None, 0
        for item in items:
            name = item.get("name", "")
            if not name:
                continue
            name_lower = name.lower()
            if name_lower in q_lower and len(name) > best_len:
                best_id = item["id"]
                best_len = len(name)
        return best_id

    uc_id = _match(catalog.get("usecases", []), uc_id)
    fw_id = _match(catalog.get("frameworks", []), fw_id)
    ctrl_id = _match(catalog.get("controls", []), ctrl_id)

    return {
        "usecase_id": uc_id or None,
        "framework_id": fw_id or None,
        "control_id": ctrl_id or None,
    }


def classify_query(query: str) -> dict:
    """Classify query: resolve entities in Python, classify sections via LLM.

    Returns dict with keys:
      - usecase_id:   str | None
      - framework_id: str | None
      - control_id:   str | None
      - sections:     list[str]     (empty [] = search all sections)
    """
    # ── Step 1: Resolve entities in Python (no LLM, no tokens) ──
    entities = _resolve_entities(query)

    # ── Step 2: LLM classifies SECTIONS only (lightweight prompt) ──
    section_prompt = f"""You are a section classifier for an AI governance knowledge base.
Given the user's query, determine which content sections are relevant.

AVAILABLE SECTIONS:
{_SECTION_DESCRIPTIONS}

ENTITY CONTEXT (already resolved):
- usecase_id: {entities['usecase_id'] or 'None'}
- framework_id: {entities['framework_id'] or 'None'}
- control_id: {entities['control_id'] or 'None'}

SECTION ROUTING RULES:
- Sections 01_ through 12_ are USE CASE sections — use ONLY when usecase_id is set.
- Sections fw_* are FRAMEWORK-ONLY — use when asking about a framework's details, description, owner, policies.
- Sections ctrl_* are CONTROL-ONLY — use when asking about a control's details, maturity, framework associations.
- Sections fc_* are FRAMEWORK-CONTROLS — use when listing controls attached to a framework.

SPECIFIC RULES:
- LIST controls for a USE CASE → 12_framework_kcis
- LIST controls for a FRAMEWORK → fc_02_attached_controls
- Framework overview/details → fw_01_overview
- Control overview/details/maturity → ctrl_overview
- Policy documents → fw_02_policy_references
- Risk POSTURE summary → 07_risk_and_controls (NOT individual controls)
- Threat assessment, control statuses, evidence → 07b_threat_assessment (NOT 07_risk_and_controls)
- AI eval scores, fraud rate, accuracy → 14_ai_eval_metrics (NOT 05_metrics)
- SBOM, vulnerabilities, CVEs → 15_ai_sbom (NOT 03_ai_bom)
- CSPM, security score → 16_ai_cspm`
- Security threats, prompt injection → 17_ai_security_threats
- Metric trends, charts → 18_ai_chart_data
- Evaluators, judging methods → 19_ai_agent_evaluators
- Model monitoring, drift, data quality → 20_monitoring_day (NOT 11_model_validation)
- Model info, version, approval → 13_ai_model_info
- Broad "AI evaluation" or "validation results" → [14_ai_eval_metrics, 15_ai_sbom, 16_ai_cspm, 17_ai_security_threats, 19_ai_agent_evaluators]
- Broad query → ["01_overview", "02_document_summary", "07_risk_and_controls"]
- Return [] ONLY if you truly cannot determine the topic.

Return ONLY valid JSON:
{{"sections": [...]}}

User query: {query}"""

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-5-mini",
            messages=[{"role": "user", "content": section_prompt}],
            max_completion_tokens=500,
            response_format={"type": "json_object"},
        )
        llm_result = json.loads(resp.choices[0].message.content)
    except Exception as e:
        print(f"  [classify] LLM section classification failed: {e}")
        llm_result = {}

    # Validate / sanitise sections
    raw_sections = llm_result.get("sections")
    if not isinstance(raw_sections, list):
        raw_sections = []
    sections = [
        s for s in raw_sections
        if s in _VALID_SECTIONS or s.startswith("20_monitoring_day")
    ]

    result = {
        "usecase_id": entities["usecase_id"],
        "framework_id": entities["framework_id"],
        "control_id": entities["control_id"],
        "sections": sections,
    }

    print(
        f"  [classify] usecase={result['usecase_id']}, "
        f"framework={result['framework_id']}, "
        f"control={result['control_id']}, "
        f"sections={result['sections']}"
    )
    return result


# ───────────────────────────────────────────────────────────────────
# TOKEN BUDGET
# ───────────────────────────────────────────────────────────────────
def _token_budget(sections: list[str] | None = None) -> int:
    """Determine max_tokens for the LLM answer based on classified sections."""
    if sections:
        if (any(s in _HEAVY_SECTIONS for s in sections)
                or any(s.startswith("20_monitoring_day") for s in sections)):
            return 8000
        if any(s in _MEDIUM_SECTIONS for s in sections):
            return 4000
        return 2000
    return 4000  # default


# ───────────────────────────────────────────────────────────────────
# RETRIEVE — Section-filtered, paginated
# ───────────────────────────────────────────────────────────────────
def retrieve(query: str, top_k: int = 10, classification: dict | None = None, cloud_id: str = "") -> list[dict]:
    """
    Retrieve relevant chunks from Bedrock KB.
    Uses LLM classification to apply entity + section metadata filters.
    """
    # ── Extract cloud_id from query if not passed explicitly ──
    if not cloud_id:
        cloud_id, query = _extract_cloud_id(query)

    # ── Classify if not already provided ──
    if classification is None:
        classification = classify_query(query)

    uc_id = classification.get("usecase_id")
    fw_id = classification.get("framework_id")
    ctrl_id = classification.get("control_id")
    sections = classification.get("sections") or []

    # Auto-increase top_k for heavy sections
    if any(s in ["12_framework_kcis", "11_model_validation"] for s in sections):
        top_k = max(top_k, 50)
    if any(s.startswith("20_monitoring_day") for s in sections):
        top_k = max(top_k, 50)

    retrieval_config = {
        "vectorSearchConfiguration": {
            "numberOfResults": min(top_k, 100),
            "overrideSearchType": "SEMANTIC",
        }
    }

    # ── Build metadata filters from classification ──
    # cloudId is ALWAYS the first filter — scopes to this tenant
    if not cloud_id:
        print("  [retrieve] Blocked — cloud_id is required")
        return []
    filters = [{"equals": {"key": "cloudId", "value": cloud_id}}]

    # Determine the entity type based on section prefixes.
    # fw_* / fc_* sections → filter by framework_id
    # ctrl_* sections → filter by control_id
    # 01_-12_ sections → filter by usecase_id

    has_fw_sections = any(s.startswith(("fw_", "fc_")) for s in sections)
    has_ctrl_sections = any(s.startswith("ctrl_") for s in sections)
    has_uc_sections = any(not s.startswith(("fw_", "fc_", "ctrl_")) for s in sections)

    # Apply entity ID filter based on section type
    if uc_id and has_uc_sections:
        filters.append({"equals": {"key": "usecase_id", "value": uc_id}})
    if fw_id and has_fw_sections:
        filters.append({"equals": {"key": "framework_id", "value": fw_id}})
    if ctrl_id and has_ctrl_sections:
        filters.append({"equals": {"key": "control_id", "value": ctrl_id}})

    if sections:
        section_filters = []
        for s in sections:
            if s.startswith("20_monitoring_day"):
                # Prefix match: 20_monitoring_day → matches day_1, day_2, etc.
                section_filters.append(
                    {"startsWith": {"key": "section", "value": "20_monitoring_day"}}
                )
            else:
                section_filters.append(
                    {"equals": {"key": "section", "value": s}}
                )
        if len(section_filters) == 1:
            filters.append(section_filters[0])
        else:
            filters.append({"orAll": section_filters})

    # Combine
    if len(filters) == 1:
        retrieval_config["vectorSearchConfiguration"]["filter"] = filters[0]
    elif len(filters) > 1:
        retrieval_config["vectorSearchConfiguration"]["filter"] = {"andAll": filters}

    # ── Enrich query with entity IDs for better semantic matching ──
    enriched_query = query
    hints = []
    if fw_id and not has_fw_sections:
        hints.append(f"framework {fw_id}")
    if ctrl_id and not has_ctrl_sections:
        hints.append(f"control {ctrl_id}")
    if hints:
        enriched_query = f"{query} ({', '.join(hints)})"

    # ── Paginated retrieval ──
    results = []
    next_token = None

    while True:
        call_kwargs = dict(
            knowledgeBaseId=KNOWLEDGE_BASE_ID,
            retrievalQuery={"text": enriched_query},
            retrievalConfiguration=retrieval_config
        )
        if next_token:
            call_kwargs["nextToken"] = next_token

        response = bedrock_agent.retrieve(**call_kwargs)

        for item in response.get("retrievalResults", []):
            results.append({
                "content": item.get("content", {}).get("text", ""),
                "score": item.get("score", 0),
                "source": item.get("location", {}).get("s3Location", {}).get("uri", "N/A")
            })

        next_token = response.get("nextToken")
        if not next_token:
            break

    section_info = f", sections={sections}" if sections else ""
    filter_info = []
    if uc_id:
        filter_info.append(f"usecase={uc_id}")
    if fw_id:
        filter_info.append(f"framework={fw_id}")
    if ctrl_id:
        filter_info.append(f"control={ctrl_id}")
    extra = f" [{', '.join(filter_info)}]" if filter_info else ""
    print(f"  [retrieve] {len(results)} chunks{section_info}{extra}")
    return results


# ───────────────────────────────────────────────────────────────────
# RETRIEVE + GENERATE — No summarisation needed
# ───────────────────────────────────────────────────────────────────
def retrieve_and_generate(query: str, top_k: int = 10, cloud_id: str = "") -> dict:
    """
    Section-aware RAG pipeline:
      1. Extract cloud_id from query (if not passed explicitly)
      2. Classify query (LLM) → entity IDs + sections
      3. Retrieve with metadata filters
      4. Filter by score
      5. Pass directly to LLM (no summarisation — sections are small)
    """
    # Extract cloud_id from query text if not passed explicitly
    if not cloud_id:
        cloud_id, query = _extract_cloud_id(query)
    if not cloud_id:
        return {
            "answer": "Cloud ID (tenant) is required. Include 'cloudId - <value>' in your question.",
            "sources": [], "chunks_used": 0,
            "retrieval_scores": [], "mean_score": 0,
        }
    print(f"  [tenant] cloudId={cloud_id}")

    classification = classify_query(query)
    sections = classification.get("sections") or []

    chunks = retrieve(query, top_k=top_k, classification=classification, cloud_id=cloud_id)

    if not chunks:
        return {
            "answer": "No relevant information found in the knowledge base.",
            "sources": [], "chunks_used": 0,
            "retrieval_scores": [], "mean_score": 0,
        }

    # Filter low-quality chunks
    MIN_SCORE = 0.25
    relevant = [c for c in chunks if c["score"] >= MIN_SCORE]
    if not relevant:
        relevant = chunks[:3]
    print(f"  [RAG] Using {len(relevant)}/{len(chunks)} chunks (score >= {MIN_SCORE})")
    chunks = relevant

    # Build context — NO summarisation, pass directly
    context = "\n\n---\n\n".join(
        f"[Source: {chunk['source']}]\n{chunk['content']}" for chunk in chunks
    )

    # Truncate safety
    MAX_CONTEXT_CHARS = 120_000
    if len(context) > MAX_CONTEXT_CHARS:
        context = context[:MAX_CONTEXT_CHARS] + "\n... (truncated)"

    answer_budget = _token_budget(classification.get("sections"))

    response = openai_client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL"),
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful AI assistant answering questions about AI governance frameworks, "
                    "risk controls, and AI inventory records.\n\n"
                    "Use ONLY the information from the provided context to answer. "
                    "If the context doesn't contain enough information, say so clearly.\n\n"
                    "FORMATTING RULES:\n"
                    "- Use Markdown formatting for readability.\n"
                    "- Use headers (##, ###) to organize different sections of the answer.\n"
                    "- Use bullet points or numbered lists when listing items.\n"
                    "- Use bold (**text**) for field names, IDs, and important labels.\n"
                    "- Use tables when presenting structured data with multiple columns "
                    "(e.g. control lists with ID, name, status).\n"
                    "- Separate logical sections with blank lines.\n\n"
                    "CRITICAL RULES:\n"
                    "- Return ALL data EXACTLY as it appears in the source documents. "
                    "Do NOT rephrase, reword, paraphrase, or alter any content.\n"
                    "- Keep ALL IDs verbatim — framework IDs, control IDs, inventory IDs, "
                    "use-case IDs, component IDs, Jira issue keys, etc.\n"
                    "- Keep field names, values, numbers, and dates exactly as they are.\n"
                    "- When mentioning Jira stories, ALWAYS include the associated "
                    "gaps / sub-tasks for each story.\n"
                    "- Write in clear, complete sentences suitable for business users.\n"
                    "- Do NOT guess or infer information not in the provided documents.\n\n"
                    "COMPLETENESS RULES:\n"
                    "- When the user asks to LIST items (controls, KCIs, frameworks, stories, "
                    "metrics, etc.), you MUST list EVERY SINGLE item found in the context. "
                    "NEVER truncate, summarize, or omit items.\n"
                    "- Do NOT say 'and more' or 'etc.' — list every item exhaustively.\n"
                    "- If there are 28 controls, list all 28. If there are 50, list all 50.\n"
                    "- Include all fields for each item (ID, name, hierarchy, question, etc.)."
                )
            },
            {
                "role": "user",
                "content": f"Context:\n{context}\n\nQuestion: {query}"
            }
        ],
        max_completion_tokens=answer_budget
    )

    # Debug: inspect what OpenAI returned
    choice = response.choices[0]
    print(f"  [openai] finish_reason={choice.finish_reason}, "
          f"content_length={len(choice.message.content or '')}, "
          f"refusal={getattr(choice.message, 'refusal', None)}")

    answer = choice.message.content or "(No answer generated — model returned empty content)"

    retrieval_scores = [c["score"] for c in chunks]
    mean_score = sum(retrieval_scores) / len(retrieval_scores) if retrieval_scores else 0
    sources = list(set(c["source"] for c in chunks))

    return {
        "answer": answer,
        "sources": sources,
        "chunks_used": len(chunks),
        "retrieval_scores": [round(s, 4) for s in retrieval_scores],
        "mean_score": round(mean_score, 4),
        "classification": classification,
    }


# ───────────────────────────────────────────────────────────────────
# INTERACTIVE CHAT LOOP
# ───────────────────────────────────────────────────────────────────
def chat():
    print("\n" + "=" * 60)
    print("  Section-Based Bedrock KB + OpenAI RAG Chat (v2)")
    print("=" * 60)
    print("  Include 'cloudId - <value>' in your question.")
    print("  Example: Show me controls for ISO 42001 with cloudId - d66cb7c7-...")
    print("  Commands: 'quit' to exit | 'retrieve' for retrieve-only mode\n")

    retrieve_only = False

    while True:
        query = input("Your question: ").strip()

        if not query:
            continue
        if query.lower() == "quit":
            print("Goodbye!")
            break
        if query.lower() == "retrieve":
            retrieve_only = not retrieve_only
            mode = "RETRIEVE ONLY" if retrieve_only else "RETRIEVE + GENERATE"
            print(f"  → Switched to {mode} mode\n")
            continue

        try:
            if retrieve_only:
                print("\nRetrieving chunks...\n")
                results = retrieve(query)
                for i, r in enumerate(results, 1):
                    print(f"--- Chunk {i} (score: {r['score']:.4f}) ---")
                    print(f"Source: {r['source']}")
                    print(f"{r['content'][:500]}...")
                    print()
            else:
                print("\nGenerating answer...\n")
                result = retrieve_and_generate(query)
                print(f"Answer:\n{result['answer']}\n")
                print(f"Sources ({result['chunks_used']} chunks used):")
                for src in result["sources"]:
                    print(f"  • {src}")
                print()
                print("--- Retrieval Metrics ---")
                print(f"  Mean Score      : {result.get('mean_score', 'N/A')}")
                print(f"  Scores          : {result.get('retrieval_scores', [])}")
                print()

        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    chat()
