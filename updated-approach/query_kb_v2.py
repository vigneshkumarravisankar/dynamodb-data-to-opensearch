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
_VALID_SECTIONS = [
    # Use case assessment sections
    "01_overview", "02_document_summary", "03_ai_bom", "04_data_bom",
    "05_metrics", "06_jira_stories", "07_risk_and_controls",
    "08_design_document", "09_rollout_and_epics", "10_tco",
    "11_model_validation", "12_framework_kcis",
    # Framework sections
    "fw_01_overview", "fw_02_policy_references",
    # Control sections
    "ctrl_01_overview", "ctrl_02_maturity_levels", "ctrl_03_framework_associations",
    # Framework-controls sections
    "fc_01_framework_summary", "fc_02_attached_controls",
]

_SECTION_DESCRIPTIONS = """01_overview: Use case overview — model name, AI category, department, vendor, risk level, status, platform, data labels, inventory
02_document_summary: Document summary and highlights
03_ai_bom: AI Bill of Materials — LLM provider, AI frameworks, hardware, hosting, PII, AI platform
04_data_bom: Data Bill of Materials — datasets, data validation
05_metrics: KPIs, performance metrics, thresholds
06_jira_stories: Jira stories, epics, gaps, acceptance criteria, assessment results, missing/valid components
07_risk_and_controls: Risk POSTURE summary — risk categories, severity breakdown, control coverage percentages, risk applicability (NOT individual control details)
08_design_document: Architecture, API design, cloud architecture, security compliance, data model, delivery plan, frontend, third party integrations
09_rollout_and_epics: Rollout plan, deployment phases, epic list
10_tco: Total Cost of Ownership — compute costs, token analysis, FTE, contractors
11_model_validation: Model validation assessment — KCI grading, remediation, justification, implementation status, not-met counts
12_framework_kcis: Individual controls and Key Control Indicators — LIST of attached controls, control IDs, control hierarchy, control names, control questions, "controls attached", "list controls""".strip()

# Sections that warrant a high token budget
_HEAVY_SECTIONS = {
    "08_design_document", "10_tco", "07_risk_and_controls",
    "11_model_validation", "12_framework_kcis",
    "fc_02_attached_controls",
    "ctrl_03_framework_associations",
}
_MEDIUM_SECTIONS = {
    "06_jira_stories", "05_metrics", "09_rollout_and_epics",
    "03_ai_bom", "04_data_bom", "02_document_summary",
    "fw_01_overview", "fw_02_policy_references",
    "fc_01_framework_summary",
    "ctrl_01_overview", "ctrl_02_maturity_levels",
}


def _validate_ids(result: dict, catalog: dict) -> None:
    """Validate / correct entity IDs against the catalog.

    LLMs sometimes truncate leading zeros (e.g. AI-CTRL-0045 instead of
    AI-CTRL-00045).  This function checks each returned ID and, if it
    doesn't match exactly, tries to find the closest match by comparing
    the numeric suffix.
    """
    id_fields = [
        ("usecase_id",   "usecases"),
        ("framework_id", "frameworks"),
        ("control_id",   "controls"),
    ]
    for field, catalog_key in id_fields:
        val = result.get(field)
        if not val:
            continue
        valid_ids = {item["id"] for item in catalog.get(catalog_key, [])}
        if val in valid_ids:
            continue
        # Try numeric-suffix match
        suffix = val.rsplit("-", 1)[-1].lstrip("0") or "0"
        for vid in sorted(valid_ids):
            vid_suffix = vid.rsplit("-", 1)[-1].lstrip("0") or "0"
            if vid_suffix == suffix and vid.split("-")[0:2] == val.split("-")[0:2]:
                result[field] = vid
                break


def classify_query(query: str) -> dict:
    """Use LLM to auto-classify which entities and sections the query is about.

    Returns dict with keys:
      - usecase_id:   str | None
      - framework_id: str | None
      - control_id:   str | None
      - sections:     list[str]     (empty [] = search all sections)
    """
    # ── Fast path: extract explicit IDs via regex ──
    explicit_uc = None
    explicit_fw = None
    explicit_ctrl = None

    m = re.search(r'(AI-UC-AST-\d+)', query, re.IGNORECASE)
    if m:
        explicit_uc = m.group(1).upper()
    m = re.search(r'(AI-ADF-\d+)', query, re.IGNORECASE)
    if m:
        explicit_fw = m.group(1).upper()
    m = re.search(r'(AI-CTRL-\d+)', query, re.IGNORECASE)
    if m:
        explicit_ctrl = m.group(1).upper()

    # ── Build compact catalog strings for the LLM ──
    catalog = _load_catalog()

    uc_list = "\n".join(
        f"  {u['id']}: {u['name']}" for u in catalog.get("usecases", [])
    ) or "  (none)"
    fw_list = "\n".join(
        f"  {f['id']}: {f['name']}" for f in catalog.get("frameworks", [])
    ) or "  (none)"
    ctrl_list = "\n".join(
        f"  {c['id']}: {c['name']}" for c in catalog.get("controls", [])
    ) or "  (none)"

    classification_prompt = f"""You are an entity classifier for an AI governance knowledge base.
Given the user's query, identify which specific entities they are asking about
and which content sections are relevant.

AVAILABLE USE CASES:
{uc_list}

AVAILABLE FRAMEWORKS:
{fw_list}

AVAILABLE CONTROLS:
{ctrl_list}

AVAILABLE SECTIONS:
{_SECTION_DESCRIPTIONS}

RULES:
- usecase_id: Set to the exact ID if the query mentions or implies a specific use case. null otherwise.
- framework_id: Set to the exact ID if the query mentions or implies a specific framework. null otherwise.
- control_id: Set to the exact ID if the query mentions or implies a specific control. null otherwise.
- sections: List of section name(s) most relevant to the query. If the query is broad
  (e.g. "tell me everything"), include ["01_overview", "02_document_summary", "07_risk_and_controls"].
  Return [] ONLY if you truly cannot determine the topic.

SECTION ROUTING RULES:
- Sections starting with "01_" through "12_" are USE CASE sections — use ONLY when the query
  targets a specific use case (by name or ID). These require usecase_id.
- Sections starting with "fw_" are FRAMEWORK-ONLY sections — use when asking about a framework's
  details, description, owner, policies. Set framework_id and use fw_ sections.
- Sections starting with "ctrl_" are CONTROL-ONLY sections — use when asking about a specific
  control's details, maturity levels, or which frameworks it belongs to. Set control_id.
- Sections starting with "fc_" are FRAMEWORK-CONTROLS sections — use when asking about which
  controls are attached to a framework (without a use case context). Set framework_id.

SPECIFIC RULES:
- When asking to LIST controls for a USE CASE → use 12_framework_kcis (requires usecase_id).
- When asking to LIST controls for a FRAMEWORK (no use case) → use fc_02_attached_controls (requires framework_id).
- When asking about a FRAMEWORK's overview/details → use fw_01_overview (requires framework_id).
- When asking about a CONTROL's overview/details → use ctrl_01_overview (requires control_id).
- When asking about a CONTROL's maturity level → use ctrl_02_maturity_levels (requires control_id).
- When asking about policy documents/links for a framework → use fw_02_policy_references.
- 07_risk_and_controls is for risk POSTURE summaries only, NOT individual controls.
- IMPORTANT: Copy IDs EXACTLY as shown in the catalogs above. Do NOT modify, truncate,
  or change the number of digits in any ID.

Return ONLY valid JSON:
{{"usecase_id": "..." or null, "framework_id": "..." or null, "control_id": "..." or null, "sections": [...]}}

User query: {query}"""

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",  # fast + cheap for classification
            messages=[{"role": "user", "content": classification_prompt}],
            temperature=0,
            max_tokens=200,
            response_format={"type": "json_object"},
        )
        result = json.loads(resp.choices[0].message.content)
    except Exception as e:
        print(f"  [classify] LLM classification failed: {e}")
        result = {}

    # Override with explicit IDs when found directly in the query text
    if explicit_uc:
        result["usecase_id"] = explicit_uc
    if explicit_fw:
        result["framework_id"] = explicit_fw
    if explicit_ctrl:
        result["control_id"] = explicit_ctrl

    # Validate / sanitise sections
    raw_sections = result.get("sections")
    if not isinstance(raw_sections, list):
        raw_sections = []
    result["sections"] = [s for s in raw_sections if s in _VALID_SECTIONS]

    # Validate entity IDs against catalog (fix truncated zeros, etc.)
    _validate_ids(result, catalog)

    # Ensure null → None for missing keys
    for key in ("usecase_id", "framework_id", "control_id"):
        if not result.get(key):
            result[key] = None

    print(
        f"  [classify] usecase={result.get('usecase_id')}, "
        f"framework={result.get('framework_id')}, "
        f"control={result.get('control_id')}, "
        f"sections={result.get('sections')}"
    )
    return result


# ───────────────────────────────────────────────────────────────────
# TOKEN BUDGET
# ───────────────────────────────────────────────────────────────────
def _token_budget(sections: list[str] | None = None) -> int:
    """Determine max_tokens for the LLM answer based on classified sections."""
    if sections:
        if any(s in _HEAVY_SECTIONS for s in sections):
            return 8000
        if any(s in _MEDIUM_SECTIONS for s in sections):
            return 4000
        return 2000
    return 4000  # default


# ───────────────────────────────────────────────────────────────────
# RETRIEVE — Section-filtered, paginated
# ───────────────────────────────────────────────────────────────────
def retrieve(query: str, top_k: int = 10, classification: dict | None = None) -> list[dict]:
    """
    Retrieve relevant chunks from Bedrock KB.
    Uses LLM classification to apply entity + section metadata filters.
    """
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

    retrieval_config = {
        "vectorSearchConfiguration": {
            "numberOfResults": min(top_k, 100),
            "overrideSearchType": "HYBRID"
        }
    }

    # ── Build metadata filters from classification ──
    # Determine the entity type based on section prefixes.
    # fw_* / fc_* sections → filter by framework_id
    # ctrl_* sections → filter by control_id
    # 01_-12_ sections → filter by usecase_id
    filters = []

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
        if len(sections) == 1:
            filters.append({"equals": {"key": "section", "value": sections[0]}})
        else:
            filters.append({
                "orAll": [
                    {"equals": {"key": "section", "value": s}} for s in sections
                ]
            })

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
def retrieve_and_generate(query: str, top_k: int = 10) -> dict:
    """
    Section-aware RAG pipeline:
      1. Classify query (LLM) → entity IDs + sections
      2. Retrieve with metadata filters
      3. Filter by score
      4. Pass directly to LLM (no summarisation — sections are small)
    """
    classification = classify_query(query)
    chunks = retrieve(query, top_k=top_k, classification=classification)

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
        model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful AI assistant answering questions about AI governance frameworks, "
                    "risk controls, and AI inventory records.\n\n"
                    "Use ONLY the information from the provided context to answer. "
                    "If the context doesn't contain enough information, say so clearly.\n\n"
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
        temperature=0.1,
        max_tokens=answer_budget
    )

    retrieval_scores = [c["score"] for c in chunks]
    mean_score = sum(retrieval_scores) / len(retrieval_scores) if retrieval_scores else 0
    sources = list(set(c["source"] for c in chunks))

    return {
        "answer": response.choices[0].message.content,
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
    print("Commands: 'quit' to exit | 'retrieve' for retrieve-only mode\n")

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
