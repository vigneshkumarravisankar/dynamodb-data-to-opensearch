"""
Setup script — Create 3 new Data Sources on the EXISTING Bedrock Knowledge Base
for the section-based pipelines:

  1. frameworks-v2/           → DATA_SOURCE_ID_FRAMEWORKS_V2
  2. controls-v2/             → DATA_SOURCE_ID_CONTROLS_V2
  3. framework-controls-v2/   → DATA_SOURCE_ID_FC_V2

Each data source uses FIXED_SIZE chunking (300 tokens, 15% overlap)
with the same configuration as the existing usecase-assessments-v2 data source.

After running, the new data source IDs are appended to .env.

Usage:
  python setup_new_data_sources.py
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

REGION = os.getenv("REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dynamo-to-opensearch-rag-frameworks")

# Reuse the existing KB
EXISTING_KB_ID = os.getenv("KNOWLEDGE_BASE_ID_V2") or os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")

bedrock_agent = boto3.client("bedrock-agent", region_name=REGION)

# ── Data sources to create ──────────────────────────────────────────
DATA_SOURCES = [
    {
        "name": "frameworks-sections-datasource",
        "s3_prefix": "frameworks-v2",
        "env_var": "DATA_SOURCE_ID_FRAMEWORKS_V2",
        "description": "Section-based framework files under frameworks-v2/",
    },
    {
        "name": "controls-sections-datasource",
        "s3_prefix": "controls-v2",
        "env_var": "DATA_SOURCE_ID_CONTROLS_V2",
        "description": "Section-based control files under controls-v2/",
    },
    {
        "name": "framework-controls-sections-datasource",
        "s3_prefix": "framework-controls-v2",
        "env_var": "DATA_SOURCE_ID_FC_V2",
        "description": "Section-based framework-control files under framework-controls-v2/",
    },
]


def get_existing_data_sources() -> dict:
    """Get all existing data sources on the KB, keyed by name."""
    existing = {}
    try:
        sources = bedrock_agent.list_data_sources(knowledgeBaseId=EXISTING_KB_ID, maxResults=100)
        for ds in sources.get("dataSourceSummaries", []):
            existing[ds["name"]] = ds["dataSourceId"]
    except Exception:
        pass
    return existing


def create_data_source(ds_config: dict, existing: dict) -> str:
    """Create a single data source. Returns the data source ID."""
    name = ds_config["name"]
    prefix = ds_config["s3_prefix"]

    # Check if already exists
    if name in existing:
        ds_id = existing[name]
        print(f"  ✅ '{name}' already exists: {ds_id}")
        return ds_id

    print(f"  📦 Creating: {name} (prefix: {prefix}/)")
    response = bedrock_agent.create_data_source(
        knowledgeBaseId=EXISTING_KB_ID,
        name=name,
        description=ds_config["description"],
        dataSourceConfiguration={
            "type": "S3",
            "s3Configuration": {
                "bucketArn": f"arn:aws:s3:::{S3_BUCKET}",
                "inclusionPrefixes": [f"{prefix}/"]
            }
        },
        vectorIngestionConfiguration={
            "chunkingConfiguration": {
                "chunkingStrategy": "FIXED_SIZE",
                "fixedSizeChunkingConfiguration": {
                    "maxTokens": 300,
                    "overlapPercentage": 15
                }
            }
        },
        dataDeletionPolicy="DELETE"
    )

    ds_id = response["dataSource"]["dataSourceId"]
    print(f"  ✅ Created: {ds_id} (prefix: {prefix}/, FIXED_SIZE 300 tokens / 15%)")
    return ds_id


def update_env_file(results: dict):
    """Append new data source IDs to .env."""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")

    with open(env_path, "r") as f:
        content = f.read()

    lines_to_add = []
    for env_var, ds_id in results.items():
        if env_var not in content:
            lines_to_add.append(f"{env_var}={ds_id}")

    if lines_to_add:
        with open(env_path, "a") as f:
            f.write("\n# Section-based data sources (frameworks, controls, framework-controls)\n")
            for line in lines_to_add:
                f.write(f"{line}\n")
        print(f"\n✅ Added to .env:")
        for line in lines_to_add:
            print(f"   {line}")
    else:
        print(f"\n✅ .env already has all data source IDs.")


def main():
    print("=" * 60)
    print("  Setup Data Sources — Section-Based Pipelines")
    print("=" * 60)
    print(f"  Region:      {REGION}")
    print(f"  S3 Bucket:   {S3_BUCKET}")
    print(f"  Existing KB: {EXISTING_KB_ID}")
    print()

    # Get existing data sources
    existing = get_existing_data_sources()
    print(f"  📋 Existing data sources on KB: {len(existing)}")
    for name, ds_id in existing.items():
        print(f"     • {name}: {ds_id}")
    print()

    # Create each data source
    results = {}
    for ds_config in DATA_SOURCES:
        try:
            ds_id = create_data_source(ds_config, existing)
            results[ds_config["env_var"]] = ds_id
        except Exception as e:
            print(f"  ❌ Failed to create '{ds_config['name']}': {e}")

    if results:
        update_env_file(results)

    print(f"\n{'='*60}")
    print("  ✅ Setup Complete!")
    print(f"{'='*60}")
    print(f"  Knowledge Base ID: {EXISTING_KB_ID}")
    for ds_config in DATA_SOURCES:
        env_var = ds_config["env_var"]
        ds_id = results.get(env_var, "NOT CREATED")
        print(f"  {env_var}: {ds_id}")

    print(f"\n  Next steps:")
    print(f"  1. python dynamo_to_s3_frameworks_sections.py")
    print(f"  2. python dynamo_to_s3_controls_sections.py")
    print(f"  3. python dynamo_to_s3_framework_controls_sections.py")


if __name__ == "__main__":
    main()
