"""
Setup script — Create a new Data Source on the EXISTING Bedrock Knowledge Base
for the section-based approach.

Instead of creating a whole new KB (which requires AOSS index creation permissions),
we reuse the existing KB and add a second data source pointing to the new
usecase-assessments-v2/ prefix. Section-based metadata filtering keeps the
data clean and separated during queries.

After running, add the output IDs to your .env:
  KNOWLEDGE_BASE_ID_V2=...   (same as existing KB)
  DATA_SOURCE_ID_V2=...      (new data source)

Usage:
  python setup_bedrock_kb.py
"""

import os
import json
import time
import boto3
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

REGION = os.getenv("REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "dynamo-to-opensearch-rag-frameworks")
S3_PREFIX = "usecase-assessments-v2"

# Reuse the existing KB
EXISTING_KB_ID = os.getenv("KNOWLEDGE_BASE_ID", "ZTCXPOQTKW")

DS_NAME = "usecase-sections-datasource"

bedrock_agent = boto3.client("bedrock-agent", region_name=REGION)


def create_data_source():
    """Create a data source on the existing KB pointing to the section-based S3 prefix."""
    print(f"\n📦 Creating Data Source: {DS_NAME}")
    print(f"   KB: {EXISTING_KB_ID}")
    print(f"   S3: s3://{S3_BUCKET}/{S3_PREFIX}/")

    # Check if it already exists
    try:
        sources = bedrock_agent.list_data_sources(knowledgeBaseId=EXISTING_KB_ID, maxResults=100)
        for ds in sources.get("dataSourceSummaries", []):
            if ds["name"] == DS_NAME:
                ds_id = ds["dataSourceId"]
                print(f"  ✅ Data source '{DS_NAME}' already exists: {ds_id}")
                return ds_id
    except Exception:
        pass

    try:
        response = bedrock_agent.create_data_source(
            knowledgeBaseId=EXISTING_KB_ID,
            name=DS_NAME,
            description=f"Section-based use case files under {S3_PREFIX}/",
            dataSourceConfiguration={
                "type": "S3",
                "s3Configuration": {
                    "bucketArn": f"arn:aws:s3:::{S3_BUCKET}",
                    "inclusionPrefixes": [f"{S3_PREFIX}/"]
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
        print(f"  ✅ Data Source created: {ds_id}")
        print(f"     Prefix:   {S3_PREFIX}/")
        print(f"     Chunking: FIXED_SIZE (300 tokens, 15% overlap)")
        return ds_id

    except Exception as e:
        print(f"  ❌ Failed to create data source: {e}")
        raise


def update_env_file(kb_id: str, ds_id: str):
    """Append the V2 IDs to .env file."""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")

    with open(env_path, "r") as f:
        content = f.read()

    lines_to_add = []
    if "KNOWLEDGE_BASE_ID_V2" not in content:
        lines_to_add.append(f"KNOWLEDGE_BASE_ID_V2={kb_id}")
    if "DATA_SOURCE_ID_V2" not in content:
        lines_to_add.append(f"DATA_SOURCE_ID_V2={ds_id}")

    if lines_to_add:
        with open(env_path, "a") as f:
            f.write("\n# Section-based KB (updated approach)\n")
            for line in lines_to_add:
                f.write(f"{line}\n")
        print(f"\n✅ Added to .env:")
        for line in lines_to_add:
            print(f"   {line}")
    else:
        print(f"\n✅ .env already has V2 IDs.")


def main():
    print("=" * 60)
    print("  Setup Data Source — Section-Based Approach")
    print("=" * 60)
    print(f"  Region:     {REGION}")
    print(f"  S3 Bucket:  {S3_BUCKET}")
    print(f"  S3 Prefix:  {S3_PREFIX}/")
    print(f"  Existing KB: {EXISTING_KB_ID}")
    print()

    # Create data source on existing KB
    ds_id = create_data_source()

    # Update .env with the IDs
    # KB ID is the same as existing; DS ID is new
    update_env_file(EXISTING_KB_ID, ds_id)

    print(f"\n{'='*60}")
    print(f"  ✅ Setup Complete!")
    print(f"{'='*60}")
    print(f"  Knowledge Base ID : {EXISTING_KB_ID} (reusing existing)")
    print(f"  Data Source ID    : {ds_id} (NEW — section-based)")
    print(f"\n  Next steps:")
    print(f"  1. Run: python dynamo_to_s3_usecase_sections.py")
    print(f"     (uploads sections to S3 and syncs KB)")
    print(f"  2. Query: python query_kb_v2.py")


if __name__ == "__main__":
    main()
