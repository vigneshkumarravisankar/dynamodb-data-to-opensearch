from opensearchpy import OpenSearch
import os
from dotenv import load_dotenv

load_dotenv()

os_client = OpenSearch(
    hosts=[{"host": os.environ["OS_HOST"], "port": 443}],
    use_ssl=True,
    verify_certs=True,
)

# Check cluster info
print(os_client.info())

# Check if index exists
print(os_client.indices.exists(index=os.environ["INDEX_NAME"]))