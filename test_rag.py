import os
import logging
from dotenv import load_dotenv
from main import rag_search, generate_answer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Example queries for RAG testing
TEST_QUERIES = [
    "List all fields and their values for a sample record.",
    "Show the metadata for the first 3 records.",
    "What are the most common frameworks in the dataset?",
    "Give a summary of all available fields in the dataset.",
    "Show a record with all fields populated."
]

def test_rag():
    for query in TEST_QUERIES:
        logger.info("=" * 60)
        logger.info(f"Testing RAG with query: {query}")
        results = rag_search(query, top_k=5)
        answer = generate_answer(query, results)
        logger.info("RAG Results:")
        logger.info(results)
        logger.info("RAG Answer:")
        logger.info(answer)
        logger.info("=" * 60)

if __name__ == "__main__":
    test_rag()
