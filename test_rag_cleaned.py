import json
import logging
from main import rag_search, generate_answer

def clean_metadata(metadata):
    """
    Cleans and formats metadata for display/testing.
    Converts lists, sets, and URLs to readable strings.
    """
    cleaned = {}
    for k, v in metadata.items():
        if isinstance(v, (list, set)):
            cleaned[k] = ", ".join(str(x) for x in v)
        elif isinstance(v, str) and v.startswith("http"):
            cleaned[k] = f"[link]({v})"
        else:
            cleaned[k] = v
    return cleaned


def test_rag_cleaned():
    query = "Show a record with all fields populated."
    results = rag_search(query, top_k=5)
    logging.info("Raw RAG Results:")
    logging.info(json.dumps(results, indent=2))
    logging.info("\nCleaned RAG Results:")
    for i, r in enumerate(results, 1):
        cleaned = clean_metadata(r.get("metadata", {}))
        logging.info(f"Record {i}:\n" + json.dumps(cleaned, indent=2))
    answer = generate_answer(query, results)
    logging.info("\nRAG Answer:")
    logging.info(answer)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_rag_cleaned()
