import chromadb
from langchain_ollama import OllamaEmbeddings
from chromadb.utils.embedding_functions import EmbeddingFunction


class LangchainEmbeddingFunction(EmbeddingFunction):
    """
    Adapter class to make langchain embeddings compatible with ChromaDB
    """

    def __init__(self, model_name):
        self.embedding_model = OllamaEmbeddings(model=model_name)

    def __call__(self, input):
        """
        Generate embeddings for the input texts

        Args:
            input: List of texts to embed

        Returns:
            List of embeddings, one per input text
        """
        embeddings = []
        for text in input:
            embedding = self.embedding_model.embed_query(text)
            embeddings.append(embedding)

        return embeddings


def store_data(data_chunks, config):
    """
    Store data chunks in ChromaDB with embeddings

    Args:
        data_chunks: List of dicts with content and metadata
        config: Configuration dictionary

    Returns:
        collection: ChromaDB collection with stored data
    """
    # Use PersistentClient for on-disk storage
    chroma_client = chromadb.PersistentClient(
        path=config["chromadb_path"]
    )

    # Create embedding function
    embedding_function = LangchainEmbeddingFunction(config["embedding_model"])

    # Create or get collection
    collection = chroma_client.get_or_create_collection(
        name="data",
        embedding_function=embedding_function
    )

    # Group chunks by file_id for efficient deletion of existing data
    chunks_by_file = {}
    for chunk in data_chunks:
        file_id = chunk["metadata"].get("file_id")
        if file_id:
            if file_id not in chunks_by_file:
                chunks_by_file[file_id] = []
            chunks_by_file[file_id].append(chunk)

    # Process each file_id separately
    for file_id, file_chunks in chunks_by_file.items():
        # Delete existing chunks for this file_id
        existing = collection.get(where={"file_id": file_id})
        if existing["ids"]:
            collection.delete(ids=existing["ids"])

        # Prepare data for ChromaDB
        documents = [chunk["content"] for chunk in file_chunks]
        metadatas = [chunk["metadata"] for chunk in file_chunks]
        ids = [
            f"{chunk['metadata'].get('schema', 'default')}_{chunk['metadata'].get('table', 'default')}_{chunk['metadata'].get('row_id', i)}"
            for i, chunk in enumerate(file_chunks)]

        # Add documents to collection in batches to avoid memory issues
        batch_size = 100
        for i in range(0, len(documents), batch_size):
            end = min(i + batch_size, len(documents))
            collection.add(
                documents=documents[i:end],
                metadatas=metadatas[i:end],
                ids=ids[i:end]
            )

        print(f"✅ Processed file_id {file_id}: deleted existing chunks and stored {len(file_chunks)} new chunks")

    # Handle chunks without file_id
    no_file_chunks = [chunk for chunk in data_chunks if "file_id" not in chunk["metadata"]]
    if no_file_chunks:
        # Prepare data for ChromaDB
        documents = [chunk["content"] for chunk in no_file_chunks]
        metadatas = [chunk["metadata"] for chunk in no_file_chunks]
        ids = [
            f"{chunk['metadata'].get('schema', 'default')}_{chunk['metadata'].get('table', 'default')}_{chunk['metadata'].get('row_id', i + len(data_chunks))}"
            for i, chunk in enumerate(no_file_chunks)]

        # Add documents to collection in batches
        batch_size = 100
        for i in range(0, len(documents), batch_size):
            end = min(i + batch_size, len(documents))
            collection.add(
                documents=documents[i:end],
                metadatas=metadatas[i:end],
                ids=ids[i:end]
            )

        print(f"✅ Stored {len(no_file_chunks)} data chunks without file_id in ChromaDB.")

    print(f"✅ Total: Stored {len(data_chunks)} data chunks in ChromaDB.")
    return collection