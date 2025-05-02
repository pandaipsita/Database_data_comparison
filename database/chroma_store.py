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

    # Prepare data for ChromaDB
    documents = [chunk["content"] for chunk in data_chunks]
    metadatas = [chunk["metadata"] for chunk in data_chunks]
    ids = [f"{chunk['metadata']['schema']}_{chunk['metadata']['table']}_{chunk['metadata']['row_id']}" for chunk in
           data_chunks]

    # Add documents to collection in batches to avoid memory issues
    batch_size = 100
    for i in range(0, len(documents), batch_size):
        end = min(i + batch_size, len(documents))
        collection.add(
            documents=documents[i:end],
            metadatas=metadatas[i:end],
            ids=ids[i:end]
        )

    print(f"✅ Stored {len(data_chunks)} data chunks in ChromaDB.")
    return collection