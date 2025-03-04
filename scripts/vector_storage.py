from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from chromadb import Documents, EmbeddingFunction, Embeddings
from google.oauth2 import service_account
from langchain_chroma import Chroma
import chromadb
import vertexai
import os

class VertexAIEmbeddingFunction(EmbeddingFunction):
    """
    Custom embedding function to interface with Vertex AI's text embedding model.
    """

    def __init__(
        self,
        model_name: str,
        credentials_path: str,
        project_id: str,
        location: str = "us-central1",
    ):
        """
        Initialize the embedding function.

        Args:
            model_name (str): Vertex AI embedding model name.
            credentials_path (str): Path to the service account JSON file.
            project_id (str): Google Cloud project ID.
            location (str): Google Cloud region.
        """
        self.model_name = model_name
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        vertexai.init(project=project_id, location=location, credentials=credentials)
        self.embedding_model = TextEmbeddingModel.from_pretrained(model_name)

    def embed_query(self, query: str) -> list[float]:
        """
        Generate an embedding for a single query.
        """
        inputs = [TextEmbeddingInput(text=query, task_type="RETRIEVAL_QUERY")]
        embedding = self.embedding_model.get_embeddings(inputs)
        return embedding[0].values

    def __call__(self, docs: Documents) -> Embeddings:
        """
        Generate embeddings for a list of input documents.

        Args:
            docs (Documents): List of strings to generate embeddings for.

        Returns:
            Embeddings: A list of embedding vectors.
        """
        try:
            batch_size = 100
            all_embeddings = []

            # Split documents into batches
            for i in range(0, len(docs), batch_size):
                batch_docs = docs[i : i + batch_size]
                # Generate embeddings for the current batch
                inputs = [
                    TextEmbeddingInput(text=str(document), task_type="RETRIEVAL_QUERY")
                    for document in batch_docs
                ]
                batch_embeddings = self.embedding_model.get_embeddings(inputs)
                all_embeddings.extend(
                    [embedding.values for embedding in batch_embeddings]
                )

            return all_embeddings

        except Exception as e:
            print(f"Error generating embeddings: {e}")
            raise


class ChromaDBHandler:
    """
    Class to handle ChromaDB operations for document embeddings and queries.
    """

    def __init__(self, embedding_function: EmbeddingFunction):
        self.client = chromadb.PersistentClient(path="vector_database")
        self.embedding_function = embedding_function

    def get_or_create_collection(self, collection_name: str):
        """
        Get or create a ChromaDB collection.

        Args:
            collection_name (str): Name of the collection.
        """
        try:
            return self.client.get_or_create_collection(
                name=collection_name,
                embedding_function=self.embedding_function,
                metadata={
                    "hnsw:space": "cosine",
                    "hnsw:search_ef": 100,
                    "hnsw:construction_ef": 200,
                },
            )
        except Exception as e:
            print(f"Error managing ChromaDB collection: {e}")
            raise

    def query_collection(self, collection, query_embeddings: str, n_results: int = 3):
        """
        Query a ChromaDB collection.

        Args:
            collection: The ChromaDB collection.
            query_embeddings (str): Query embeddings.
            n_results (int): Number of results to retrieve.

        Returns:
            tuple: Retrieved data and scores.
        """
        try:
            chromadb_result = collection.query(
                query_embeddings=query_embeddings, n_results=n_results
            )
            retrieved_data = []
            scores = {}
            for idx, metadata in enumerate(chromadb_result["metadatas"][0]):
                table_name = metadata["table"]
                columns = chromadb_result["documents"][0][idx].split(", ")
                similarity_score = chromadb_result["distances"][0][idx]
                scaled_score = round((1 - similarity_score) * 100, 2)
                retrieved_data.append(f"table:{table_name}\n columns:{columns}")
                scores[table_name] = scaled_score
            return retrieved_data, scores
        except Exception as e:
            print(f"Error querying ChromaDB: {e}")
            raise

    def load_vector_storage(self):
        """
        Load the vector storage.

        Returns:
            Chroma: The loaded vector storage using Langchain Chroma.
        """
        return Chroma(
            client=self.client,
            collection_name="data_migration",
            embedding_function=self.embedding_function,
        )

    def add_to_collection(self, collection, tables):
        """
        Add tables, embeddings, metadata, and IDs to a ChromaDB collection.

        Args:
            collection: The ChromaDB collection to which data will be added.
            tables (list): List of tables containing metadata with table_name.
        """
        try:
            documents = list(map(str, tables))  # Convert tables to string format for documents
            collection.add(
                documents=documents,
                embeddings=self.embedding_function(documents),
                metadatas=[{"table": list(table.keys())[0]} for table in tables], #[{"table": table["table_name"]} for table in tables],
                ids=list(map(str, range(len(tables)))),
            )
        except Exception as e:
            print(f"Error adding data to ChromaDB collection: {e}")
            raise