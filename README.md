# TianGong Flow Graph

This project provides a full workflow for data processing, knowledge graph construction, vector embedding, similarity calculation, and LLM-based judgment. It is designed for mapping flows in LCA.

## Quick Start

### Install Dependencies

Python 3.8+ is recommended.

pip install -r requirements.txt

### Configure Environment Variables (.env)

This project uses a .env file for sensitive information and configuration. Create a .env file in the root directory with content like:

```plaintext
# OpenAI API
API_KEY=sk-xxxxxxx

# Neo4j configuration
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASS=your_neo4j_password
```

Note: Do not commit .env to GitHub. The .gitignore file excludes it by default.

### Start Neo4j Database

If you don’t have Neo4j installed, see the Neo4j docs.

Quick start with Docker:

```bash
docker run -d \
 --name neo4j \
 -p 7687:7687 -p 7474:7474 \
 -e NEO4J_AUTH=neo4j/your_neo4j_password \
 neo4j:5
```

### Run the Entire Pipeline

Run python scripts from 1 to 6.

```bash
python 1_data_processing.py
python 2_knowledge_graph.py
python 3_vector_embedding.py
python 4_similarity_calculation.py
python 5_llm_judgment.py
python 6_main.py
```

### Start FastAPI

Run the FastAPI server:

```bash
uvicorn 7_main:app --reload
```
