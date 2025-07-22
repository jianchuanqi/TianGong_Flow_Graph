from py2neo import Graph
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")

# 1. 连接Neo4j
graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# 2. 读取等价pair
df = pd.read_csv("llm_disambig_ctxfiltered.csv")
df = df[df["llm_result"] == "是"]


q = """
    MATCH (f1:Flow)-[:HAS_RECORD]->(r1:DatabaseRecord {id: $id_a, database: $db_a}),
          (f2:Flow)-[:HAS_RECORD]->(r2:DatabaseRecord {id: $id_b, database: $db_b})
    WHERE f1.flow_id <> f2.flow_id
    MERGE (f1)-[:SAME_AS]->(f2)
    """

seen = set()
for _, row in df.iterrows():
    # 对A-B小组合唯一
    key1 = (row["id_a"], row["database_a"], row["id_b"], row["database_b"])
    key2 = (row["id_b"], row["database_b"], row["id_a"], row["database_a"])
    if key1 in seen or key2 in seen:
        continue
    seen.add(key1)
    seen.add(key2)
    # A→B
    graph.run(
        q,
        id_a=row["id_a"],
        db_a=row["database_a"],
        id_b=row["id_b"],
        db_b=row["database_b"],
    )
    # B→A
    graph.run(
        q,
        id_a=row["id_b"],
        db_a=row["database_b"],
        id_b=row["id_a"],
        db_b=row["database_a"],
    )

print("全部等价流对已批量写入Neo4j SAME_AS边。")
