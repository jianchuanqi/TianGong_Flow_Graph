import pandas as pd
from py2neo import Graph, Node, Relationship
import uuid
from dotenv import load_dotenv
import os

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")

# 连接Neo4j
graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

# 读取清洗后的数据
df = pd.read_pickle("step1_cleaned.pkl")


# ---在读取df后、入库循环前加---
def gen_flow_key(row):
    if row["cas_std"] and row["formula_std"]:
        return f"{row['cas_std']}__{row['formula_std']}__{row['compartment_std']}__{row['subcompartment_std']}__{row['name_std']}"
    else:
        all_names = [row["name_std"]] + list(row["synonyms_std"])
        all_names = sorted(set([n for n in all_names if n]))
        name_hash = "_".join(all_names)
        return f"{name_hash}__{row['compartment_std']}__{row['subcompartment_std']}"


df["flow_key"] = df.apply(gen_flow_key, axis=1)

# 用于Flow节点唯一性判定
flow_nodes = {}

for idx, row in df.iterrows():
    # 1. Flow节点（以flow_key唯一）
    flow_id = flow_nodes.setdefault(row["flow_key"], str(uuid.uuid4()))
    flow_node = Node(
        "Flow",
        flow_id=flow_id,
        cas=row["cas_std"],
        formula=row["formula_std"],
        preferred_name=row["name_std"],
        synonyms=list(row["synonyms_std"]),
        compartment=row["compartment_std"],
        subcompartment=row["subcompartment_std"],
    )
    graph.merge(flow_node, "Flow", "flow_id")

    # 2. DatabaseRecord节点，联合主键（id, database）
    record_node = Node(
        "DatabaseRecord",
        id=row["ID"],
        name=row["Name"],
        compartment=row["Compartment"],
        subcompartment=row["Subcompartment"],
        cas=row["CAS Number"],
        formula=row["Formula"],
        synonyms=row["Synonym"],
        database=row["Database"],
    )
    graph.merge(record_node, "DatabaseRecord", ("id", "database"))

    # 3. HAS_RECORD关系
    rel = Relationship(flow_node, "HAS_RECORD", record_node)
    graph.merge(rel)

print("数据已全部导入Neo4j。")
