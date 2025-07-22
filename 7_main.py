from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from neo4j import GraphDatabase, basic_auth
from dotenv import load_dotenv
import os

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")

app = FastAPI()
driver = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASS))


class Record(BaseModel):
    record_id: str
    database: str
    name: Optional[str]


class QueryResult(BaseModel):
    results: List[Record]


@app.get("/api/related-records", response_model=QueryResult)
def get_related_records(id: str = Query(..., description="Record UUID")):
    cypher = """
    MATCH (start:DatabaseRecord {id: $id})
    <-[:HAS_RECORD]-(f:Flow)
    OPTIONAL MATCH (f)-[:SAME_AS*0..]-(eq:Flow)
    MATCH (eq)-[:HAS_RECORD]->(rec:DatabaseRecord)
    WHERE NOT (rec.id = start.id AND rec.database = start.database)
    RETURN DISTINCT rec.id AS record_id, rec.database AS database, rec.name AS name
    """
    with driver.session() as session:
        try:
            res = session.run(cypher, id=id)
            rows = [
                Record(
                    record_id=r["record_id"], database=r["database"], name=r.get("name")
                )
                for r in res
            ]
            return {"results": rows}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


import atexit

atexit.register(lambda: driver.close())

# uvicorn 7_main:app --reload