import openai
import pandas as pd
from tqdm import tqdm
import time
import json
import re
from dotenv import load_dotenv
import os

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL")

# 1. 读取pair表与原始DataFrame
pair_df = pd.read_csv("faiss_pairs_ctxfiltered_simfiltered.csv")
df = pd.read_pickle("step2_with_embedding.pkl")

# 用（database, ID）做多级索引，保证唯一
df["ID"] = df["ID"].astype(str)
df["database"] = df["Database"].astype(str)
df.set_index(["database", "ID"], inplace=True, drop=False)


# 2. 构造判别描述
def build_desc(record):
    return (
        f"Name: {record.get('Name', '')}; "
        f"Synonym: {record.get('Synonym', '')}; "
        f"Compartment: {record.get('Compartment', '')}; "
        f"Subcompartment: {record.get('Subcompartment', '')}; "
        # f"Formula: {record.get('Formula', '')}; "
        # f"CAS: {record.get('CAS Number', '')}; "
        f"Database: {record.get('Database', '')}; "
        f"ID: {record.get('ID', '')}"
    )


def safe_json_loads(answer):
    if not answer:
        return {"result": "未知", "reason": "LLM输出为空"}
    match = re.search(r"\{[\s\S]*?\}", answer)
    if match:
        try:
            return json.loads(match.group())
        except Exception as e:
            return {"result": "未知", "reason": f"JSON解析失败: {e}"}
    return {"result": "未知", "reason": "无JSON结构"}


def llm_judge(a_desc, b_desc):
    prompt = (
        "结合你自己的知识和信息，严格判断以下两组物质流描述是否为同一种。即使它们的名称或部分描述很接近，但只要结构、成分、材料、化学式、产品型号或关键属性存在差别，都判为不是同一种，并简单说明理由。不要仅凭字面相似做判断。只返回如下JSON结构，不要输出其它内容：\n"
        '{ "result": "是/否", "reason": "简要说明" }\n'
        f"A: {a_desc}\n"
        f"B: {b_desc}\n"
    )
    retry = 3
    while retry:
        try:
            resp = openai.chat.completions.create(
                model=LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=128,
            )
            answer = resp.choices[0].message.content.strip()
            return safe_json_loads(answer)
        except Exception as e:
            retry -= 1
            print(f"API失败重试：{e}")
            time.sleep(2)
    return {"result": "未知", "reason": "LLM异常"}


results = []
for idx, row in tqdm(pair_df.iterrows(), total=len(pair_df), desc="LLM判别"):
    id_a, db_a = str(row["id_a"]), str(row["database_a"])
    id_b, db_b = str(row["id_b"]), str(row["database_b"])
    rec_a = df.loc[(db_a, id_a)]
    rec_b = df.loc[(db_b, id_b)]
    a_desc = build_desc(rec_a)
    b_desc = build_desc(rec_b)
    out = llm_judge(a_desc, b_desc)
    results.append(
        {
            "id_a": id_a,
            "database_a": db_a,
            "id_b": id_b,
            "database_b": db_b,
            "sim": row["sim"],
            "llm_result": out.get("result", "未知"),
            "llm_reason": out.get("reason", ""),
        }
    )

result_df = pd.DataFrame(results)
result_df.to_csv("llm_disambig_ctxfiltered.csv", index=False)
print("LLM判别完成，结果保存至 llm_disambig_ctxfiltered.csv")
