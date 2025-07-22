import openai
import pandas as pd
from tqdm import tqdm
import numpy as np
import time
from dotenv import load_dotenv
import os

load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")
EMBED_MODEL = os.getenv("EMBED_MODEL")

# 1. 读取pkl
df = pd.read_pickle("step1_cleaned.pkl")


# 2. 合成用于embedding的文本（Name + Synonym + Compartment + Subcompartment 等）
def build_text(row):
    fields = [
        str(row.get("Name", "")),
        str(row.get("Synonym", "")),
        str(row.get("Compartment", "")),
        str(row.get("Subcompartment", "")),
        str(row.get("Formula", "")),
        str(row.get("CAS Number", "")),
    ]
    return " | ".join([f for f in fields if f and f.lower() != "nan"])


df["text_for_embed"] = df.apply(build_text, axis=1)


# 3. 批量获取embedding
def get_embedding(text):
    retry = 5
    while retry:
        try:
            resp = openai.embeddings.create(input=text[:3000], model=EMBED_MODEL)
            emb = np.array(resp.data[0].embedding, dtype=np.float32)
            return emb
        except Exception as e:
            print(f"Error: {e} -> 重试")
            retry -= 1
            time.sleep(2)
    return np.zeros(1536, dtype=np.float32)  # 保证格式兼容


# 批量生成embedding
embeddings = []
for text in tqdm(df["text_for_embed"], desc="Embedding生成"):
    emb = get_embedding(text)
    embeddings.append(emb)
df["embedding"] = embeddings

# 4. 保存带embedding的新pkl
df.to_pickle("step2_with_embedding.pkl")
print("已保存带embedding的DataFrame至 step2_with_embedding.pkl")
