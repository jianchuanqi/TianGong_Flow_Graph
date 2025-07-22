import pandas as pd
import numpy as np
import faiss
from tqdm import tqdm

# 1. 读取带embedding的DataFrame
df = pd.read_pickle('step2_with_embedding.pkl')
# 标准化字段，避免有空值/None
df['compartment'] = df['Compartment'].fillna('').astype(str)
df['subcompartment'] = df['Subcompartment'].fillna('').astype(str)
df['database'] = df['Database'].fillna('').astype(str)
df['ID'] = df['ID'].astype(str)

# 2. embedding矩阵
embeddings = np.stack(df['embedding'].to_list()).astype('float32')
n, d = embeddings.shape

# 3. L2归一化（cosine相似度）
faiss.normalize_L2(embeddings)

# 4. 建Faiss索引
index = faiss.IndexFlatIP(d)
index.add(embeddings)

# 5. 批量召回Top-K近邻
K = 30  # 可调
D, I = index.search(embeddings, K + 1)  # 包含自身，后续去除

# 6. 只保留compartment/subcompartment完全一致的pair，并带database字段
pairs = []
for i in tqdm(range(n), desc='召回TopK并过滤'):
    comp_i = df.iloc[i]['compartment']
    subcpt_i = df.iloc[i]['subcompartment']
    db_i = df.iloc[i]['database']
    id_i = df.iloc[i]['ID']
    for rank, (j, sim) in enumerate(zip(I[i], D[i])):
        if i == j:
            continue  # 跳过自身
        if i < j:    # 避免重复pair
            comp_j = df.iloc[j]['compartment']
            subcpt_j = df.iloc[j]['subcompartment']
            db_j = df.iloc[j]['database']
            id_j = df.iloc[j]['ID']
            if (comp_i == comp_j) and (subcpt_i == subcpt_j):
                pairs.append({
                    'idx_a': i,
                    'idx_b': j,
                    'id_a': id_i,
                    'database_a': db_i,
                    'id_b': id_j,
                    'database_b': db_j,
                    'compartment': comp_i,
                    'subcompartment': subcpt_i,
                    'sim': sim,
                    'rank': rank
                })

# 7. 转为DataFrame并保存
pair_df = pd.DataFrame(pairs)
pair_df.to_csv('faiss_pairs_ctxfiltered.csv', index=False)
print(f'Faiss召回并严格上下文过滤完成，pair数={len(pair_df)}，已保存至faiss_pairs_ctxfiltered.csv')

# 8. 可选：再按sim阈值过滤
THRESH_SIM = 0.80
pair_df_filtered = pair_df[pair_df['sim'] > THRESH_SIM]
pair_df_filtered.to_csv('faiss_pairs_ctxfiltered_simfiltered.csv', index=False)
print(f'再按sim>{THRESH_SIM}过滤，pair数={len(pair_df_filtered)}，已保存至faiss_pairs_ctxfiltered_simfiltered.csv')