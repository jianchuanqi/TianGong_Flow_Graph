import pandas as pd
import re

# 1. 读入原始CSV
df = pd.read_csv('flow_mapping.csv')

# 确保Database列存在（如果没有请补上/更名/手工指定）
assert 'Database' in df.columns, "原始数据需有 'Database' 列（如无请补）"

# 2. 主要字段预处理函数
def std_text(s):
    if pd.isnull(s):
        return ''
    s = str(s).lower()
    s = re.sub(r'[^a-z0-9]', '', s)  # 只保留字母数字
    return s

# Name标准化
df['name_std'] = df['Name'].apply(std_text)

# Synonym合并并标准化
def merge_synonyms(syn):
    if pd.isnull(syn):
        return set()
    names = set([std_text(x) for x in str(syn).split('\n') if x.strip()])
    return names

df['synonyms_std'] = df['Synonym'].apply(merge_synonyms)

# Compartment/Subcompartment标准化
df['compartment_std'] = df['Compartment'].apply(std_text)
df['subcompartment_std'] = df['Subcompartment'].apply(std_text)

# CAS/Formula标准化（缺失则空字符串）
df['cas_std'] = df['CAS Number'].apply(std_text)
df['formula_std'] = df['Formula'].apply(std_text)

# 3. 生成聚合锚点（Flow锚点）
def flow_key(row):
    # 先CAS
    if row['cas_std']:
        return f"{row['cas_std']}__{row['compartment_std']}__{row['subcompartment_std']}"
    else:
        # 没有CAS时，用标准化名称和同义词
        all_names = [row['name_std']] + list(row['synonyms_std'])
        all_names = sorted(set([n for n in all_names if n]))
        name_hash = '_'.join(all_names)
        return f"{name_hash}__{row['compartment_std']}__{row['subcompartment_std']}"

df['flow_key'] = df.apply(flow_key, axis=1)

# 4. 增加联合主键列
df['database'] = df['Database'].astype(str)
df['unique_id'] = df['database'] + '||' + df['ID'].astype(str)

# 5. 检查结果
print(df[['ID', 'Database', 'unique_id', 'Name', 'name_std', 'synonyms_std', 'cas_std', 'formula_std', 'compartment_std', 'subcompartment_std', 'flow_key']])

# 6. 存储供下一步使用
df.to_pickle('step1_cleaned.pkl')