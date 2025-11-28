#%%
import sys
import os

# Add dir to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "code_vm"))

from agrupamento_nomes import group_names_df_v2, business_names # type: ignore
from agrupamento_nomes_v2 import group_names_df_v2 as group_names_df_v3 #type: ignore
import pandas as pd
from IPython.display import display


def example_new(function):
    
    # Dados de exemplo
    data = {
        'nome': business_names
    }

    df = pd.DataFrame(data)
    df['inicio'] = df['nome'].str[0]

    df = df.sort_values(by='nome')

    df = function(df, group_cols='inicio', nome_col='nome')

    #df = df.drop(columns=['inicio', 'retirado_de_resultado', 'resultado_names'])
     #Exibir o DataFrame resultante

    return df

def display_examples():
        
    df = example_new(group_names_df_v2)
    display(df)


    df = example_new(group_names_df_v3)
    display(df)

#%%

def check_time():
        
    import timeit

    # tempo da func1
    tempo1 = timeit.timeit("example_new(group_names_df_v2)", globals=globals(), number=10000)
    # tempo da func2
    tempo2 = timeit.timeit("example_new(group_names_df_v3)", globals=globals(), number=10000)

    print(f"group_names_df_v2: {tempo1:.5f} segundos")
    print(f"group_names_df_v3: {tempo2:.5f} segundos")

    """group_names_df_v2: 102.76884 segundos
       group_names_df_v3: 71.47461 segundos"""


#%%

from group_tam_id import (
    load_to_gbq,
    group_and_clean,

    deal_merged_places,
    deal_merged_docs,
    pos_tratamento,
    execute_with_context,
    assign_group_ids,
)

import time
import numpy as np

def prepare_data(df):
    return df.copy()


def init_group_id(df, LEVEL_GROUP):

    QTD_NOME_COMUM = 20
    df = df.copy()

    nomes_comuns = df['nome_master'].value_counts()
    nomes_comuns = nomes_comuns[nomes_comuns>QTD_NOME_COMUM].index.to_list()

    df['group_id'] = df['resultado_names'].apply(lambda x: list(np.sort(x))[-1]) 

    df['new_id'] = np.arange(df.shape[0])
    df.loc[df['nome_master'].isin(nomes_comuns), 'group_id'] = 'Nome Comum: ' + df['new_id'].astype(str)

    df = df.rename(columns={'resultado_names': 'grouped_names'})
    df = df.drop(columns=['new_id'])

    df = assign_group_ids(df, ['group_id'], final_col='group_idx_nome', LEVEL_GROUP=LEVEL_GROUP)

    return df





def deal_unmerged_places(df, LEVEL_GROUP):
    df = df.copy()


    df['id_merge_places'] = df['merchant_market_hierarchy_id'].apply(lambda x: str(x) if not pd.isna(x) else np.nan) #deal with nan in Int
    df['new_id'] = np.arange(df.shape[0])
    df.loc[df['subs_asterisk']=='Ton', 'id_merge_places'] = 'Ton' + df['id_ton'].astype(str)
    df.loc[df['id_merge_places'].isnull(), 'id_merge_places'] = 'created' + df['new_id'].astype(str)


    nome_grupo = 'group_idx_merge_places'

    df = assign_group_ids(df, ['group_idx_nome', 'id_merge_places'], final_col=nome_grupo, LEVEL_GROUP=LEVEL_GROUP)
    

    return df


def main_data_treat_muni_v1(df, file_name):

    """
    Main function to process data with options to display intermediate DataFrames and print verbose messages.

    Parameters:
    - read_data_func: Function to read initial data.
    - display_flag: Boolean flag indicating whether to display the DataFrame after each processing step.
    - verbose: Boolean flag indicating whether to print verbose messages during processing.
    """
    display_flag=False

    verbose = True
    LEVEL_GROUP = ['cod_muni']
    
    
    print(f'Starting main data treat for file {file_name}, with size {len(df)}')

    start = time.time()
    execute_ctx = execute_with_context(LEVEL_GROUP, display_flag=display_flag, verbose=verbose)

    df = df.sort_values(by='nome')
    function = group_names_df_v3
    df = df.groupby(LEVEL_GROUP, as_index=False
                    ).apply(function, group_cols='inicio', nome_col='nome')
    display(df)

    
    df = execute_ctx(prepare_data, df)
    df = execute_ctx(init_group_id, df)
    df = execute_ctx(deal_unmerged_places, df)
    df = execute_ctx(deal_merged_places, df)
    df = execute_ctx(deal_merged_docs, df)
    df = execute_ctx(pos_tratamento, df)
    print(f'Main data treat Execution time file {file_name}: {time.time() - start:.2f} seconds')
    return df


business_names = [
 'ANDREINA',
 'ANDREIA',
 'ANDREIAESTETICAEB',
 'ANDREIAESTETICAEBRONZ',
 'AUTOCAR',
 'AUTOCARRO1',
 'AUTOCARCENTRO',
 'AUTOCARRO2'
]


# Dados de exemplo
data = {
    'nome': business_names
}

df = pd.DataFrame(data)

mmhid_groups = {
    #'1': ['AUTOCARCENTRO', 'AUTOCARRO1'],
    #'2': ['ANDREIA', 'ANDREINA'],
    '3': ['AUTOCARRO2', 'ANDREIA'],
    '4': ['AUTOCAR', 'ANDREIAESTETICAEB'],

}

df['merchant_market_hierarchy_id'] = df['nome'].apply(
    lambda x: next((key for key, values in mmhid_groups.items() if any(v == x for v in values)), pd.NA)
)

df['subs_asterisk']= 'Outros'
df['id_ton'] = 'x'
df['numero_inicio'] = ''
df['cpf'] = ''
df['cnpj'] = ''



df['inicio'] = df['nome'].str[0]


df['cod_muni'] = '1'
df['merchant_tax_id'] = '39988858892'
df['nome_master'] = df['nome']

df_original = df.copy()

#%%

df = df_original.copy()





df = main_data_treat_muni_v1(df, 'nome')
if 'tam_id' in df.columns:
    df['tam_id'] = df['tam_id'].astype(str).str[:-1]
display(df)

df['nome_mmhid'] = df['nome'] + '|' + df['merchant_market_hierarchy_id'].astype(str).replace('<NA>', '  ')

if 'tam_id' in df.columns:
    display(df.groupby(['tam_id'])['nome_mmhid'].apply(list).reset_index())


#%%



df = df_original.copy()

display(df)



def main_data_treat_muni_v2(df, file_name):

    """
    Main function to process data with options to display intermediate DataFrames and print verbose messages.

    Parameters:
    - read_data_func: Function to read initial data.
    - display_flag: Boolean flag indicating whether to display the DataFrame after each processing step.
    - verbose: Boolean flag indicating whether to print verbose messages during processing.
    """
    display_flag=False

    verbose = True
    LEVEL_GROUP = ['cod_muni']
    
    
    print(f'Starting main data treat for file {file_name}, with size {len(df)}')

    start = time.time()
    execute_ctx = execute_with_context(LEVEL_GROUP, display_flag=display_flag, verbose=verbose)
    
    df = execute_ctx(prepare_data, df)
    df = execute_ctx(init_group_id, df)

    df = execute_ctx(pos_tratamento, df)
    print(f'Main data treat Execution time file {file_name}: {time.time() - start:.2f} seconds')
    return df


df = main_data_treat_muni_v2(df, 'nome')
if 'tam_id' in df.columns:
    df['tam_id'] = df['tam_id'].astype(str).str[:-1]
display(df)

df['nome_mmhid'] = df['nome'] + '|' + df['merchant_market_hierarchy_id'].astype(str).replace('<NA>', '  ')

if 'tam_id' in df.columns:
    display(df.groupby(['tam_id'])['nome_mmhid'].apply(list).reset_index())


#%%

import pandas as pd
from rapidfuzz.distance.Levenshtein import distance as levenshtein

business_names = [
 'ANDREINA',
 'ANDREIA',
 'ANDREIAESTETICAEB',
 'ANDREIAESTETICAEBRONZ',
 'AUTOCAR',
 'AUTOCARRO1',
 'AUTOCARCENTRO',
 'AUTOCARRO2'
]

# Dados de exemplo
data = {
    'nome': business_names
}

df = pd.DataFrame(data)

mmhid_groups = {
    #'1': ['AUTOCARCENTRO', 'AUTOCARRO1'],
    #'2': ['ANDREIA', 'ANDREINA'],
    '3': ['AUTOCARRO2', 'ANDREIA'],
    '4': ['AUTOCAR', 'ANDREIAESTETICAEB'],

}

df['merchant_market_hierarchy_id'] = df['nome'].apply(
    lambda x: next((key for key, values in mmhid_groups.items() if any(v == x for v in values)), pd.NA)
)





import numpy as np
from rapidfuzz import process, distance, fuzz


names = df[df['merchant_market_hierarchy_id'].isna()]['nome'].to_list()
names2 = df[df['merchant_market_hierarchy_id'].notna()]['nome'].to_list()
print(names)
print(names2)


from rapidfuzz.distance import Levenshtein, Indel
from rapidfuzz import fuzz

def common_prefix_len(a: str, b: str) -> int:
    i, m = 0, min(len(a), len(b))
    while i < m and a[i] == b[i]:
        i += 1
    return i

def best_matches_indel(queries, choices):
    """
    Nearest neighbor by Indel distance only (no prefix bonus).
    Tie-breakers:
      1) larger common prefix
      2) higher fuzzy partial_ratio
      3) smaller length gap
    Returns a DataFrame: query, match, indel_distance, common_prefix, partial_ratio
    """
    # 1) All-vs-all Indel distance (fast & parallel)
    D = process.cdist(
        queries, choices,
        scorer=Indel.distance,   # ins=1, del=1, sub=2
        workers=-1,
        dtype=np.int16,
        processor=None
    )

    # 2) Rowwise best (handle ties manually)
    best_j = D.argmin(axis=1)
    rows = []

    for i, q in enumerate(queries):
        row = D[i]
        dmin = row.min()
        cands = np.where(row == dmin)[0]

        
        # tie-break with deterministic ranking
        tb = []
        for j in cands:
            b = choices[j]
            tb.append((
                j,
                common_prefix_len(q, b),          # higher is better
                fuzz.partial_ratio(q, b),         # higher is better
                -abs(len(q) - len(b))             # less gap is better
            ))
        # sort by: -common_prefix, -partial_ratio, +length_closeness
        tb.sort(key=lambda x: (-x[1], -x[2], -x[3]))
        #jbest = tb[0][0]
        #print(tb[0][0])
        for jbest in tb:
            jbest = jbest[0]
            print(jbest)


            b = choices[jbest[0]]

            rows.append({
                "query": q,
                "match": b,
                "indel_distance": int(D[i, jbest[0]]),
                "common_prefix": common_prefix_len(q, b),
                "partial_ratio": float(fuzz.partial_ratio(q, b))
            })

    return pd.DataFrame(rows)

out = best_matches_indel(names, names2
                         )
df_matches = best_matches_indel(names, names2)
print(df_matches)


#%%

import numpy as np
import pandas as pd
from rapidfuzz import process, fuzz
from rapidfuzz.distance import Indel



def prefix_ratio(s1: str, s2: str, score_cutoff=None) -> float:
    """
    Compara apenas os prefixos das strings s1 e s2, 
    limitando ao tamanho do menor termo.
    Retorna um score de 0 a 100.
    """
    # descobre o tamanho mínimo entre as duas strings
    min_len = min(len(s1), len(s2))
    
    # corta ambas no prefixo desse tamanho
    prefix1 = s1[:min_len]
    prefix2 = s2[:min_len]
    
    # calcula o ratio sobre os prefixos
    return fuzz.ratio(prefix1, prefix2, score_cutoff=score_cutoff)


def indel_prefix_distance(s1: str, s2: str, score_cutoff) -> float:
    """
    Similar ao prefix_ratio, mas usando Indel.distance.
    Retorna um score de 0 a 100.
    """
    min_len = min(len(s1), len(s2))
    prefix1 = s1[:min_len]
    prefix2 = s2[:min_len]
    
    # Indel.distance retorna a distância (0 = idêntico, maior é pior)
    dist = Indel.distance(prefix1, prefix2)
    
    
    return dist


def common_prefix_len(a: str, b: str) -> int:
    i, m = 0, min(len(a), len(b))
    while i < m and a[i] == b[i]:
        i += 1
    return i

def all_pairs_with_favorites(queries, choices):
    """
    Build a long-form table with scores for every (query, choice) pair,
    and mark the favorite (nearest neighbor) per query using:
      1) smallest Indel distance
      2) longest common prefix (tie-break)
      3) highest partial_ratio (tie-break)
      4) smallest absolute length gap (tie-break)
    """
    q = list(queries)
    c = list(choices)
    n, m = len(q), len(c)

    # 1) Scores (compute once)
    D = process.cdist(q, c, scorer=Indel.distance, workers=-1, dtype=np.int16, processor=None)
    #P = process.cdist(q, c, scorer=fuzz.partial_ratio, workers=-1, dtype=np.float32, processor=None)
    #R = process.cdist(q, c, scorer=prefix_ratio, workers=-1, dtype=np.float32, processor=None)
    I = process.cdist(q, c, scorer=indel_prefix_distance, workers=-1, dtype=np.float32, processor=None)


    # 2) Common-prefix matrix (cheap once)
    CPL = np.array([[common_prefix_len(a, b) for b in c] for a in q], dtype=np.int16)

    # 3) Length gap (broadcasted)
    #Lq = np.fromiter((len(s) for s in q), dtype=np.int32)
    #Lc = np.fromiter((len(s) for s in c), dtype=np.int32)
    #LG = np.abs(Lq[:, None] - Lc[None, :])



    # 4) Flatten into a long-form DataFrame
    df = pd.DataFrame({
        "query_idx":   np.repeat(np.arange(n), m),
        "choice_idx":  np.tile(np.arange(m), n),
        "query":       np.repeat(q, m),
        "choice":      np.tile(c, n),
        "indel_prefix_distance": I.ravel().astype(float),
        "indel_distance": D.ravel().astype(int),
        #"common_prefix":  CPL.ravel().astype(int),
        #"partial_ratio":  P.ravel().astype(float),
        #"length_gap":     LG.ravel().astype(int),
        #"prefix_ratio":   R.ravel().astype(float),

    })

    # 5) Mark favorites per query with deterministic tie-breakers
    #    Sort by: distance asc, common_prefix desc, partial_ratio desc, length_gap asc
    
    for c in ['common_prefix', 'partial_ratio', 'prefix_ratio']:
        if c in df.columns:
            df[c] = -df[c]


    df_sorted = df.sort_values(
        by=["query_idx", 
            #'prefix_ratio',
            #"indel_distance", 
          
            #"partial_ratio", 
            #"length_gap"
            'indel_prefix_distance',
            'indel_distance',
            #"common_prefix", 

            ],
        #ascending=[True, True, True, True, True],
        kind="mergesort"  # stable
    )
    best_rows = df_sorted.drop_duplicates(subset=["query_idx"], keep="first").index

    df["is_favorite"] = False
    df.loc[best_rows, "is_favorite"] = True

    # Clean helper cols
    df = df.sort_values(["query_idx", "choice_idx"]).reset_index(drop=True)
    return df

# ---------- Example ----------
names  = ['ANDREINA', 'ANDREIAESTETICAEBRONZ', 'AUTOCARRO1', 'AUTOCARCENTRO']
names2 = ['ANDREIA', 'ANDREIAESTETICAEB', 'AUTOCAR', 'AUTOCARRO2']

df_all = all_pairs_with_favorites(names, names2)

# If you want just the favorites:
favorites = df_all[df_all["is_favorite"]].copy()
print("\nFavorites:\n")
display(favorites[[x for x in ["query","choice","indel_distance","common_prefix","partial_ratio"] if x in favorites.columns]])
display(df_all)