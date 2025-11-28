#%%
import numpy as np
import pandas as pd
from rapidfuzz import process, fuzz
from rapidfuzz.distance import Indel, Levenshtein

from IPython.display import display

from utils import download_data, read_data

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
    dist = Indel.distance(prefix1, prefix2, score_cutoff=-score_cutoff)

    return -dist

def common_prefix_len(a: str, b: str, score_cutoff=None) -> int:
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
        "indel_distance": D.ravel().astype(int),
        #"common_prefix":  CPL.ravel().astype(int),
        #"partial_ratio":  P.ravel().astype(float),
        #"length_gap":     LG.ravel().astype(int),
        #"prefix_ratio":   R.ravel().astype(float),
        "indel_prefix_distance": I.ravel().astype(float),
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
    best_rows = df_sorted.drop_duplicates(subset=["query_idx"], keep="first")

    #df["is_favorite"] = False
    #df.loc[best_rows.index, "is_favorite"] = True

    # Clean helper cols
    #df = df.sort_values(["query_idx", "choice_idx"]).reset_index(drop=True)
    #return df
    return best_rows[['query', 'choice']].reset_index(drop=True)


# If you want just the favorites:
#favorites = df_all[df_all["is_favorite"]].copy()
#print("\nFavorites:\n")
#display(favorites[[x for x in ["query","choice","indel_distance","common_prefix","partial_ratio"] if x in favorites.columns]])
#display(df_all)


def update_sample_data():
    download_data('sample_final_nomes', update=True)


group_mp = ['MercadoLivre2', 'MercadoPago', 'MercadoPago_mmhid_problema',
       'MercadoPago_subPagarme']



#%%


cod_muni = 3541406
df = read_data('sample_final_nomes')
df = df[df['inicio'].str.strip()!='']
df = df[df['cod_muni']==cod_muni]
df = df.sort_values(['cod_muni', 'inicio', 'nome_merge', 'is_mp'])
df_orig = df.copy()

from rapidfuzz import fuzz, process
import pandas as pd
import numpy as np
import tqdm


def all_best_per_query(queries, choices, scorer, min_score=None):
    """
    Usa rapidfuzz.process.extract_iter para achar o melhor (e empates)
    por query sem materializar matriz nxm.
    """
    rowsres = []
    #row_no_match = []

    for q in queries:
        best = float("-inf") if min_score is None else min_score
        ties = []

        # extract_iter não aceita workers, só score_cutoff
        for c, s, _ in process.extract_iter(
            q, choices, scorer=scorer, score_cutoff=best
        ):
            if s > best:
                best = s
                ties = [(q, c)]
            elif s == best:
                ties.append((q, c))

        #if not ties:
            #row_no_match.append(q)
        #else:
        rowsres.extend(ties)

    #row_no_match = []

    #return pd.DataFrame(rowsres, columns=["query", "choice"]), row_no_match
    return rowsres


def indel_scorer(s1, s2, score_cutoff=None):
    dist = Indel.distance(s1, s2, score_cutoff=score_cutoff)
    return -dist



def merge_exato(querys_list, choices_list):
    
    rowsres = [q for q in querys_list if q in choices_list]
    queries_no_match1 = [q for q in querys_list if q not in choices_list]

    rowsres = [(q, q) for q in rowsres]

    return rowsres, queries_no_match1




def merge_complexo(querys_df, choices_df):


    querys_list = querys_df['nome_merge'].unique()
    choices_list = choices_df['nome_merge'].unique()

    res_eq, queries_no_match1 = merge_exato(querys_list, choices_list)


    if len(queries_no_match1) == 0:
        return res_eq, []

    
    res_pre_indel = all_best_per_query(queries_no_match1, choices_list, scorer=indel_prefix_distance, min_score=-2)

    if len(res_pre_indel) == 0:
        return res_eq, []
        
    if len(res_eq) == 0:
        return [], res_pre_indel
    else:
        return res_eq, res_pre_indel


def desempate_indel(df, querys_df, choices_df):
    init_cols = ['query', 'choice', 'indel_distance', 'inicio']

    df = df[init_cols].copy()

    #df['indel_distance'] = df[df['indel_distance'].isnull()].apply(lambda row: indel_scorer(row['query'], row['choice']), axis=1)

    df = df.merge(querys_df.rename(columns={'nome_merge':'query'}), on=['inicio', 'query'], how='left')

    df = df.merge(choices_df.rename(columns={'nome_merge':'choice'}), on=['inicio', 'choice'], how='left', suffixes=('_query', '_choice'))

    df['mp_merge_score'] = -(df['is_mp_query'] == df['is_mp_choice']).astype(int)

    df = df.sort_values([
        'indel_distance', 
        'mp_merge_score'])

    df = df.drop_duplicates(subset=['query', 'is_mp_query'], keep='first').reset_index(drop=True)

    final_cols = ['inicio', 'query', 'choice', 'is_mp_query', 'is_mp_choice', 'mmhid_choice']

    return df[final_cols]





def pool_exec_run():

    from concurrent.futures import ProcessPoolExecutor

    dfs = [dfg for _, dfg in df.groupby('inicio')]

    with ProcessPoolExecutor() as executor:
        results = list(tqdm.tqdm(
            executor.map(merge_complexo, dfs),
            total=len(dfs)
        ))

    df_final = pd.concat(results, ignore_index=True) #type: ignore
    return df_final


def thread_exec_run():
    from concurrent.futures import ThreadPoolExecutor

    dfs = [dfg for _, dfg in df.groupby('inicio')]

    with ThreadPoolExecutor() as executor:
        results = list(tqdm.tqdm(
            executor.map(merge_complexo, dfs),
            total=len(dfs)
        ))

    df_final = pd.concat(results, ignore_index=True) #type: ignore
    return df_final


def prep_data(df_orig):
        
    df = df_orig.copy()

    # === BLOCO OTIMIZADO (mantém df_no_match) ===
    m_q = df['mmhid_merge'].isnull()
    m_c = ~m_q

    # contagem por grupo via soma de booleanos (mais rápido que count())
    cnt_q = m_q.groupby(df['inicio']).sum()
    cnt_c = m_c.groupby(df['inicio']).sum()

    # mesmas colunas auxiliares (se você usa depois)
    qtd_queries = df['inicio'].map(cnt_q).fillna(0).astype(int)
    qtd_choices = df['inicio'].map(cnt_c).fillna(0).astype(int)

    # linhas a descartar: (query em grupo sem choices) OU (grupo sem queries)
    mask_no_match = (m_q & (qtd_choices == 0)) | (qtd_queries == 0)
    df_no_match = df[mask_no_match].copy()


    print(f'Eliminando {mask_no_match.sum()} linhas sem possibilidade de match...')
    df = df[~mask_no_match]
    print(f'Restam {df.shape[0]} linhas para processar...')

    dict_dataframes = {inicio: (dfg[dfg['mmhid_merge'].isnull()].drop(columns='mmhid_merge'),
                            dfg[dfg['mmhid_merge'].notnull()].rename(columns={'mmhid_merge':'mmhid_choice'})) for inicio, dfg in df.groupby('inicio')}
    
    return df, df_no_match, dict_dataframes




def post_treat(df):
   
    df.loc[df['mmhid_merge'].notnull(), 'choice'] = np.nan

    df['idx_group'] = df['mmhid_merge'].fillna(df['mmhid_choice']).astype("string").fillna(df['inicio'])

    return df


def normal_run(dict_dataframes):

    def custom_merge(q_df, c_df):
        return merge_complexo(q_df, c_df)
    
    return [[custom_merge(q_df, c_df), inicio] for inicio, (q_df, c_df) in dict_dataframes.items()]


def final_list_rows_to_df(final_list):
    # Preallocate lists for each column to avoid DataFrame concatenation overhead
    queries, choices, inicios, indel_distance = [], [], [], []
    for item in final_list:
        inicio = item[1]
        list_eq, list_pre_indel = item[0]
        for q, c in list_eq:
            queries.append(q)
            choices.append(c)
            inicios.append(inicio)
            indel_distance.append(0)

        for q, c in list_pre_indel:
            queries.append(q)
            choices.append(c)
            inicios.append(inicio)
            indel_distance.append(np.nan)
    return pd.DataFrame({'query': queries, 'choice': choices, 'inicio': inicios, 'indel_distance': indel_distance})

def new_agrupamento_tam_v2(df_orig, cod_muni):
    df, df_no_match, dict_dataframes = prep_data(df_orig)   

    final_list = normal_run(dict_dataframes)

    dfs = final_list_rows_to_df(final_list)

    dfs['indel_distance'] = dfs['indel_distance'].fillna(dfs.apply(lambda row: indel_scorer(row['query'], row['choice']), axis=1))

    querys_df = df[df['mmhid_merge'].isnull()].drop(columns='mmhid_merge')
    choices_df = df[df['mmhid_merge'].notnull()].rename(columns={'mmhid_merge':'mmhid_choice'})

    df_final = desempate_indel(dfs, querys_df, choices_df)

    df_final = df.merge(df_final[['inicio', 'query', 'is_mp_query', 'mmhid_choice', 'choice']].rename(columns={
        'query':'nome_merge',
        'is_mp_query': 'is_mp'
        }), on=['inicio', 'is_mp', 'nome_merge'], how='left')

    df_final = pd.concat([
        df_final, df_no_match
        ], ignore_index=True)

    df_final = post_treat(df_final)

    df_final['nome_master'] = df_final['inicio'] + df_final['nome_merge']

    df_final['cod_muni'] = cod_muni

    return df_final


df_final = new_agrupamento_tam_v2(df_orig, cod_muni)


df_final.to_parquet(f'teste_agrupamento_novo_{cod_muni}.parquet', index=False)
#%%

