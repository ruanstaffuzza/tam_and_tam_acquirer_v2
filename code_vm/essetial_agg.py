
import pandas as pd
import numpy as np

from IPython.display import display
#import networkx as nx

from collections import defaultdict

import time


def execute(func, df, display_flag=True, verbose=True, LEVEL_GROUP=None, use_level=False, **kwargs):
    """
    Executes a given function on the DataFrame, optionally displays the result, and shows execution time.

    Parameters:
    - func: The processing function to execute. It should accept a DataFrame and return a DataFrame.
    - df: The DataFrame to process.
    - display_flag: Boolean flag indicating whether to display the DataFrame after processing.
    - verbose: Boolean flag indicating whether to print execution details.
    - LEVEL_GROUP: Optional parameter to pass a level group to the function.
    - use_level: Boolean flag indicating whether to use the LEVEL_GROUP parameter.
    - **kwargs: Additional keyword arguments to pass to the function.

    Returns:
    - The processed DataFrame.
    """

    start_time = time.time()  # Start timing before function execution

    if verbose:
        print(f'Executing function {func.__name__}...')

    if use_level:
        df = func(df, LEVEL_GROUP=LEVEL_GROUP, **kwargs)
    else:
        df = func(df, **kwargs)

    end_time = time.time()  # End timing after function execution
    execution_time = end_time - start_time  # Calculate execution time

    if verbose:
        print(f'Function {func.__name__} executed successfully in {execution_time:.2f} seconds.')
    
    #if 'group_id' in df.columns: print(f'Number of groups: {df["group_id"].nunique()}')
    
    if display_flag:
        display(df)
    return df

def execute_with_context(LEVEL_GROUP, display_flag=False, verbose=False, **kwargs):
    import inspect
    def execute_context(func, df, **kwargs):
        # Check if 'LEVEL_GROUP' is in the function's parameters
        params = inspect.signature(func).parameters
        if 'LEVEL_GROUP' in params:
            # If 'LEVEL_GROUP' is a parameter, pass it along with 'df'
            result = execute(func, df, display_flag=display_flag, verbose=verbose, LEVEL_GROUP=LEVEL_GROUP,
                             use_level=True, **kwargs)
        else:
            # If 'LEVEL_GROUP' is not a parameter, call the function without it
            result = execute(func, df, display_flag=display_flag, verbose=verbose, use_level=False, **kwargs)
        return result
    return execute_context



def is_cpf_valido(cpf: str) -> bool:
    TAMANHO_CPF = 11
    if len(cpf) != TAMANHO_CPF:
        return False

    if cpf in (c * TAMANHO_CPF for c in "1234567890"):
        return False

    cpf_reverso = cpf[::-1]
    for i in range(2, 0, -1):
        cpf_enumerado = enumerate(cpf_reverso[i:], start=2)
        dv_calculado = sum(map(lambda x: int(x[1]) * x[0], cpf_enumerado)) * 10 % 11
        if cpf_reverso[i - 1:i] != str(dv_calculado % 10):
            return False

    return True


def fix_cnpjs_errados(data, doc_col='cpf_cnpj'):
    '''
    A master faz um padding 14 digitos para
    muitos CPFs. Precisamos corrigir isso.
    Além disso, durante o merge com documento raiz
    alguns desses casos foram identificados com TPV
    e tbm precisam ser corrigidos.
    Nota: O método de validação de CNPJs valida muitos CPFs
    como CNPJ. Por isso fazemos o processo inverso.
    '''
    data = data.copy()
    data['cpf_em_potencial'] = False
    data['cpf_em_potencial'] = np.where(
        (~data[doc_col].isna())\
            & (data[doc_col].str.startswith('000')) \
                & (data[doc_col].str.len()==14),
        True,
        False
    )
    data.loc[(data['cpf_em_potencial']),'cpf_valido'] = \
        data.loc[(data['cpf_em_potencial']),doc_col].apply(lambda x : is_cpf_valido(x[3:]))
    data['cpf_valido'] = data['cpf_valido'].fillna(False)
    
    data.loc[(data['cpf_valido']), doc_col] = \
        data.loc[(data['cpf_valido']), doc_col].apply(lambda x : x[3:])
    

    data.drop(
        columns=['cpf_em_potencial','cpf_valido'],
        inplace=True
    )

    
    return data


def prepare_data(df):
    df = df.drop(columns=['agrupamento_nome_1'])
    df = fix_cnpjs_errados(df, doc_col='merchant_tax_id')
    return df


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if self.parent.setdefault(x, x) != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, x, y):
        root_x = self.find(x)
        root_y = self.find(y)
        if root_x != root_y:
            self.parent[root_y] = root_x


def assign_group_ids_orig(df, id_cols, final_col):    
    uf = UnionFind()

    # Unir nós que compartilham os mesmos valores em id_cols
    for col in id_cols:
        value_to_index = defaultdict(list)
        for idx, value in df[col].items():
            value_to_index[value].append(idx)
        for indices in value_to_index.values():
            for i in range(1, len(indices)):
                uf.union(indices[i - 1], indices[i])

    # Criar um mapeamento de índice para group_id
    index_to_group = {idx: uf.find(idx) for idx in df.index}

    # Mapear as raízes dos grupos para IDs sequenciais
    root_to_group_id = {root: i + 1 for i, root in enumerate(set(index_to_group.values()))}
    df[final_col] = df.index.map(lambda idx: root_to_group_id[index_to_group[idx]])

    return df


def assign_group_ids(df, id_cols, final_col, LEVEL_GROUP):
    df = df.copy()
    
    #df = df.groupby(LEVEL_GROUP, group_keys=False, as_index=False).apply(lambda x: assign_group_ids_orig(x, id_cols, final_col))
    
    if len(id_cols)>1:
        df = assign_group_ids_orig(df, id_cols, final_col)
    else:
        df[final_col] = df.groupby(id_cols[0]).ngroup() + 1

    return df

def init_group_id(df, LEVEL_GROUP):

    QTD_NOME_COMUM = 20
    df = df.copy()

    nomes_comuns = df['nome_master'].value_counts()
    nomes_comuns = nomes_comuns[nomes_comuns>QTD_NOME_COMUM].index.to_list()

    df['group_id'] = df['resultado_names'].apply(lambda x: list(np.sort(x))[-1]) 
    df['group_id'] = df[['group_id', 'cod_muni']].astype(str).apply(lambda x: '|'.join(x), axis=1)

    df['new_id'] = np.arange(df.shape[0])
    df.loc[df['nome_master'].isin(nomes_comuns), 'group_id'] = 'Nome Comum: ' + df['new_id'].astype(str)

    df = df.rename(columns={'resultado_names': 'grouped_names'})
    df = df.drop(columns=['new_id'])

    df = assign_group_ids(df, ['group_id'], final_col='group_idx_nome', LEVEL_GROUP=LEVEL_GROUP)

    return df



def deal_unmerged_places(df, LEVEL_GROUP):
    df = df.copy()

    #df['mmhid_merge'] = df['merchant_market_hierarchy_id'].where((df['subs_asterisk'].isin(OUTROS)), None)
    df['mmhid_merge'] = df['merchant_market_hierarchy_id']

    df['id_merge_places'] = df['mmhid_merge'].apply(lambda x: str(x) if not pd.isna(x) else np.nan) #deal with nan in Int
    df['new_id'] = np.arange(df.shape[0])
    #df.loc[df['subs_asterisk']=='Ton', 'id_merge_places'] = 'Ton' + df['id_ton'].astype(str)
    df.loc[(df['subs_asterisk']=='Ton') and (df['id_merge_places'].isnull()), 'id_merge_places'] = 'Ton' + coalesce(df, ['cpf', 'cnpj'])
    df.loc[df['id_merge_places'].isnull(), 'id_merge_places'] = 'created' + df['new_id'].astype(str)


    if len(LEVEL_GROUP)>1:
        df['id_merge_places'] = df['id_merge_places'] + ' - ' + df[LEVEL_GROUP].astype(str).apply(lambda x: '|'.join(x), axis=1)#df['muni']
    elif len(LEVEL_GROUP)==1:
        df['id_merge_places'] = df['id_merge_places'] + ' - ' + df[LEVEL_GROUP[0]].astype(str)

    nome_grupo = 'group_idx_merge_places'

    df = assign_group_ids(df, ['group_idx_nome', 'id_merge_places'], final_col=nome_grupo, LEVEL_GROUP=LEVEL_GROUP)
    
    df_grouped = df.copy()
    # SPLIT group_id by | . Get first element
    #df_grouped['group_id'] = df_grouped['group_id'].str.split('|').apply(lambda x: x[0])

    # concat group_id with tam_id
    df_grouped = df_grouped[[nome_grupo, 'group_id']].drop_duplicates()
    df_grouped = df_grouped.groupby(nome_grupo, as_index=False)['group_id'].apply(lambda x: ', '.join(x))

    df = df.drop(columns=['group_id', 'new_id', 'id_ton'])
    df = df.merge(df_grouped, on=nome_grupo, how='left')

    #df = df.drop(columns=['tam_id'])
    return df



def deal_merged(df, LEVEL_GROUP,
                unmerge_col,
                pre_level, post_level):
    
    df_grouped = df.copy()

    
    df_grouped = df_grouped[df_grouped[unmerge_col].notnull()]
    df_grouped = df_grouped[LEVEL_GROUP + [pre_level, unmerge_col]].drop_duplicates()
    # Group by the specified columns and aggregate
    df_grouped = df_grouped.groupby(LEVEL_GROUP + [pre_level]).agg(
        unique_unmerge_col=(unmerge_col, 'first'),
        nunique_unmerge_col=(unmerge_col, 'nunique')
    ).reset_index()

    df = df.merge(df_grouped, on=LEVEL_GROUP + [pre_level], how='left')


    df[post_level] = df[pre_level].astype(str)
    
    df.loc[(df['nunique_unmerge_col']>1)
           #& (df['subs_asterisk'].isin(OUTROS))
           , post_level] = df[post_level].astype(str) + ' - ' + df[unmerge_col].astype(str)
    
    df.loc[(df['nunique_unmerge_col']>1)
           #& (~df['subs_asterisk'].isin(OUTROS))
           , post_level] = df[post_level].astype(str) + ' - ' + df['unique_unmerge_col'].astype(str)

    
    df = assign_group_ids(df, [post_level], final_col=post_level, LEVEL_GROUP=LEVEL_GROUP)

    df.loc[(df['nunique_unmerge_col']>1)
           #& (df['subs_asterisk'].isin(OUTROS))
           , 'group_id'] = df['group_id'].astype(str) + ' - ' + df[unmerge_col].astype(str)
    
    df.loc[(df['nunique_unmerge_col']>1)
           #& (~df['subs_asterisk'].isin(OUTROS))
           , 'group_id'] = df['group_id'].astype(str) + ' - ' + df['unique_unmerge_col'].astype(str)
    
    df = df.drop(columns=[unmerge_col, 'unique_unmerge_col', 'nunique_unmerge_col'])
    return df
    


def deal_merged_places(df, LEVEL_GROUP):

    df = df.copy()

    #df['mmhid_places'] = df.loc[df['subs_asterisk'].isin(OUTROS), 'merchant_market_hierarchy_id']
    df['mmhid_places'] = df['merchant_market_hierarchy_id']

    pre_level = 'group_idx_merge_places'
    post_level = 'group_idx_unmerge_places'
    unmerge_col = 'mmhid_places'

    df = deal_merged(df, LEVEL_GROUP, unmerge_col, pre_level, post_level)

    return df

def coalesce(df, columns):
    """
    Mimics the SQL COALESCE function in pandas. For each row, returns the first non-null value among the specified columns.
    
    Parameters:
    - df: pandas DataFrame.
    - columns: list of column names to consider for the coalesce operation.
    
    Returns:
    - Series: A pandas Series with the first non-null value found across the specified columns for each row.
    """
    return df[columns].bfill(axis=1).iloc[:, 0]



def deal_merged_docs(df, LEVEL_GROUP):
    df = df.copy()

    df['doc_unmerge'] = df['numero_inicio']
    mask = df['subs_asterisk'].isin(['Outros_Stone', 'Ton'])
    df.loc[mask, 'doc_unmerge'] = coalesce(df[mask], ['cnpj', 'cpf'])

    pre_level = 'group_idx_unmerge_places'
    post_level = 'group_idx_unmerge_docs'
    unmerge_col = 'doc_unmerge'

    df = deal_merged(df, LEVEL_GROUP, unmerge_col, pre_level, post_level)

    #df['tam_id'] = (df['group_idx_unmerge_docs'].astype(str) + df['cod_muni'].astype(str)).astype(int)
    return df


def create_tam_id_orig(df):
    df['tam_id'] = df.groupby(['group_idx_unmerge_docs']).ngroup()+1
    return df

def create_tam_id(df, LEVEL_GROUP):
    df = df.groupby(LEVEL_GROUP, group_keys=False
                    ).apply(lambda x: create_tam_id_orig(x))
    return df

def pos_tratamento(df, LEVEL_GROUP):
    df = df.copy()
    
    df = create_tam_id(df, LEVEL_GROUP)
    df['tam_id'] = (df['tam_id'].astype(str) + df['cod_muni'].astype(str)).astype(int)

    return df


def main_data_treat_muni(df, file_name):

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
    df = execute_ctx(deal_unmerged_places, df)
    df = execute_ctx(deal_merged_places, df)
    df = execute_ctx(deal_merged_docs, df)
    df = execute_ctx(pos_tratamento, df)
    print(f'Main data treat Execution time file {file_name}: {time.time() - start:.2f} seconds')
    return df
