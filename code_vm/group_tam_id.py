
import pandas as pd
import numpy as np

from IPython.display import display
import networkx as nx

from collections import defaultdict

import time


#OUTROS = ['Outros', 'Outros_Pags', 'Outros_SumUp', 'Outros_Stone']


# Custom aggregation function to join unique, non-null values
def join_unique(series):
    return list(series.dropna().unique())

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


def draw_munis():
        
    df = read_data('pop_muni') # type: ignore
    df.populacao_residente.describe()

    draw1 = pd.concat([
        df.query('populacao_residente<=5000').sample(2),
        df.query('5000<populacao_residente<=10000').sample(2),
        df.query('10000<populacao_residente<=25000').sample(2),
        df.query('25000<populacao_residente').sample(2)
    ])['id_municipio'].to_list()


    from numpy.random import choice
    draw = choice(df['id_municipio'], 12,
                p=df['populacao_residente']/df['populacao_residente'].sum())

    df_sample = df[df['id_municipio'].isin(list(draw)+draw1)]

    df_sample.sort_values(by='populacao_residente', ascending=False)

    df_sample['id_municipio'].astype(int).to_list()


def download_data():
    from utils import download_data
        
    download_data('nomes_unicidade', 
                #update=True
                )


    download_data('get_ton_unicidade', 
                #update=True
                )


def open_ton(read_data_func, LEVEL_GROUP):
    read_data= read_data_func

    df = read_data('get_ton_unicidade')

    df = df.rename(columns={'document': 'document_ton'})

    df['id_ton'] = np.arange(len(df))

    df = pd.concat([
        df.assign(nome_ton=lambda x: x['legal_name']).drop(columns=['legal_name', 'trade_name']),    
        df.assign(nome_ton=lambda x: x['trade_name']).drop(columns=['legal_name', 'trade_name']),
    ]).reset_index(drop=True)

    df = df[df['nome_ton'].notnull()]
    

    df['nome_master'] = df['nome_ton'].str.replace(r'[^A-Z]', '', regex=True)
    df = df.rename(columns = {'nome_ton': 'nome_master_sem_espaco'})

    df = df[df['nome_master'].str.len()>=5]
    df = df.drop_duplicates([x for x in df.columns if x not in ['nome_master_sem_espaco']])

    df['qtd_documents'] = df.groupby(LEVEL_GROUP + ['nome_master'])['document_ton'].transform('nunique')

    df.loc[df['qtd_documents']>1, 'nome_master'] = df['nome_master'] + df['document_ton']
    
    df.loc[df['document_ton'].str.len()==14, 'cnpj'] = df['document_ton']
    df.loc[df['document_ton'].str.len()==11, 'cpf'] = df['document_ton']

    df = df.drop(columns=['qtd_documents'])

    df['subs_asterisk'] = 'Ton'
    return df



def unsused_functions(OUTROS):
    #OUTROS = ['Outros', 'Outros_Pags', 'Outros_SumUp', 'Outros_Stone']
    

    def open_init_data(df, ton, LEVEL_GROUP):

        df = df.copy()
        ton = ton.copy()

        ## Column Name                   Data Type                     
        # merchant_market_hierarchy_id  Int64                         
        # subs_asterisk                 object                        
        # nome_master                   object                        
        # qtd_cpfs                      Int64                         
        # qtd_cnpjs                     Int64                         
        # nome_muni                     object                        
        # uf                            object
        # city_places                   object                        
        # city_places_cleansed          object                        
        # state                         object                        
        # state_cleansed                object      

        #df = df.drop_duplicates([x for x in df.columns if x not in ['merchant_market_hierarchy_id']])

        print('Apenas mmhid de Outro')
        df['merchant_market_hierarchy_id'] = df['merchant_market_hierarchy_id'].where(df['subs_asterisk'].isin(OUTROS), None)

        df = df.drop_duplicates(['subs_asterisk', 'nome_master', 'merchant_market_hierarchy_id'] + LEVEL_GROUP)

        #df = df[df['subs_asterisk']!='Outros']

        df['nome_master'] = df['nome_master'].str.replace(r'[^A-Z]', '', regex=True)


        df = pd.concat([
            df,
            ton,
        ])

        df['inicio'] = df['nome_master'].str[:6]

        #display(df[['nome_muni', 'uf']].value_counts())

        #display(df['subs_asterisk'].value_counts())

        #if LEVEL_GROUP:
            #display(df[LEVEL_GROUP].value_counts())


        #else:         print('Sem LEVEL_GROUP')
        #display(df[LEVEL_GROUP + ['inicio']].value_counts().head(5))

        return df

    


    def test_multiple_names():    
        df = open_init_data()

        df['inicio'] = df['nome_master'].str[:6]

        df['merchant_market_hierarchy_id'] = df['merchant_market_hierarchy_id'].where(df['subs_asterisk'].isin(OUTROS), None)


        df['nome'] = df['nome_master'] + ' ' + df['nome_muni'] + ' ' + df['uf']
        df['qtd_nome'] = df.groupby('merchant_market_hierarchy_id')['nome'].transform('nunique')



        df = df[df['qtd_nome']>5]
        df = df.sort_values(['qtd_nome', 'merchant_market_hierarchy_id', 'nome']).reset_index(drop=True)

        df = test_multiple_names()
        #df = df[df['qtd_nome']<50]

        # row_number() over (partition by merchant_market_hierarchy_id order by rand()) as row_number

        #sort by random
        #df = df.sample(frac=1)

        df = df.groupby(['merchant_market_hierarchy_id', 'qtd_nome'], as_index=False).agg({
            'nome_master': join_unique,
            'nome_muni': join_unique,
            'subs_asterisk': join_unique,
        })

        df['nome_master'] = df['nome_master'].apply(lambda x: '/ '.join(x))
        df.to_csv('multiple_names.csv', index=False)


        return df


    def deal_with_big_mmhid(df, LEVEL_GROUP):
        df = df.copy()

        big_mmhid = df[['merchant_market_hierarchy_id', 'nome_master'] + LEVEL_GROUP].drop_duplicates()['merchant_market_hierarchy_id'].value_counts()
        big_mmhid = big_mmhid[big_mmhid>10].index.to_list()

        df['merchant_tax_id'] = df['merchant_tax_id'].replace({
            '10573521000191': None,  # MP
            '': None,
            '16668076000120': None, # sumup
            '08561701000101': None, # PagSeguro
            '14380200000121': None, # Ifood
            '18727053000174': None, # Pagarme
            '03816413000137': None, # Pagueveloz
            '06308851000182': None, # MOOZ SOLUCOES FINANCEIRAS LTDA
            })

        df['merchant_tax_id'] = df['merchant_tax_id'].where(
            (df['subs_asterisk']!='CloudWalk')
            & (~df['merchant_market_hierarchy_id'].isin(big_mmhid))
            , None
        )

        df['mmhid_merge'] = df['merchant_market_hierarchy_id'].where(
            (df['subs_asterisk'].isin(OUTROS))
            & (~df['merchant_market_hierarchy_id'].isin(big_mmhid)), None)

        df['merchant_market_hierarchy_id'] = df['merchant_market_hierarchy_id'].where(
            (df['subs_asterisk'].isin(OUTROS)), None)

        return df




    def grouped_subs_asterisk(df, grouping_id=['agrupamento_inspecao']):
        df = df.copy()
        dict_dummies = {
            'MercadoPago_subPagarme': 'mp_subs',
            'Ton': 'ton',
            'MercadoPago': 'mp',
            'PagSeguro': 'pagseguro',
            'SumUp': 'sumup',
            'Outros_Stone': 'stone',
            }
        dict_dummies.update({x: 'outros' for x in OUTROS})

        df_grouped = df[grouping_id +['subs_asterisk']].copy().drop_duplicates()
        df_grouped['subs_asterisk'] = df_grouped['subs_asterisk'].map(dict_dummies)


        # create dummies form subs_asterisk in ['MercadoPago_subPagarme', 'Ton', 'Outros']
        df_grouped = pd.get_dummies(df_grouped, columns=['subs_asterisk'], 
                                    prefix='has', prefix_sep='_')
        missing_cols = set([f'has_{x}' for x in dict_dummies.values()]) - set(df_grouped.columns)
        if missing_cols:
            for col in missing_cols:
                df_grouped[col] = 0

        has_cols = [x for x in df_grouped.columns if 'has_' in x]

        df_grouped['has_not_ton'] = df_grouped[[x for x in has_cols if x!='has_ton']].sum(axis=1)>=1

        df_grouped = df_grouped.groupby(grouping_id).sum()>=1
        df_grouped = df_grouped.reset_index()


        df = df.merge(df_grouped, on=grouping_id, how='left')

        return df


def group_and_clean(df, LEVEL_GROUP):
    df = df.copy()
    from test_grouping import group_names_df

    print('Tamanho do DataFrame:', df.shape)

    #df['qtd_por_inicio'] = df.groupby(['nome_muni', 'uf'])['nome_master'].transform('count')
    #df = df[df['qtd_por_inicio']>1]

    df = df.sort_values(by=LEVEL_GROUP + ['nome_master']).reset_index(drop=True)

    df = group_names_df(df, group_cols=[
                                        'inicio',
                                        ] + LEVEL_GROUP, 
                        nome_col='nome_master',
                        delete_aux_cols=False,
                        verbose=False)
    
    if LEVEL_GROUP:
        df['agrupamento_nome_1'] = df['res_aux'].apply(lambda x: ', '.join(x)) + df[LEVEL_GROUP].astype(str).apply(lambda x: '|'.join(x), axis=1)
    else:
        df['agrupamento_nome_1'] = df['res_aux'].apply(lambda x: ', '.join(x))
    
    df = df.drop(columns=['resultado_prefixes', 'res_aux', 'retirado_de_resultado'])
    
    return df


"""
def create_agg_nomes_agrupados():
    # Exemplo de uso
    directory = 'data/nomes_agrupados'
    df = generate_parquet_summary(directory)
    df = df.sort_values('line_count')
    df['id'] = calculate_group_ids(df['line_count'])

    import json
    dict_files = df.groupby('id')['file_name'].apply(lambda x: list(x)).to_dict()
    return dict_files
"""
import pandas as pd
from itertools import cycle
import numpy as np

def is_cnpj_valido(cnpj: str) -> bool:
    LENGTH_CNPJ = 14
    if len(cnpj) != LENGTH_CNPJ:
        return False

    if cnpj in (c * LENGTH_CNPJ for c in "1234567890"):
        return False

    cnpj_r = cnpj[::-1]
    for i in range(2, 0, -1):
        cnpj_enum = zip(cycle(range(2, 10)), cnpj_r[i:])
        dv = sum(map(lambda x: int(x[1]) * x[0], cnpj_enum)) * 10 % 11
        if cnpj_r[i - 1:i] != str(dv % 10):
            return False

    return True


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
    df.loc[df['subs_asterisk']=='Ton', 'id_merge_places'] = 'Ton' + df['id_ton'].astype(str)
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
    
    df = df.copy()
    gcols = LEVEL_GROUP + [pre_level]

    # Per-group stats
    grp = df.groupby(gcols, dropna=False)[unmerge_col]
    df['nunique_unmerge_col'] = grp.transform(lambda s: s.dropna().nunique())
    df['unique_unmerge_col'] = grp.transform(lambda s: s.dropna().iloc[0] if s.dropna().size else np.nan)

    multi_mask = df['nunique_unmerge_col'] > 1
    
    # Base post_level from pre_level
    df[post_level] = df[pre_level].astype(str)

    # Append both unmerge_col and unique_unmerge_col in one go
    df.loc[multi_mask, post_level] = (
        df.loc[multi_mask, post_level]
        + ' - ' + df.loc[multi_mask, unmerge_col].astype(str)
        + ' - ' + df.loc[multi_mask, 'unique_unmerge_col'].astype(str)
    )

    # Assign group ids based on post_level
    df = assign_group_ids(df, [post_level], final_col=post_level, LEVEL_GROUP=LEVEL_GROUP)

    # Same combined append for group_id
    df.loc[multi_mask, 'group_id'] = (
        df.loc[multi_mask, 'group_id'].astype(str)
        + ' - ' + df.loc[multi_mask, unmerge_col].astype(str)
        + ' - ' + df.loc[multi_mask, 'unique_unmerge_col'].astype(str)
    )

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


def create_tam_id_orig(df):
    df['tam_id'] = df.groupby(['group_idx_unmerge_docs']).ngroup()+1
    return df

def create_tam_id(df, LEVEL_GROUP):
    df = df.groupby(LEVEL_GROUP, group_keys=False
                    ).apply(lambda x: create_tam_id_orig(x))
    return df

def prepare_data(df):
    df = df.drop(columns=['agrupamento_nome_1'])
    df = fix_cnpjs_errados(df, doc_col='merchant_tax_id')
    return df

def pos_tratamento(df, LEVEL_GROUP):
    df = df.copy()
    
    df = create_tam_id(df, LEVEL_GROUP)
    df['tam_id'] = (df['tam_id'].astype(str) + df['cod_muni'].astype(str)).astype(int)

    return df


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



def choose_prefered_document(df):
    # Dá preferência para o tax_id da places (depois do agrupamento), depois para o tax_id de 'MP e Ton por fora', Depois o cnpj do algorítimo e por último cpf do algorítmo.

    df = df.copy()
    

    df.loc[df['numero_inicio'].notnull(), 'cnpj_contains_numero_inicio'] = df.loc[df['numero_inicio'].notnull()].apply(lambda row: str(row['cnpj']).startswith(str(row['numero_inicio'])) if row['cnpj'] is not None and row['numero_inicio'] is not None else False, axis=1)
    df['cnpj_contains_numero_inicio'] = df['cnpj_contains_numero_inicio'].fillna(False)

    df.loc[df['cnpj_contains_numero_inicio'], 'numero_inicio'] = df['cnpj']

    df = df.drop(columns=['cnpj_contains_numero_inicio'])

    df['cnpj'] = coalesce(df, ['numero_inicio', 'cnpj'])

    df['len_resultado'] = df.groupby('tam_id')['nome_master'].transform('count')

    df['document'] = coalesce(df, ['merchant_tax_id', 'numero_inicio', 'cnpj', 'cpf', 'cpf_brasil'])

    df['source_document'] = 'Busca'

    df.loc[df['merchant_tax_id'].notnull(), 'source_document'] = 'Places'

    df.loc[df['subs_asterisk'].isin(['Ton', 'MercadoPago_subPagarme', 'Outros_Stone']), 'source_document'] = df['subs_asterisk']


    order_importance_docu = {
        'Outros_Stone': 0,    
        'Ton': 1,
        'Places': 2,   
        'MercadoPago_subPagarme': 3,
        'Busca': 4
    }

    # Step 2: Identify the preferred document for each group
    # First, sort the DataFrame by 'order_docu' to ensure the row with the minimum 'order_docu' is first within each group
    df_sorted = df[(df['document'].notnull())
                & (df['len_resultado']>1)
                ][['tam_id', 'document', 'source_document']].copy()
    
    df_sorted['order_docu'] = df_sorted['source_document'].map(order_importance_docu)
    
    df_sorted = df_sorted.sort_values(by=['tam_id', 'order_docu'])

    # Then, drop duplicates to keep the first occurrence (minimum 'order_docu') within each group
    df_preferred = df_sorted.drop_duplicates(subset=['tam_id'], keep='first')

    df_preferred = df_preferred.rename(columns={'document': 'documento_final'})
    df_preferred = df_preferred.drop(columns=['order_docu'])


    df = df.merge(df_preferred, on=['tam_id'], how='left',
                    suffixes=('', '_final'))

    df['source_document'] = coalesce(df, ['source_document_final', 'source_document'])
    df['documento_final'] = coalesce(df, ['documento_final', 'document'])

    df = df.drop(columns=['source_document_final'])

    return df


#%load_ext line_profiler
#%lprun -f choose_prefered_document choose_prefered_document(df)

def create_agrupamento_inspecao(df):
    df = df.copy()

    df['document_mod'] = np.arange(df.shape[0])
    df['document_mod'] = 'fakedoc' + df['document_mod'].astype(str)

    df.loc[df['documento_final'].notnull(), 'document_mod'] = df['documento_final']

    df = assign_group_ids(df, ['agrupamento_nome_1', 'tam_id', 'document_mod'], final_col='agrupamento_inspecao')

    df = df.drop(columns=['document_mod'])
    df['qtd_distinct_agrupamento_nome_1'] = df.groupby('agrupamento_inspecao')['agrupamento_nome_1'].transform('nunique')

    df['len_inspecao'] = df.groupby('agrupamento_inspecao')['nome_master'].transform('nunique')

    return df


def final_ajustes(df):
    df = df.copy()
    
    df = df[~((df['subs_asterisk']=='MercadoPago_subPagarme') & (df['len_resultado']==1))]

    df.loc[df['documento_final'].isnull(), 'source_document'] = 'Sem Documento'

    #df = df.sort_values(by=['nome_muni', 'uf', 'group_id', 'nome_master', 'subs_asterisk', 'len_resultado']).reset_index(drop=True)
    
    order_cols = [
    'group_id', 'tam_id', 'nome_muni', 'uf', 'merchant_market_hierarchy_id', 'nome_master', 'subs_asterisk', 
    'documento_final', 'source_document',
    'merchant_tax_id', 'numero_inicio', 'cnpj', 'cpf', 'cpf_brasil',
    'grouped_names']
    
    for col in order_cols:
        if col not in df.columns:
            print('Coluna não encontrada:', col)

    df = df[order_cols + [x for x in df.columns if x not in order_cols]]
    return df


#%load_ext line_profiler
#%lprun -f choose_prefered_document choose_prefered_document(dfg)


def create_doc_final(df, LEVEL_GROUP, save_excel_inspecao=False):
    df = df.copy()

    df['qtd_mmhid_tax_id'] = df.groupby('merchant_tax_id')['merchant_market_hierarchy_id'].transform('nunique')
    df['qtd_mmhid_tax_id'] = df.groupby('group_id')['qtd_mmhid_tax_id'].transform('max')


    df = assign_group_ids(df, ['merchant_tax_id', 'group_id'], final_col='grouped_tax_id', LEVEL_GROUP=LEVEL_GROUP)

    # 1 way
    df['muni'] = df['nome_muni'] + df['uf']
    df['qtd_muni_group'] = df.groupby('group_id')['muni'].transform('nunique')

    #df = df.drop(columns=['muni'])

    df['qtd_nomes'] = df.groupby('group_id')['nome_master'].transform('nunique')

    def sample_by_cols(df, cols, n=100):

        groups = df[cols].drop_duplicates()
        max_n = groups.shape[0]
        groups = groups.sample(min(n, max_n))

        return df.merge(groups, on=cols, how='inner')


    def delete_filter_cols(df, drop_has=True):
        df = df.copy()
        columns_has = [x for x in df.columns if 'has_' in x]
        df = df.drop(columns=['inicio', 'len_resultado', 
                            #'nome_ton',  
                            'qtd_mmhid_tax_id',
                            'grouped_tax_id',
                            'agrupamento_nome_1'])
        if drop_has:
            df = df.drop(columns=columns_has)
        return df


    def excel_ajustes(df, sample_by=['agrupamento_inspecao'], sort_by=['agrupamento_inspecao', 'nome_muni', 'uf','group_id', 'nome_master', 'subs_asterisk']):
        df = df.copy()
        df = df.sort_values(by=sort_by).reset_index(drop=True)
        df = sample_by_cols(df, sample_by, n=100)
        df = delete_filter_cols(df)
        df['grouped_names'] = df['grouped_names'].apply(lambda x: '[' + ', '.join(x) + ']')
        df = df.rename(columns={'merchant_market_hierarchy_id': 'mmhid',
                                'qtd_distinct_agrupamento_nome_1': 'qtd_aggname'})
        df = df.drop_duplicates()
        
        return df
    
    def final_file_ajustes(df):
        sort_by = [
            'grouped_tax_id', 
            'nome_muni', 'uf', 'group_id', 'nome_master', 'subs_asterisk']
        df = df.sort_values(by=sort_by).reset_index(drop=True)
        df = delete_filter_cols(df, drop_has=False)
        extra_cols_del = ['qtd_distinct_agrupamento_nome_1', 'agrupamento_inspecao']
        df = df.drop(columns=extra_cols_del, errors='ignore')
        # drop duplicates all except list types series
        df['grouped_names'] = df['grouped_names'].apply(lambda x: ', '.join(x) )
        df = df.drop_duplicates()
        print(df.columns)
        return df



    def ajust_colum_width(writer, df, sheet_name):

        for column in df:
            column_length = max(df[column].astype(str).map(len).max(), len(column))
            col_idx = df.columns.get_loc(column)
            if column not in ['group_id', 'grouped_names']:
                writer.sheets[sheet_name].set_column(col_idx, col_idx, column_length + 1)
            else:
                writer.sheets[sheet_name].set_column(col_idx, col_idx, 20)



    def save_dataframes_to_excel(dataframes_list, filename):
        """
        Save multiple dataframes to an Excel file with specified sheet names.
        
        Parameters:
        dataframes_list (list of dict): List of dictionaries where each dictionary contains 'nome_aba' and 'dataframe'.
        filename (str): The name of the Excel file to be created.
        """
        # Create a Pandas Excel writer using XlsxWriter as the engine
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            for item in dataframes_list:
                sheet_name = item['nome_aba']
                df = item['dataframe']
                # Write the dataframe to the specified sheet name
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                ajust_colum_width(writer, df, sheet_name)

    # Exemplo de uso:
    # dataframes_list = [
    #     {'nome_aba': 'dataframe1', 'dataframe': df1},
    #     {'nome_aba': 'dataframe2', 'dataframe': df2}
    # ]
    # save_dataframes_to_excel(dataframes_list, 'output.xlsx')


    if save_excel_inspecao:
        dataframes_list = [
            {'nome_aba': 'Com Ton', 'dataframe': excel_ajustes(df[(df['len_inspecao']>1) & (df['has_ton']) & (df['has_not_ton'])])},
            {'nome_aba': 'Com MP adcionado', 'dataframe': excel_ajustes(df[(df['len_inspecao']>1) & (df['has_mp_subs'])])},
            {'nome_aba': 'Com Pags', 'dataframe': excel_ajustes(df[(df['len_inspecao']>1) & (df['has_pagseguro'])])},
            {'nome_aba': 'Com Outros', 'dataframe': excel_ajustes(df[(df['len_inspecao']>1) & (df['has_outros'])])},
            {'nome_aba': 'Todos', 'dataframe': excel_ajustes(df[(df['len_inspecao']>1) ])}, 
            {'nome_aba': 'mais de um agrupamento', 'dataframe': excel_ajustes(df[df['qtd_distinct_agrupamento_nome_1']>1])},
            {'nome_aba': 'Sem agrupamento', 'dataframe': excel_ajustes(df[df['len_inspecao']==1])},
            {'nome_aba': 'Mais de um mmhid', 'dataframe': excel_ajustes(df[df['qtd_mmhid_tax_id']>1],
                                                                        sort_by=(
                                                                            ['grouped_tax_id']
                                                                            + LEVEL_GROUP + ['merchant_tax_id','group_id', 'nome_master', 'subs_asterisk']),
                                                                        sample_by=['grouped_tax_id'])},
            {'nome_aba': 'Com mais de um muni', 'dataframe': excel_ajustes(df[df['qtd_muni_group']>1])},
            {'nome_aba': 'Com muitos nomes', 'dataframe': excel_ajustes(df[df['qtd_nomes']>20])},
        ]

        save_dataframes_to_excel(dataframes_list, 'conferencia_nomes_unicidade.xlsx')
    else: 
        return final_file_ajustes(df)

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


import pandas as pd
from google.cloud import bigquery
import datetime



def upload_partitioned_table_to_bigquery(df, destination_table):
    """
    Uploads a DataFrame to BigQuery with partitioning on 'ingestion_date' column.
    If the table exists, it appends the data. If not, it creates the table.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        destination_table (str): Destination table in the format 'project.dataset.table'.

    Returns:
        None
    """
    # Add 'ingestion_date' column
    df['ingestion_date'] = datetime.datetime.now()

    # Create BigQuery client
    client = bigquery.Client()

    # Define job configuration
    job_config = bigquery.LoadJobConfig(
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingestion_date"
        ),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append data if table exists, otherwise create table
    )

    # Load DataFrame to BigQuery
    job = client.load_table_from_dataframe(df, destination_table, job_config=job_config)

    # Wait for the job to complete
    job.result()
    print("Data successfully loaded into BigQuery")




def load_available_data(directory, destination_table):
    files = os.listdir(directory)
    for file in files:
        print(f'Opening {file}...')
        df = pd.read_parquet(f'{directory}/{file}')
        print(f'Sending {file} to {destination_table}')
        upload_partitioned_table_to_bigquery(df, destination_table)
        print(f'{file} sent to {destination_table} successfully!')

import os
import pandas as pd
import json

def load_available_data(directory, destination_table, track_file='uploaded_files.json'):
    """
    Loads data from parquet files in the given directory and uploads them to a BigQuery table.
    Tracks uploaded files to avoid re-uploading.

    Args:
        directory (str): The directory containing parquet files.
        destination_table (str): The BigQuery table to upload data to.
        track_file (str): The path to the JSON file used to track uploaded files.

    Returns:
        None
    """
    # Load the list of uploaded files
    if os.path.exists(track_file):
        with open(track_file, 'r') as f:
            uploaded_files = json.load(f)
    else:
        uploaded_files = []

    # Get list of files in the directory
    files = os.listdir(directory)

    for file in files:
        if file not in uploaded_files:
            print(f'Opening {file}...')
            df = pd.read_parquet(os.path.join(directory, file))
            
            print(f'Sending {file} to {destination_table}')
            upload_partitioned_table_to_bigquery(df, destination_table)
            
            print(f'{file} sent to {destination_table} successfully!')

            # Update the list of uploaded files and save it
            uploaded_files.append(file)
            with open(track_file, 'w') as f:
                json.dump(uploaded_files, f)
        else:
            print(f'{file} has already been uploaded. Skipping.')



def load_chunk_gbq(chunk, i, destination_table):
    start =  time.time()
    chunk.to_gbq(destination_table=destination_table, project_id='dataplatform-prd', if_exists='append')
    print(f'Time to gbq {i}: ', time.time() - start)


def load_to_gbq(df, destination_table, chunk_size=10000):
  init_start =  time.time()
  for i, chunk in enumerate(np.array_split(df, np.ceil(len(df) / chunk_size))):
      load_chunk_gbq(chunk, i, destination_table=destination_table)
  print(f'Total time: ', time.time() - init_start)



def split_parquet_by_size(file_name, output_dir, min_rows_per_file=3000000):
    from utils import read_data
    df_orig = read_data(file_name)
    unique_cod_muni = df_orig['cod_muni'].unique()
    
    current_file_index = 0
    current_df = pd.DataFrame()

    for cod_muni in unique_cod_muni:
        df_subset = df_orig[df_orig['cod_muni'] == cod_muni]
        if len(current_df) + len(df_subset) >= min_rows_per_file:
            current_df.to_parquet(f"{output_dir}/part_{current_file_index}.parquet")
            current_file_index += 1
            current_df = df_subset
        else:
            current_df = pd.concat([current_df, df_subset], ignore_index=True)
    
        if not current_df.empty:
            current_df.to_parquet(f"{output_dir}/part_{current_file_index}.parquet")


#split_parquet_by_size('final_nomes', 'data')

# find all 'part_*.parquet' files in 'data' directory
#import os
#files = [f for f in os.listdir('data') if f.startswith('part_') and f.endswith('.parquet')]





def value_counts_with_total(series):
    # Calcula o value_counts
    counts = series.value_counts()
    
    # Adiciona a linha 'total'
    counts['Total'] = counts.sum()
    
    #value_counts_with_total(df[['group_id', 'source_document']].drop_duplicates()['source_document'])

    return counts

def testes():
    LEVEL_GROUP = ['nome_muni', 'uf']
        
    def study_add_mp(df):
        df = df.copy()

        df['qtd_distinct_subs'] = df.groupby(LEVEL_GROUP + ['group_id'])['subs_asterisk'].transform('nunique')

        df['actual_mp'] = (df['subs_asterisk']=="MercadoPago")
        df['actual_mp'] = df.groupby(LEVEL_GROUP + ['group_id'])['actual_mp'].transform('any')

        df['added_mp'] = (df['subs_asterisk']=="MercadoPago_subPagarme") & (df['cnpj'].notnull())
        df['added_mp'] = df.groupby(LEVEL_GROUP + ['group_id'])['added_mp'].transform('any')



        df = df[(df['added_mp']) & (df['qtd_distinct_subs']>1)]


        df = df.drop(columns=[
        'retirado_de_resultado',
        'resultado_names', 
        'len_resultado', 
        'qtd_names_mmhid', 
        'cnpj_raiz', 'tax_id_raiz', 'added_mp'])

        return df
    df_mp = study_add_mp(dfg)









    ##################
    display(dfg[
        
        (dfg['subs_asterisk']=='Outros')
        & (dfg['merchant_tax_id'].isnull())
        ].assign(has_cpf = lambda x: x['cpf'].notnull())['has_cpf'].agg(['count', 'sum']))

    ###################





    # Group the DataFrame and then aggregate 'cpf' and 'cnpj' using the custom function
    dfg2 = dfg.groupby(LEVEL_GROUP +['group_id', 'len_resultado'], as_index=False).agg({
        'cpf': join_unique,
        'cnpj': join_unique,
        'merchant_tax_id': join_unique,
        'cnpj_raiz': join_unique,
        'tax_id_raiz': join_unique,
    })


    dfg2 = dfg[LEVEL_GROUP + ['merchant_market_hierarchy_id', 'nome_master', 'subs_asterisk', 'group_id', 'len_resultado']].drop_duplicates().merge(dfg2, on=LEVEL_GROUP +['group_id', 'len_resultado'], how='inner')



    dfg2 = dfg2.explode('cpf')
    ############################




    # Group the DataFrame and then aggregate 'cpf' and 'cnpj' using the custom function
    dfg = dfg.groupby(LEVEL_GROUP + ['group_id'] + ['len_resultado'], as_index=False).agg({
        'cpf': join_unique,
        'cnpj': join_unique,
        'merchant_tax_id': join_unique,
        'cnpj_raiz': join_unique,
        'tax_id_raiz': join_unique,
        'merchant_market_hierarchy_id': join_unique,
    })

    # Calculate the length of 'cpf' and 'cnpj' lists after splitting by ', '
    dfg['len_cpfs'] = dfg['cpf'].apply(len)
    dfg['len_cnpjs'] = dfg['cnpj'].apply(len)
    dfg['len_tax_ids'] = dfg['merchant_tax_id'].apply(len)


    dfg['cnpj_diff_tax'] = dfg.apply(lambda x: set(x['cnpj_raiz'])!=set(x['tax_id_raiz']) and len(x['cnpj_raiz'])>0 and len(x['tax_id_raiz'])>0, axis=1)



    dfg[(dfg['len_cnpjs']>0) & dfg['cnpj_diff_tax']]


    dfg[(dfg['len_cnpjs']>0) | (dfg['len_tax_ids']>0)]



    ###############################

    def random_sample(df, n=10):
        #from random import shuffle
        #inicios = list(df['inicio'].unique())
        #shuffle(inicios)
        inicios = ['ANACAR']
        for ini in inicios[:20]:
            print(ini)
            display(df[df['inicio']==ini][['subs_asterisk', 'nome_master', 'nome_muni', 'uf', 'resultado_names']])


def save_name_grouping(df, cod_muni, output_dir):
    print('-----------\n Analyzing:', cod_muni)
    df = df.copy()
    LEVEL_GROUP = []
    df = df[['nome_master', ]]
    df = group_and_clean(df, LEVEL_GROUP)
    df.to_parquet(f"{output_dir}/part_{cod_muni}.parquet")
    print('Saved:', cod_muni)


def vm_load_grouping():
    import os
    output_dir = r'data/nomes_agrupados'
    df = pd.concat([pd.read_parquet(f"{output_dir}/{f}") for f in os.listdir(output_dir)])
    return df


