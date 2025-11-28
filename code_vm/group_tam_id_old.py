
import pandas as pd
import numpy as np

from IPython.display import display
import networkx as nx

from collections import defaultdict

import time


OUTROS = ['Outros', 'Outros_Pags', 'Outros_SumUp']


# Custom aggregation function to join unique, non-null values
def join_unique(series):
    return list(series.dropna().unique())



def assign_group_ids(df, id_cols, final_col):
    
    # Criando um grafo onde cada nó é uma linha do DataFrame
    G = nx.Graph()
    for idx, row in df.iterrows():
        for col in id_cols:
            G.add_edge(f"{col}_{row[col]}", idx)

    # Encontrando os componentes conectados no grafo
    components = list(nx.connected_components(G))

    # Criando um mapeamento de índice para group_id
    index_to_group = {}
    for group_id, component in enumerate(components, start=1):
        indices = [node for node in component if isinstance(node, int)]
        for idx in indices:
            index_to_group[idx] = group_id

    # Adicionando a coluna group_id ao DataFrame original
    df[final_col] = df.index.map(index_to_group)
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

def assign_group_ids(df, id_cols, final_col):
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
    df = df[df['nome_ton'].str.len()>=5]

    df['nome_master'] = df['nome_ton'].str.replace(r'[^A-Z]', '', regex=True)

    df = df.drop_duplicates()

    df['qtd_documents'] = df.groupby(LEVEL_GROUP + ['nome_master'])['document_ton'].transform('nunique')

    df.loc[df['qtd_documents']>1, 'nome_master'] = df['nome_master'] + df['document_ton']
    
    df.loc[df['document_ton'].str.len()==14, 'cnpj'] = df['document_ton']
    df.loc[df['document_ton'].str.len()==11, 'cpf'] = df['document_ton']

    df = df.drop(columns=['qtd_documents', 'document_ton'])

    df['subs_asterisk'] = 'Ton'
    return df


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

    

    #display(df[['nome_muni', 'uf']].value_counts())

    #display(df['subs_asterisk'].value_counts())

    #if LEVEL_GROUP:
        #display(df[LEVEL_GROUP].value_counts())
    

    #else:         print('Sem LEVEL_GROUP')
    #display(df[LEVEL_GROUP + ['inicio']].value_counts().head(5))

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
        
    big_mmhid = df[['merchant_market_hierarchy_id', 'nome_master'] + LEVEL_GROUP].drop_duplicates()['merchant_market_hierarchy_id'].value_counts() # Verificar isso daqui melhor 
    
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


def init_group_id(df):
    df = df.copy()

    df['muni'] = df['nome_muni'] + ' ' + df['uf']
    df['group_id'] = df['resultado_names'].apply(lambda x: list(np.sort(x))[-1]) 
    df['group_id'] = df[['group_id', 'muni']].apply(lambda x: '|'.join(x), axis=1)

    df = df.rename(columns={'resultado_names': 'grouped_names'})
    df = df.drop(columns=['muni'])

    return df



def deal_unmerged_places(df, LEVEL_GROUP):
    df = df.copy()

    df['mmhid_places'] = df['mmhid_merge'].apply(lambda x: str(x) if not pd.isna(x) else np.nan) #deal with nan in Int
    df['new_id'] = np.arange(df.shape[0])
    df.loc[df['subs_asterisk']=='Ton', 'mmhid_places'] = 'Ton' + df['id_ton'].astype(str)
    df.loc[df['mmhid_places'].isnull(), 'mmhid_places'] = 'created' + df['new_id'].astype(str)

    #df['muni'] =  df[LEVEL_GROUP].astype(str).apply(lambda x: ' '.join(x))

    if len(LEVEL_GROUP)>1:
        df['mmhid_places'] = df['mmhid_places'] + ' - ' + df[LEVEL_GROUP].astype(str).apply(lambda x: '|'.join(x), axis=1)#df['muni']
    elif len(LEVEL_GROUP)==1:
        df['mmhid_places'] = df['mmhid_places'] + ' - ' + df[LEVEL_GROUP[0]].astype(str)


    df = assign_group_ids(df, ['group_id', 'mmhid_places'], final_col='group_id_index')
    
    df_grouped = df.copy()
    # SPLIT group_id by | . Get first element
    #df_grouped['group_id'] = df_grouped['group_id'].str.split('|').apply(lambda x: x[0])

    # concat group_id with group_id_index
    #df_grouped = df_grouped.groupby(['group_id_index'], as_index=False)['group_id'].apply(lambda x: list(x.dropna().unique()))
    #df_grouped = df_grouped.groupby(['group_id_index'], as_index=False)['group_id'].apply(lambda x: list(x.dropna().unique()))
    df_grouped = df_grouped[['group_id_index', 'group_id']].drop_duplicates().groupby('group_id_index', as_index=False).head(10)
    
    df_grouped = df_grouped.groupby(['group_id_index'], as_index=False)['group_id'].apply(lambda x: ', '.join(x))

    df = df.drop(columns=['group_id', 'mmhid_places', 'new_id', 'id_ton'])
    df = df.merge(df_grouped, on='group_id_index', how='left')

    #df = df.drop(columns=['group_id_index'])
    return df


def deal_merged_places(df, LEVEL_GROUP):
    df = df.copy()

    
    df['mmhid_places'] = df.loc[df['subs_asterisk'].isin(OUTROS), 'merchant_market_hierarchy_id']

    df_grouped = df.copy()

    level_group2 = ['group_id_index']
    
    df_grouped = df_grouped[df_grouped['mmhid_places'].notnull()]
    df_grouped = df_grouped[LEVEL_GROUP + level_group2 +  ['mmhid_places']].drop_duplicates()
    # Group by the specified columns and aggregate
    df_grouped = df_grouped.groupby(LEVEL_GROUP + level_group2).agg(
        unique_mmhid_places=('mmhid_places', 'first'),
        nunique_mmhid_places=('mmhid_places', 'nunique')
    ).reset_index()

    df = df.merge(df_grouped, on=LEVEL_GROUP + level_group2, how='left')
    

    df.loc[(df['nunique_mmhid_places']>1)
           & (df['subs_asterisk'].isin(OUTROS))
           , 'group_id'] = df['group_id'].astype(str) + ' - ' + df['mmhid_places'].astype(str)
    
    df.loc[(df['nunique_mmhid_places']>1)
           & (~df['subs_asterisk'].isin(OUTROS))
           , 'group_id'] = df['group_id'].astype(str) + ' - ' + df['unique_mmhid_places'].astype(str)

    
    df = assign_group_ids(df, ['group_id'], final_col='group_id_index')
    
    df = df.drop(columns=['mmhid_places', 'unique_mmhid_places', 'nunique_mmhid_places'])
    
    return df

def choose_prefered_document(df):
    # Dá preferência para o tax_id da places (depois do agrupamento), depois para o tax_id de 'MP e Ton por fora', Depois o cnpj do algorítimo e por último cpf do algorítmo.

    #document = coalesc(cpf, cnpj)
    df = df.copy()
    

    df['cnpj'] = np.where(df['qtd_cnpjs'].fillna(0)<=1, df['cnpj'], None)
    df['cpf'] = np.where(df['qtd_cpfs'].fillna(0)<=1, df['cpf'], None)
    del df['qtd_cpfs'], df['qtd_cnpjs']


    df.loc[df['numero_inicio'].notnull(), 'cnpj_contains_numero_inicio'] = df.loc[df['numero_inicio'].notnull()].apply(lambda row: str(row['cnpj']).startswith(str(row['numero_inicio'])) if row['cnpj'] is not None and row['numero_inicio'] is not None else False, axis=1)
    df['cnpj_contains_numero_inicio'] = df['cnpj_contains_numero_inicio'].fillna(False)

    df.loc[df['cnpj_contains_numero_inicio'], 'numero_inicio'] = df['cnpj']

    df = df.drop(columns=['cnpj_contains_numero_inicio'])

    df['cnpj'] = coalesce(df, ['numero_inicio', 'cnpj'])


    df['len_resultado'] = df.groupby('group_id_index')['nome_master'].transform('count')

    df['document'] = coalesce(df, ['merchant_tax_id', 'numero_inicio', 'cnpj', 'cpf', 'cpf_brasil'])

    df['source_document'] = 'Busca'
    df.loc[df['subs_asterisk'].isin(['Ton', 'MercadoPago_subPagarme']), 'source_document'] = df['subs_asterisk']

    df.loc[df['merchant_tax_id'].notnull(), 'source_document'] = 'Places'

    order_importance_docu = {
        'Places': 0,       
        'Ton': 1, 'MercadoPago_subPagarme': 2,
        'Busca': 3
    }

    # Step 2: Identify the preferred document for each group
    # First, sort the DataFrame by 'order_docu' to ensure the row with the minimum 'order_docu' is first within each group
    df_sorted = df[(df['document'].notnull())
                & (df['len_resultado']>1)
                ][['group_id_index', 'document', 'source_document']].copy()
    
    df_sorted['order_docu'] = df_sorted['source_document'].map(order_importance_docu)
    
    df_sorted = df_sorted.sort_values(by=['group_id_index', 'order_docu'])

    # Then, drop duplicates to keep the first occurrence (minimum 'order_docu') within each group
    df_preferred = df_sorted.drop_duplicates(subset=['group_id_index'], keep='first')

    df_preferred = df_preferred.rename(columns={'document': 'documento_final'})
    df_preferred = df_preferred.drop(columns=['order_docu'])


    df = df.merge(df_preferred, on=['group_id_index'], how='left',
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

    df = assign_group_ids(df, ['agrupamento_nome_1', 'group_id_index', 'document_mod'], final_col='agrupamento_inspecao')

    df = df.drop(columns=['document_mod'])
    df['qtd_distinct_agrupamento_nome_1'] = df.groupby('agrupamento_inspecao')['agrupamento_nome_1'].transform('nunique')

    df['len_inspecao'] = df.groupby('agrupamento_inspecao')['nome_master'].transform('nunique')

    return df


def grouped_subs_asterisk(df, grouping_id=['agrupamento_inspecao']):
    df = df.copy()
    dict_dummies = {
        'MercadoPago_subPagarme': 'mp_subs',
        'Ton': 'ton',
        'MercadoPago': 'mp',
        'PagSeguro': 'pagseguro',
        'SumUp': 'sumup',
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


def final_ajustes(df):
    df = df.copy()
    
    df = df[~((df['subs_asterisk']=='MercadoPago_subPagarme') & (df['len_resultado']==1))]

    df.loc[df['documento_final'].isnull(), 'source_document'] = 'Sem Documento'

    #df = df.sort_values(by=['nome_muni', 'uf', 'group_id', 'nome_master', 'subs_asterisk', 'len_resultado']).reset_index(drop=True)
    
    order_cols = [
    'group_id', 'group_id_index', 'nome_muni', 'uf', 'merchant_market_hierarchy_id', 'nome_master', 'subs_asterisk', 
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


    df = assign_group_ids(df, ['merchant_tax_id', 'group_id'], final_col='grouped_tax_id')

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
                            'nome_ton',  'qtd_mmhid_tax_id',
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
        sort_by = ['grouped_tax_id', 'nome_muni', 'uf', 'group_id', 'nome_master', 'subs_asterisk']
        df = df.sort_values(by=sort_by).reset_index(drop=True)
        df = delete_filter_cols(df, drop_has=False)
        extra_cols_del = ['qtd_distinct_agrupamento_nome_1', 'agrupamento_inspecao']
        df = df.drop(columns=extra_cols_del)
        # drop duplicates all except list types series
        df['grouped_names'] = df['grouped_names'].apply(lambda x: ', '.join(x) )
        df = df.drop_duplicates()
        return df


    import pandas as pd


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

def execute_with_context(LEVEL_GROUP, display_flag=False, verbose=False):
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

def main_data_treat(df, ton, LEVEL_GROUP, display_flag=False, verbose=False):
    """
    Main function to process data with options to display intermediate DataFrames and print verbose messages.

    Parameters:
    - read_data_func: Function to read initial data.
    - display_flag: Boolean flag indicating whether to display the DataFrame after each processing step.
    - verbose: Boolean flag indicating whether to print verbose messages during processing.
    """
    execute_ctx = execute_with_context(LEVEL_GROUP, display_flag=display_flag, verbose=verbose)

    df = open_init_data(df, ton, LEVEL_GROUP)
    df = execute_ctx(group_and_clean, df)
    df = execute_ctx(deal_with_big_mmhid, df)
    df = execute_ctx(init_group_id, df)
    df = execute_ctx(deal_merged_places, df)
    df = execute_ctx(deal_unmerged_places, df)
    df = execute_ctx(choose_prefered_document, df)
    df = execute_ctx(create_agrupamento_inspecao, df)
    df = execute_ctx(grouped_subs_asterisk, df)
    df = execute_ctx(final_ajustes, df)
    return df


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




def main_old():
    from utils import read_data
    LEVEL_GROUP = ['cod_muni']
    df_orig = read_data('final_nomes')
    #df_orig['cod_muni'] = df_orig['cod_muni'].fillna(9999)
    df_orig = df_orig[df_orig['cod_muni'].notnull()]
    ton_orig = open_ton(read_data, LEVEL_GROUP)
    #ton_orig['cod_muni'] = ton_orig['cod_muni'].fillna(9999)

    for cod_muni in sorted(df_orig['cod_muni'].unique()):
        df = df_orig[df_orig['cod_muni']==cod_muni]
        ton = ton_orig[ton_orig['cod_muni']==cod_muni]

        start = time.time()

        df = main_data_treat(df, ton, LEVEL_GROUP, display_flag=False, verbose=False)
        df = create_doc_final(df, LEVEL_GROUP, save_excel_inspecao=False)

        print(f'Main data treat Execution time: {time.time() - start:.2f} seconds')
        df['group_id_index'] = (df['group_id_index'].astype(str) + ' - ' + df['cod_muni'].astype(str))
        load_to_gbq(df,
                    destination_table='master_contact.temp_nomes_agrupados',
                    chunk_size=10000)
        
    import gc# Liberar memória do subconjunto processado
    del df
    del ton
    gc.collect()



def open_part(number, LEVEL_GROUP):
  from utils import read_data
  df = read_data(f'part_{number}')
  ton = open_ton(read_data, LEVEL_GROUP)
  ton = ton[ton['cod_muni'].isin(df['cod_muni'])]
  return df, ton

def treat_and_save_part(number):

    LEVEL_GROUP = ['cod_muni']
    df, ton = open_part(number, LEVEL_GROUP)

    display(df)
    display(ton)
    
    start = time.time()

    df = main_data_treat(df, ton, LEVEL_GROUP, display_flag=False, verbose=True)
    
    df = create_doc_final(df, LEVEL_GROUP, save_excel_inspecao=False)

    print(f'Main data treat Execution time: {time.time() - start:.2f} seconds')
    #df['group_id_index'] = (df['group_id_index'].astype(str) + ' - ' + df['cod_muni'].astype(str))
    #load_to_gbq(df,
    #           destination_table='master_contact.temp_nomes_agrupados',
        #              chunk_size=50000)
    return df
    

def main():
    number = 17
    df = treat_and_save_part(number)
    return df



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

def vm_save_grouping(read_data_func):
    output_dir = r'data/nomes_agrupados'
    df = read_data_func('final_nomes')
    LEVEL_GROUP = ['cod_muni']
    ton = open_ton(read_data_func, LEVEL_GROUP)
    df = open_init_data(df, ton, LEVEL_GROUP)
    df['cod_muni'] = df['cod_muni'].fillna(9999)
    df = df[['nome_master', 'inicio', 'cod_muni']].drop_duplicates()
    unique_cod_muni = df['cod_muni'].unique()
    for muni in unique_cod_muni:
        save_name_grouping(df[df['cod_muni']==muni], muni, output_dir)
        

def vm_load_grouping():
    import os
    output_dir = r'data/nomes_agrupados'
    df = pd.concat([pd.read_parquet(f"{output_dir}/{f}") for f in os.listdir(output_dir)])
    return df



if __name__ == '__main__':
    main()

def test():
        
    from utils import read_data
    LEVEL_GROUP = ['nome_muni', 'uf']
    df = read_data('final_nomes')
    ton = open_ton(read_data, LEVEL_GROUP)

    df = open_init_data(df, ton, LEVEL_GROUP)

    s = df[LEVEL_GROUP + ['inicio']].value_counts()

    def tempo_execucao_polinomio(quantidade_nomes):
        
        
        # Coeficientes do polinômio ajustado
        a = 3.26250498e-06
        b = -1.07651651e-04
        c = -0.0630932160

        # Aplicação do polinômio
        tempo_execucao = a * quantidade_nomes**2 + b * quantidade_nomes + c
        
        return tempo_execucao

    quantidade = 5318
    tempo = tempo_execucao_polinomio(quantidade)

    s = s.reset_index()
    s.columns = ['nome_muni', 'uf', 'inicio', 'qtd']

    s['tempo'] = s['qtd'].apply(tempo_execucao_polinomio)
    s['tempo'] = s['tempo'].where(s['tempo']>=0, 0.1)


    tempo_total = s[s['qtd']>200]['tempo'].sum()

    print(f'Tempo total: {tempo_total/3600:.2f} horas')

    s[s['qtd']>1000]

    log = """
    Executing function group_and_clean...
    Tamanho do DataFrame: (19669386, 16)
    Quantidade de nomes: 1123
    Tempo de execução: 3.780973196029663
    Quantidade de nomes: 1024
    Tempo de execução: 4.419109344482422
    Quantidade de nomes: 1830
    Tempo de execução: 7.36138129234314
    Quantidade de nomes: 2733
    Tempo de execução: 32.72299766540527
    Quantidade de nomes: 3332
    Tempo de execução: 31.888190269470215
    Quantidade de nomes: 1273
    Tempo de execução: 6.989509582519531
    Quantidade de nomes: 1670
    Tempo de execução: 6.141047477722168
    Quantidade de nomes: 1627
    Tempo de execução: 7.164242744445801
    Quantidade de nomes: 2352
    Tempo de execução: 17.032291650772095
    Quantidade de nomes: 1003
    Tempo de execução: 4.0595197677612305
    Quantidade de nomes: 1409
    Tempo de execução: 8.498892784118652
    Quantidade de nomes: 1662
    Tempo de execução: 10.121505737304688
    Quantidade de nomes: 1167
    Tempo de execução: 5.7216477394104
    Quantidade de nomes: 1225
    Tempo de execução: 3.1907546520233154
    Quantidade de nomes: 1804
    Tempo de execução: 7.180134534835815
    Quantidade de nomes: 2219
    Tempo de execução: 12.225756406784058
    Quantidade de nomes: 1055
    Tempo de execução: 3.032090425491333
    Quantidade de nomes: 3688
    Tempo de execução: 45.44279479980469
    Quantidade de nomes: 1341
    Tempo de execução: 6.954554557800293
    Quantidade de nomes: 2321
    Tempo de execução: 20.71984553337097
    Quantidade de nomes: 3548
    Tempo de execução: 30.18700408935547
    Quantidade de nomes: 5318
    Tempo de execução: 76.37330985069275
    Quantidade de nomes: 1495
    Tempo de execução: 4.896443128585815
    Quantidade de nomes: 2200
    Tempo de execução: 11.026203393936157
    Quantidade de nomes: 1349
    Tempo de execução: 4.0867838859558105
    Quantidade de nomes: 1444
    Tempo de execução: 5.045894145965576
    Quantidade de nomes: 1161
    Tempo de execução: 2.9747674465179443
    Quantidade de nomes: 1067
    Tempo de execução: 2.527006149291992
    Quantidade de nomes: 5907
    Tempo de execução: 82.99416208267212
    Quantidade de nomes: 1815
    Tempo de execução: 7.542592287063599
    Quantidade de nomes: 1034
    Tempo de execução: 2.3580548763275146
    Quantidade de nomes: 1055
    Tempo de execução: 2.5167362689971924
    Quantidade de nomes: 2224
    Tempo de execução: 11.343559741973877
    Quantidade de nomes: 1278
    Tempo de execução: 3.577608823776245
    Quantidade de nomes: 1426
    Tempo de execução: 4.460250377655029
    Quantidade de nomes: 1105
    Tempo de execução: 2.9247143268585205
    Quantidade de nomes: 1004
    Tempo de execução: 2.2051308155059814
    Quantidade de nomes: 1287
    Tempo de execução: 3.644083023071289
    Quantidade de nomes: 1765
    Tempo de execução: 6.880047082901001
    Quantidade de nomes: 2568
    Tempo de execução: 15.633466720581055
    Quantidade de nomes: 1266
    Tempo de execução: 3.5107030868530273
    Quantidade de nomes: 2286
    Tempo de execução: 11.742856740951538
    Quantidade de nomes: 2873
    Tempo de execução: 19.022929668426514
    Quantidade de nomes: 3378
    Tempo de execução: 27.750410795211792
    Quantidade de nomes: 1504
    Tempo de execução: 5.053581237792969
    Quantidade de nomes: 1323
    Tempo de execução: 3.786128282546997
    Quantidade de nomes: 1031
    Tempo de execução: 2.1968703269958496
    Quantidade de nomes: 1064
    Tempo de execução: 2.4682247638702393
    Quantidade de nomes: 1402
    Tempo de execução: 4.225332975387573
    Quantidade de nomes: 1996
    Tempo de execução: 9.014804363250732
    Quantidade de nomes: 2665
    Tempo de execução: 16.404634952545166
    Quantidade de nomes: 3343
    Tempo de execução: 26.131651401519775
    Quantidade de nomes: 2198
    Tempo de execução: 11.402621984481812
    Quantidade de nomes: 1017
    Tempo de execução: 2.281825542449951
    Quantidade de nomes: 3965
    Tempo de execução: 36.02621006965637
    Quantidade de nomes: 1893
    Tempo de execução: 8.192617416381836
    Quantidade de nomes: 1587
    Tempo de execução: 5.540434122085571
    Quantidade de nomes: 1935
    Tempo de execução: 8.395827770233154
    Quantidade de nomes: 2475
    Tempo de execução: 14.408753395080566
    Quantidade de nomes: 1067
    Tempo de execução: 2.4618022441864014
    Quantidade de nomes: 1017
    Tempo de execução: 2.2257449626922607
    Quantidade de nomes: 1205
    Tempo de execução: 3.1383583545684814
    Quantidade de nomes: 2727
    Tempo de execução: 16.99620509147644
    Quantidade de nomes: 4099
    Tempo de execução: 39.84337306022644
    Quantidade de nomes: 4811
    Tempo de execução: 55.64328145980835
    Quantidade de nomes: 1193
    Tempo de execução: 3.032071590423584
    Quantidade de nomes: 1010
    Tempo de execução: 2.2495338916778564
    Quantidade de nomes: 1373
    Tempo de execução: 4.013135671615601
    Quantidade de nomes: 1220
    Tempo de execução: 3.228930711746216
    Quantidade de nomes: 1017
    Tempo de execução: 2.3129069805145264
    Quantidade de nomes: 2388
    Tempo de execução: 13.151104211807251
    Quantidade de nomes: 1726
    Tempo de execução: 6.608948469161987
    Quantidade de nomes: 1188
    Tempo de execução: 3.0810327529907227
    Quantidade de nomes: 1273
    Tempo de execução: 3.5449841022491455
    Quantidade de nomes: 1400
    Tempo de execução: 4.264076471328735
    Quantidade de nomes: 1265
    Tempo de execução: 3.5486435890197754
    Quantidade de nomes: 1487
    Tempo de execução: 4.956701755523682
    Quantidade de nomes: 1347
    Tempo de execução: 3.975614070892334
    Quantidade de nomes: 1128
    Tempo de execução: 2.713658571243286
    Quantidade de nomes: 2090
    Tempo de execução: 9.8557448387146
    Quantidade de nomes: 2388
    Tempo de execução: 13.368314266204834
    Quantidade de nomes: 1439
    Tempo de execução: 4.514467477798462
    Quantidade de nomes: 1020
    Tempo de execução: 2.1910433769226074
    Quantidade de nomes: 1794
    Tempo de execução: 7.121836423873901
    Quantidade de nomes: 1945
    Tempo de execução: 8.248716115951538
    Quantidade de nomes: 1115
    Tempo de execução: 2.7288260459899902
    Quantidade de nomes: 2021
    Tempo de execução: 8.860324382781982
    Quantidade de nomes: 3630
    Tempo de execução: 30.041093111038208
    Quantidade de nomes: 2719
    Tempo de execução: 16.76395559310913
    Quantidade de nomes: 1692
    Tempo de execução: 6.11924934387207
    Quantidade de nomes: 1161
    Tempo de execução: 2.8925678730010986
    Quantidade de nomes: 1549
    Tempo de execução: 5.301831245422363
    Quantidade de nomes: 1895
    Tempo de execução: 8.04400086402893
    Quantidade de nomes: 1009
    Tempo de execução: 2.214205741882324
    Quantidade de nomes: 1212
    Tempo de execução: 3.138049364089966
    Quantidade de nomes: 2134
    Tempo de execução: 10.164833545684814
    Quantidade de nomes: 2426
    Tempo de execução: 13.010083675384521
    Quantidade de nomes: 1902
    Tempo de execução: 8.007477283477783
    Quantidade de nomes: 3036
    Tempo de execução: 21.493298768997192
    Quantidade de nomes: 3864
    Tempo de execução: 35.44588112831116
    Quantidade de nomes: 1104
    Tempo de execução: 2.6849279403686523
    Quantidade de nomes: 1433
    Tempo de execução: 4.840120315551758
    Quantidade de nomes: 1400
    Tempo de execução: 4.331254720687866
    Quantidade de nomes: 1053
    Tempo de execução: 2.501276969909668
    Quantidade de nomes: 7243
    Tempo de execução: 132.52485823631287
    Quantidade de nomes: 1034
    Tempo de execução: 2.357257127761841
    Quantidade de nomes: 2084
    Tempo de execução: 10.003133773803711
    Quantidade de nomes: 4154
    Tempo de execução: 41.62828850746155
    Quantidade de nomes: 5042
    Tempo de execução: 64.23828625679016
    Quantidade de nomes: 1895
    Tempo de execução: 8.133081197738647
    Quantidade de nomes: 2341
    Tempo de execução: 12.248432159423828
    Quantidade de nomes: 4048
    Tempo de execução: 40.04891633987427
    Quantidade de nomes: 4529
    Tempo de execução: 49.56388735771179
    Quantidade de nomes: 1061
    Tempo de execução: 2.4267561435699463
    Quantidade de nomes: 1418
    Tempo de execução: 4.474344968795776
    Quantidade de nomes: 1741
    Tempo de execução: 6.812260627746582
    Quantidade de nomes: 1542
    Tempo de execução: 5.288418769836426
    Quantidade de nomes: 1646
    Tempo de execução: 5.924682378768921
    Quantidade de nomes: 1194
    Tempo de execução: 3.089625597000122
    Quantidade de nomes: 1178
    Tempo de execução: 2.951561689376831
    Quantidade de nomes: 1099
    Tempo de execução: 2.6026113033294678
    Quantidade de nomes: 1147
    Tempo de execução: 2.756854772567749
    Quantidade de nomes: 1777
    Tempo de execução: 6.847253084182739
    Quantidade de nomes: 1763
    Tempo de execução: 6.684283971786499
    Quantidade de nomes: 1437
    Tempo de execução: 4.437581300735474
    Quantidade de nomes: 2021
    Tempo de execução: 9.0010347366333
    Quantidade de nomes: 2520
    Tempo de execução: 14.009820461273193
    Quantidade de nomes: 1255
    Tempo de execução: 3.9303460121154785
    Quantidade de nomes: 2467
    Tempo de execução: 15.495421648025513
    Quantidade de nomes: 2413
    Tempo de execução: 12.302613258361816
    Quantidade de nomes: 2344
    Tempo de execução: 12.844786405563354
    Quantidade de nomes: 1005
    Tempo de execução: 2.2224202156066895
    Quantidade de nomes: 1140
    Tempo de execução: 3.1185414791107178
    Quantidade de nomes: 1069
    Tempo de execução: 2.5841567516326904
    Quantidade de nomes: 1265
    Tempo de execução: 3.5815305709838867
    Quantidade de nomes: 1877
    Tempo de execução: 8.423616170883179
    Quantidade de nomes: 2233
    Tempo de execução: 11.664642572402954
    Quantidade de nomes: 1456
    Tempo de execução: 4.562468528747559
    Quantidade de nomes: 1712
    Tempo de execução: 6.5154547691345215
    Quantidade de nomes: 1819
    Tempo de execução: 7.208775043487549
    Quantidade de nomes: 1079
    Tempo de execução: 2.5190412998199463
    Quantidade de nomes: 1211
    Tempo de execução: 3.0691134929656982
    Quantidade de nomes: 1801
    Tempo de execução: 7.0491297245025635
    Quantidade de nomes: 2229
    Tempo de execução: 10.916043758392334
    Quantidade de nomes: 2789
    Tempo de execução: 17.365405321121216
    Quantidade de nomes: 1354
    Tempo de execução: 3.8879499435424805
    Quantidade de nomes: 2979
    Tempo de execução: 19.909943103790283
    Quantidade de nomes: 3206
    Tempo de execução: 23.61382222175598
    Quantidade de nomes: 3968
    Tempo de execução: 36.39641213417053
    Quantidade de nomes: 1143
    Tempo de execução: 2.7707083225250244
    Quantidade de nomes: 1363
    Tempo de execução: 4.043673515319824
    Quantidade de nomes: 1610
    Tempo de execução: 5.6916184425354
    Quantidade de nomes: 1162
    Tempo de execução: 2.876742362976074
    Quantidade de nomes: 1535
    Tempo de execução: 5.151716709136963
    Quantidade de nomes: 1831
    Tempo de execução: 7.374971151351929
    Quantidade de nomes: 1628
    Tempo de execução: 5.6864848136901855
    Quantidade de nomes: 2942
    Tempo de execução: 19.459635972976685
    Quantidade de nomes: 3196
    Tempo de execução: 22.923614025115967
    Quantidade de nomes: 1169
    Tempo de execução: 2.9923629760742188
    Quantidade de nomes: 1140
    Tempo de execução: 2.81054949760437
    Quantidade de nomes: 1358
    Tempo de execução: 3.9155945777893066
    Quantidade de nomes: 2056
    Tempo de execução: 9.143181085586548
    Quantidade de nomes: 1314
    Tempo de execução: 3.8759546279907227
    Quantidade de nomes: 1614
    Tempo de execução: 5.761342763900757
    Quantidade de nomes: 1096
    Tempo de execução: 2.572582244873047
    Quantidade de nomes: 1083
    Tempo de execução: 2.5226879119873047
    Quantidade de nomes: 1085
    Tempo de execução: 2.4180281162261963
    Quantidade de nomes: 2151
    Tempo de execução: 10.352117538452148
    Quantidade de nomes: 1870
    Tempo de execução: 7.666909694671631
    Quantidade de nomes: 1064
    Tempo de execução: 2.4532761573791504
    Quantidade de nomes: 1144
    Tempo de execução: 2.937824249267578
    """

    def create_table_log(log):
        import re
        import pandas as pd
        matches = re.findall(r'Quantidade de nomes: (\d+)', log)
        times = re.findall(r'Tempo de execução: (\d+\.\d+)', log)
        df = pd.DataFrame({'quantidade_nomes': matches, 'tempo_execucao': times})
        df['quantidade_nomes'] = df['quantidade_nomes'].astype(int)
        df['tempo_execucao'] = df['tempo_execucao'].astype(float)
        return df

    df_log = create_table_log(log)

    # count 'Quantidade de nomes' in the log
    import re
    matches = re.findall(r'Quantidade de nomes:', log)
    print(len(matches))