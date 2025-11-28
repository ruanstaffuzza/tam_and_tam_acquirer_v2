#%%

import pandas as pd
import pandas_gbq


table_temp = 'dataplatform-prd.master_contact.temp_infinite_sample'

def create_sample_infinite():
        
    query_1 = """
    select a.*, b.merchant_market_hierarchy_id is not null as in_big_mmhid
    from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` a
    left join `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` b using(reference_month, merchant_market_hierarchy_id)
    where subs_asterisk = 'CloudWalk'
    and reference_month = '2025-06-30'
    order by rand()
    limit 1000
    """



    if not 'df_1' in locals():
        df_1 = pandas_gbq.read_gbq(query_1, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')



    """
    TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')) AS nome,

    """
    if df_1 is None or df_1.empty:
        raise ValueError('Query returned no data or table not found.')

    df = df_1.copy()
    df['nome'] = df['nome_limpo'].str.replace(r'^SHOPEE|IFOOD', '', regex=True).str.strip()
    df['nome_master'] = df['nome'].str.replace(' ', '', regex=False)

    print('Quantidade única de municípios:', df['cod_muni'].nunique())
    print('Quantidade única de nomes:', df['nome_master'].nunique())

    # upload to temporary bigquery table
    

    pandas_gbq.to_gbq(df, table_temp, project_id='sfwthr2a4shdyuogrt3jjtygj160rs', if_exists='replace')


#%%

table_temp = 'dataplatform-prd.master_contact.temp_infinite_sample'

query_2 = f"""
WITH nomes AS (
SELECT DISTINCT
reference_month reference_date, 
  nome_master,
  cod_muni,
  from `{table_temp}`
)

select *
from  (
select distinct reference_date, tam_id from `dataplatform-prd.master_contact.v2_aux_tam_pos_python` 
inner join nomes using(reference_date, nome_master, cod_muni)
where reference_date = '2025-06-30'
)
left join `dataplatform-prd.master_contact.v2_aux_tam_pos_python` using(reference_date, tam_id)
"""

df_2 = pandas_gbq.read_gbq(query_2, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')
#%%


query_3 = f"""
WITH nomes AS (
SELECT DISTINCT
reference_month reference_date, 
  nome_master,
  cod_muni,
  from `{table_temp}`
)

select *
from  (
select distinct reference_date, tam_id from `dataplatform-prd.master_contact.aux_tam_pos_python` 
inner join nomes using(reference_date, nome_master, cod_muni)
where reference_date = '2025-06-30'
)
left join `dataplatform-prd.master_contact.aux_tam_pos_python` using(reference_date, tam_id)
"""

if not 'df_3' in locals():
    df_3 = pandas_gbq.read_gbq(query_3, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')


#%%

colunas_atencao = [
    #'reference_date', 
    'tam_id', 
    #'merchant_market_hierarchy_id',
       'merchant_name', 'subs_asterisk', 
       #'document', 'source_document',
       #'merchant_tax_id', 'numero_inicio', 'cnpj', 'cpf', 'cpf_brasil',
       'cod_muni', 'inicio', 'nome_master', 
       #'is_mp', 
       'pre_doc_unmerge',
       #'new_mmhid_merge', 'nome_merge', 
       'mmhid_merge', 'mmhid_choice',
       'choice', 'idx_group', 'res_group_antigo', 'group_idx_unmerge_docs',
       #'ingestion_date', 
       #'id_ton', 'document_rfb', 'first_seen_week',
       #'mcc_places', 'len_resultado', 'in_bus_flag_30d', 
       'source_name'
]

df = df_2[colunas_atencao].copy()
sub_diff_infinite = df.groupby('tam_id')['subs_asterisk'].transform(lambda x: x.nunique()>1)
df = df[sub_diff_infinite]
ids = df['nome_master'].drop_duplicates()



#%%
#sample_nome = ids.sample(1).values[0]
sample_nome = 'JOAOBATISTADOMINGOS'


df = df_2[colunas_atencao].copy()
sample_tam_id = df[df['nome_master']==sample_nome]['tam_id'].values
df_sample = df[df['tam_id'].isin(sample_tam_id)].copy().drop_duplicates()
df_sample

#%%
colunas_atencao2 = [
    #'reference_date', 
    'tam_id', 
    'merchant_market_hierarchy_id',
       'merchant_name', 'subs_asterisk', 
       #'document', 'source_document',
       #'merchant_tax_id', 'numero_inicio', 'cnpj', 'cpf', 'cpf_brasil',
       'grouped_names', 'nome_master', 'cod_muni', 'inicio', 'group_idx_nome',
       'mmhid_merge', 'id_merge_places', 'group_idx_merge_places', 'group_id',
       'group_idx_unmerge_places', 'group_idx_unmerge_docs', 
       #'ingestion_date',
       #'first_seen_week', 'mcc_places', 'len_resultado', 'in_bus_flag_30d',
       'source_name'
       ]

nomes_procura = ['JOAOBATISTADEBRITO', 'JOAOBATISTADOMINGOS']


df = df_3[colunas_atencao2].copy()
#sample_tam_id = df[df['nome_master']==sample_nome]['tam_id'].values
#df_sample_3 = df[df['tam_id'].isin(sample_tam_id)].copy()

sample_tam_ids = df[df['nome_master'].isin(nomes_procura)]['tam_id'].values
df_sample_3 = df[df['tam_id'].isin(sample_tam_ids)].copy()

df_sample['grouped_names'] = df_sample_3['grouped_names'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
df_sample = df_sample.drop_duplicates()
df_sample_3

#%%


query_4 = f"""

select *
from `dataplatform-prd.master_contact.aux_tam_pos_python`
where reference_date = '2025-06-30'
and cod_muni = 2611101
and inicio = 'JOAOBA'
"""

if not 'df_4' in locals():
    df_4 = pandas_gbq.read_gbq(query_4, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')

df_4

#valid_names = df_4[df_4['nome_master'].str.startswith('JOAOBATISTA')]['group_idx_nome'].unique()

nomes_int = ['JOAOBATISTA', 'JOAOBATISTALI', 'JOAOBATISTADOMINGOS',
       'JOAOBATISTADEBRITO']

valid_names = df_4[df_4['nome_master'].isin(nomes_int)]['group_idx_nome'].unique()

i = 1
for g in valid_names:
    print(f'Grupo {i} - group_idx_nome: {g}')
    i+=1
    display(df_4[df_4['group_idx_nome']==g][colunas_atencao2])
