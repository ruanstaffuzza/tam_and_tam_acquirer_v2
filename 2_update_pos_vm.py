#%%
from datetime import datetime, timedelta

from IPython.display import display
import pandas as pd

import pandas_gbq

from utils_update import run_query


def create_query_check_n_rows(table_name, ref_month, ref_month_name = 'reference_month'):
    query = f"""
    select count(*) as n_rows from {table_name} where {ref_month_name} = '{ref_month}'"""
    return pandas_gbq.read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')


def create_query_check_last_n_rows(table_name, ref_month_name = 'reference_month', date_start = '2024-10-01'):
    query = f"""
    select 
    {ref_month_name},
    count(*) as n_rows
    from {table_name}
    where {ref_month_name} >= '{date_start}'
    group by 1
    order by 1 
    """
    display(pandas_gbq.read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs'))


def create_query_check_last_n_rows_v2(table_name, ref_month_name = 'reference_month', date_start = '2024-10-01'):
    query = f"""
    select 
    {ref_month_name},
    count(*) as n_rows,
    count(distinct tam_id) as n_tam_id,
    count(distinct cod_muni) as n_cod_muni,
    from {table_name}
    where {ref_month_name} >= '{date_start}'
    group by 1
    order by 1 
    """
    display(pandas_gbq.read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs'))


from google.cloud import bigquery


def delete_by_reference_month(table_name, ref_month, ref_month_name='reference_month'):
    client = bigquery.Client(project='sfwthr2a4shdyuogrt3jjtygj160rs')
    
    query = f"""
    DELETE FROM {table_name}
    WHERE {ref_month_name} = '{ref_month}'
    """
    
    query_job = client.query(query)
    result = query_job.result()  # Waits for job to complete
    
    print(f"Deleted {query_job.num_dml_affected_rows} rows from {table_name}.")
    return query_job

agora = datetime.now()

#agora = datetime(2025, 6, 1)

comeco_mes = agora.replace(day=1)

#anomes = str(agora.year) + str(agora.month).zfill(2)

end_previous_month_str  = (comeco_mes - timedelta(days=1)).strftime('%Y-%m-%d')
#end_previous_month_str = '2025-06-30'
print(f'{end_previous_month_str}')


delete_previous = False

#%%
create_query_check_last_n_rows("""
    (select * except(ingestion_date), DATE(ingestion_date) ingestion_date from `dataplatform-prd.master_contact.v2_aux_tam_python_agrupados`)
                               """, ref_month_name='ingestion_date')



create_query_check_last_n_rows_v2('(select * from `dataplatform-prd.master_contact.v2_aux_tam_python_agrupados` where ingestion_date >= "2025-09-17")'
                               ,ref_month_name='reference_month')


create_query_check_last_n_rows_v2('(select * from `dataplatform-prd.master_contact.v3_aux_tam_python_agrupados` where ingestion_date >= "2025-09-17")'
                               ,ref_month_name='ingestion_date')


#%%

create_query_check_last_n_rows_v2('(select * from `dataplatform-prd.master_contact.v3_aux_tam_python_agrupados` where ingestion_date >= "2025-09-17")'
                               ,ref_month_name='reference_month')
#%%
if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v3_aux_tam_pos_python`', end_previous_month_str, ref_month_name='reference_date')

run_query('9_tam', dict_query={'ref_month': end_previous_month_str,
                               'ingestion_filter': "and ingestion_date >= '2025-09-29'"
                               }, update=True)

create_query_check_last_n_rows_v2('`dataplatform-prd.master_contact.v3_aux_tam_pos_python`', ref_month_name='reference_date')

#%%

from utils import download_data

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_tmp_mid_de42_acquirer_model`', end_previous_month_str, ref_month_name='reference_date')


df = download_data('10_acquirers_model', 
                   dict_query={'ref_month': end_previous_month_str,
                                                     #'version_city': '_v2',
                                                     }, update=True)

#df = df[df['reference_date'] == '2024-10-31']
from main_model_acquirer import apply_model


display(df)

df_final = apply_model(df, to_gbq=False)


df_final['outside_model'] = True
df_final['metadata__insert_time'] = pd.to_datetime('now')



print('Saving to Google Big Query')

import pandas_gbq

pandas_gbq.to_gbq(df_final[['tam_id', 'reference_date', 'acquirer']].drop_duplicates().reset_index(drop=True), 'dataplatform-prd.master_contact.v2_tmp_mid_de42_acquirer_model', project_id='sfwthr2a4shdyuogrt3jjtygj160rs',
                if_exists='append')


create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_tmp_mid_de42_acquirer_model`', ref_month_name='reference_date')

#%%

create_query_check_last_n_rows("""
    ( select * from                     
    `dataplatform-prd.master_contact.v2_tmp_mid_de42_acquirer_model`
   where acquirer<>'Cielo'
    )""", ref_month_name='reference_date')


#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_acquirers`', end_previous_month_str, ref_month_name='reference_date')

run_query('11_acquirers', dict_query={
    'ref_month': end_previous_month_str,
    'other_filter': ''
            }, update=True)


create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_acquirers`', ref_month_name='reference_date')

#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_presenca_online`', end_previous_month_str, ref_month_name='reference_date')

run_query('12_presenca_online', dict_query={'ref_month': end_previous_month_str}, update=True)

create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_presenca_online`', ref_month_name='reference_date')

#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_final_tam`', end_previous_month_str, ref_month_name='reference_date')

run_query('13.1_final_tam', dict_query={'ref_month': end_previous_month_str}, update=True)

create_query_check_last_n_rows_v2('`dataplatform-prd.master_contact.v2_aux_tam_final_tam`', ref_month_name='reference_date')
#%%

#if delete_previous:
 #   delete_by_reference_month('`dataplatform-prd.economic_research.aux_tam_all_docs_names`', end_previous_month_str, ref_month_name='reference_date')

#run_query('13.0_all_docs_names', dict_query={'ref_month': end_previous_month_str}, update=True)


#create_query_check_last_n_rows('`dataplatform-prd.economic_research.aux_tam_all_docs_names`', ref_month_name='reference_date')


#%%


if delete_previous:
    delete_by_reference_month('`dataplatform-prd.economic_research.v2_pre_tam_acquirer_completa`', end_previous_month_str, ref_month_name='reference_date')

run_query('14.0_pre_final_tam_acquirer', dict_query={'ref_month': end_previous_month_str}, update=True)
create_query_check_last_n_rows('`dataplatform-prd.economic_research.v2_pre_tam_acquirer_completa`', ref_month_name='reference_date')


#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.economic_research.v2_tam_acquirer_completa`', end_previous_month_str, ref_month_name='reference_date')

run_query('14.1_final_tam_acquirer', dict_query={'ref_month': end_previous_month_str}, update=True)
create_query_check_last_n_rows('`dataplatform-prd.economic_research.v2_tam_acquirer_completa`', ref_month_name='reference_date')





#%%


#end_previous_month_str = '2025-07-31'

if delete_previous:
#if True:
    delete_by_reference_month('`dataplatform-prd.master_contact.final_tam`', end_previous_month_str, ref_month_name='reference_date')
    delete_by_reference_month('`dataplatform-prd.master_contact.final_tam_acquirer`', end_previous_month_str, ref_month_name='reference_date')
    delete_by_reference_month('`dataplatform-prd.master_contact.final_tam_marketplace`', end_previous_month_str, ref_month_name='reference_date')
    delete_by_reference_month('`dataplatform-prd.master_contact.final_tam_acquirer_marketplace`', end_previous_month_str, ref_month_name='reference_date')


run_query('17_final_tam_and_mktplace', dict_query={'ref_month': end_previous_month_str}, update=True)


create_query_check_last_n_rows('`dataplatform-prd.master_contact.final_tam`', ref_month_name='reference_date')

create_query_check_last_n_rows('`dataplatform-prd.master_contact.final_tam_acquirer`', ref_month_name='reference_date')

create_query_check_last_n_rows('`dataplatform-prd.master_contact.final_tam_marketplace`', ref_month_name='reference_date')

create_query_check_last_n_rows('`dataplatform-prd.master_contact.final_tam_acquirer_marketplace`', ref_month_name='reference_date')


#%%



