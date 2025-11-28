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


query = """
select distinct de42 
from `dataplatform-prd.master_contact.tam_acquirer_pre_previsao` a
left join `dataplatform-prd.master_contact.de42_acquirer_model` b using(de42)
where b.de42 is null

"""




import pandas_gbq

df = pandas_gbq.read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')

if df is None or df.shape[0]==0:
    raise ValueError('No new de42 to predict')


df['tam_id'] = '320213301702202509'
df['reference_date'] = pd.to_datetime('2025-09-30')

#df = pd.read_parquet('data/10_acquirers_model.parquet')
#df = df.drop(columns=['lower_name'])

from main_model_acquirer import apply_model


display(df)

df_final = apply_model(df, to_gbq=False)


df_final['outside_model'] = True
df_final['metadata__insert_time'] = pd.to_datetime('now')


df_final = df_final.drop(columns=[ 'tam_id', 'reference_date'])

df_final
#%%
print('Saving to Google Big Query')
pandas_gbq.to_gbq(df_final[['de42', 'acquirer', 'metadata__insert_time']], 
                  'dataplatform-prd.master_contact.de42_acquirer_model', 
                  project_id='sfwthr2a4shdyuogrt3jjtygj160rs',
                if_exists='replace'
                )

