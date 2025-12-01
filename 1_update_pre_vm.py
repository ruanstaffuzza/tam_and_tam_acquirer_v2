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


def create_query_check_last_n_rows_v0(table_name, ref_month_name = 'reference_month', date_start = '2024-11-01',
                                   custom_filter = ''):
    query = f"""
    select 
    {ref_month_name},
    count(*) as n_rows
    from {table_name}
    where {ref_month_name} >= '{date_start}'
    {custom_filter}
    group by 1
    order by 1 
    """
    display(pandas_gbq.read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs'))




agora = datetime.now()

comeco_mes = agora.replace(day=1)

end_previous_month_str  = (comeco_mes - timedelta(days=1)).strftime('%Y-%m-%d')

print(f'{end_previous_month_str}')

delete_previous = False


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


def delete_by_reference_month2(table_name, ref_months_str, ref_month_name='reference_month'):
    client = bigquery.Client(project='sfwthr2a4shdyuogrt3jjtygj160rs')
    
    query = f"""
    DELETE FROM {table_name}
    WHERE {ref_month_name} IN ({ref_months_str})
    """
    
    query_job = client.query(query)
    result = query_job.result()  # Waits for job to complete
    
    print(f"Deleted {query_job.num_dml_affected_rows} rows from {table_name}.")
    return query_job


#%%


import matplotlib.pyplot as plt
from pandas_gbq import read_gbq

def create_query_check_last_n_rows(table_name, ref_month_name='reference_month', 
                                   date_start='2024-11-01', custom_filter=''):
    
    # Monta query
    query = f"""
        select 
            {ref_month_name},
            count(*) as n_rows
        from {table_name}
        where {ref_month_name} >= '{date_start}'
        {custom_filter}
        group by 1
        order by 1 
    """

    # Lê dados

    print('Lendo os dados da tabela: ', table_name)
    
    if custom_filter != '':
        print('Com filtro adicional: ', custom_filter)


    df = read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')

    if df is None or df.empty:
        print("No data returned from the query.")
        return

    # Exibe tabela
    display(df)

    # Garante que colunas de data estejam em datetime
    df[ref_month_name] = pd.to_datetime(df[ref_month_name])

    # Identifica último mês disponível
    ultimo_mes = df[ref_month_name].max()

    # --- Plot ---
    plt.figure(figsize=(10, 5))
    plt.plot(df[ref_month_name], df['n_rows'], marker='o')
    plt.title("Número de linhas por mês\n`" + '.'.join(table_name.split('.')[1:]))
    plt.xlabel("Mês de referência")
    plt.ylabel("n_rows")

    # Personaliza rótulos do eixo X (destaca em negrito o último mês)
    x_labels = [x.strftime('%m/%y') for x in df[ref_month_name]]
    plt.xticks(df[ref_month_name], x_labels, rotation=45)
    ax = plt.gca()
    for label in ax.get_xticklabels():
        if label.get_text() == ultimo_mes.strftime('%m/%y'):
            label.set_fontweight('bold')

    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

create_query_check_last_n_rows('`dataplatform-prd.economic_research.tam_acquirer_exclusivos`', ref_month_name='reference_date')


#%% Update subs_asterisk_city table


# check if 'geo_places' is updated
table = '`dataplatform-prd.economic_research.geo_places`'
col_ref  = 'reference_month'
df = pandas_gbq.read_gbq(f'select max({col_ref}) as max_ref from {table}')
print(df)
if df is None:
    raise ValueError(f'Table {table} not found or empty.')

if df.values[0][0].strftime('%Y-%m-%d') == end_previous_month_str:
    print(f'Tabela {table} atualizada')

else:
    print(f'Tabela {table} não atualizada')


#delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`', '2024-08-31')
#delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk`', '2024-08-31')

#%%


if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`', end_previous_month_str)
    

run_query('.1_subs_asterisk_city', dict_query={'ref_month': end_previous_month_str})

create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`')


#%%


create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`',
                                       
                                       custom_filter="and subs_asterisk in ('Vindi')"
                                       )



create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`',
                                       
                                       custom_filter="and subs_asterisk in ('delete_asterisk')"
                                       )



#%% Update get_document_master


        
run_query('4.0_cpf_by_city')

run_query('4.1_names_documents')

run_query('5_get_mp', dict_query={'ref_month': end_previous_month_str})

create_query_check_last_n_rows('`dataplatform-prd.master_contact.clients_mercado_pago`')



#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_pre_acquirer`', end_previous_month_str)

run_query('.5.1_pre_acquirer', 
         dict_query={'ref_month': end_previous_month_str,
                      })
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_pre_acquirer`')


#%% 

run_query('.6.0_cross_company', dict_query={'ref_month': end_previous_month_str})
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_temp_cross_company`', 
                               ref_month_name='reference_date')




#%%
if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_get_document_master`', end_previous_month_str)

run_query('.6.1_get_document_master', dict_query={'ref_month': end_previous_month_str,
                                               })

create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_get_document_master`')

#create_query_check_last_n_rows('`dataplatform-prd.master_contact.aux_tam_get_document_master`')

#%% 

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_big_mmhid`', end_previous_month_str)

run_query('.7_big_mmhid', dict_query={'ref_month': end_previous_month_str})
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_big_mmhid`')




#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_temp_ruan_autorizador`', end_previous_month_str)


run_query('.8.0_autorizador_ton', dict_query={'ref_month': end_previous_month_str})
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_temp_ruan_autorizador`')

#%%                   

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_ton_auth`', end_previous_month_str,
                              ref_month_name='reference_date')

run_query('.8.1_get_ton_auth', dict_query={'ref_month': end_previous_month_str})
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_ton_auth`', ref_month_name='reference_date')


#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_ton_mastercard`', end_previous_month_str, ref_month_name='reference_date')

run_query('.8.2_get_ton_mastercard', dict_query={'ref_month': end_previous_month_str})
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_ton_mastercard`', ref_month_name='reference_date')

#%%
if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_ton_match_part1`', end_previous_month_str, ref_month_name='reference_date')

run_query('.8.3_merge_ton_part1', dict_query={'ref_month': end_previous_month_str})
#create_query_check_last_n_rows('`dataplatform-prd.master_contact.aux_tam_ton_match_part1`', ref_month_name='reference_date')
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_ton_match_part1`', ref_month_name='reference_date')


#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_ton_match_part2`', end_previous_month_str, ref_month_name='reference_date')


run_query('.8.4_merge_ton_part2', dict_query={'ref_month': end_previous_month_str})
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_ton_match_part2`', ref_month_name='reference_date')


#%%

if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_ton_to_mmhid`', end_previous_month_str, ref_month_name='reference_date')
run_query('.8.5_merge_ton_final', dict_query={'ref_month': end_previous_month_str})
create_query_check_last_n_rows('`dataplatform-prd.master_contact.v2_aux_tam_ton_to_mmhid`', ref_month_name='reference_date')


#%% final nomes







if delete_previous:
    delete_by_reference_month('`dataplatform-prd.master_contact.v3_aux_tam_final_nomes`', end_previous_month_str)

run_query('.8.6_final_nomes_new', dict_query={'ref_month': end_previous_month_str,
                                              'add_filter': '' 
                                              })

#create_query_check_last_n_rows('`dataplatform-prd.master_contact.aux_tam_final_nomes`')

create_query_check_last_n_rows('`dataplatform-prd.master_contact.v3_aux_tam_final_nomes`')


#%%
#query_job = delete_by_reference_month('`dataplatform-prd.master_contact.v2_aux_tam_python_agrupados`', end_previous_month_str)



#create_query_check_last_n_rows('(select * from `dataplatform-prd.master_contact.v3_aux_tam_final_nomes` where subs_asterisk = "Ton")')


create_query_check_last_n_rows('`dataplatform-prd.master_contact.v3_aux_tam_final_nomes`',
                                        custom_filter="and subs_asterisk in ('Ton')"
                                        )

#%%



#create_query_check_last_n_rows('(select * from `dataplatform-prd.master_contact.v3_aux_tam_final_nomes` where subs_asterisk = "Pagarme")')


create_query_check_last_n_rows('`dataplatform-prd.master_contact.v3_aux_tam_final_nomes`',
                                        custom_filter="and subs_asterisk in ('Pagarme')"
                                        )