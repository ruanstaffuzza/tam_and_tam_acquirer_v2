#%%
from datetime import datetime, timedelta
import os

CREATE_TABLES_QUERIES_DIR = fr"queries\create tables"

from IPython.display import display
import pandas as pd

# function to get current date and time
def get_current_time():
    from datetime import datetime
    return "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]"

def read_gbq_(query):
    project_id= 'sfwthr2a4shdyuogrt3jjtygj160rs'
    print(f'{get_current_time()} Getting dataset from BQ...')
    return pd.read_gbq(query, progress_bar_type='tqdm',
        project_id=project_id, max_results=1)

def read_gbq_from_template(template_query, dict_query):
    query = template_query
    if dict_query:
        from jinja2 import Template
        # Reads a query from a template and returns the query with the variables replaced
        # template_query: query as string, may use jinja2 templating
        # dict_query: dictionary of query parameters, to render from the template with jinja2
        query = Template(template_query).render(dict_query)
    return read_gbq_(query)


def read_text(file_name, encoding='utf-8'):
    with open(file_name, 'r', encoding=encoding) as f:
        return f.read()

def run_query(query_name,  file_format='parquet', update=False,
                  dict_query = {}, **kwargs):


    #(file_path, template_query, force_query=False,):
    # Get data either from file_path or from BQ
    # file_path: path to file
    # template_query: query as string, may use jinja2 templating
    # force_query: force query to be executed
    # make_dir: make directory if it doesn't exist
    # dict_query: dictionary of query parameters, to render from the template with jinja2
    # kwargs: additional parameters to pass to pandas.read functions
    if not query_name.endswith('.sql'):
        query_name += '.sql'
    
    if file_format.startswith('.'):
        file_format = file_format[1:]

    template_query = read_text(os.path.join(CREATE_TABLES_QUERIES_DIR, query_name))


    if update:
        print(f'Running query {query_name}...')
        df = read_gbq_from_template(template_query, dict_query)
        print(f'Query {query_name} executed.')
        return df


def create_query_check_n_rows(table_name, ref_month, ref_month_name = 'reference_month'):
    query = f"""
    select count(*) as n_rows from {table_name} where {ref_month_name} = '{ref_month}'"""
    return pd.read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')

def create_query_check_last_n_rows(table_name, ref_month_name = 'reference_month'):
    query = f"""
    select 
    {ref_month_name},
    count(*) as n_rows
    from {table_name}
    where {ref_month_name} >= '2024-03-01'
    group by 1
    order by 1 
    """
    return pd.read_gbq(query, project_id='sfwthr2a4shdyuogrt3jjtygj160rs')


agora = datetime.now()

comeco_mes = agora.replace(day=1)

anomes = str(agora.year) + str(agora.month).zfill(2)


end_previous_month_str  = (comeco_mes - timedelta(days=1)).strftime('%Y-%m-%d')
print(f'{anomes} -  {end_previous_month_str}')

#%%

run_query('15_make_addressable_market', dict_query={'ref_month': end_previous_month_str}, update=True)

#%%

def get_all_table_names(project_id='sfwthr2a4shdyuogrt3jjtygj160rs',
                        dataset_id='dataplatform-prd.master_contact'
                        ):
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    tables = client.list_tables(dataset_id)
    return [table.table_id for table in tables]

#s = get_all_table_names(dataset_id='dataplatform-prd.addressable_market')

s = ['mktshare', 'tam', 'tam_acquirer', 'tam_geocoded', 'tam_tier']

#%%
for table in s:
    print(table)
    table_id = f'`dataplatform-prd.addressable_market.{table}`'
    display(create_query_check_last_n_rows(table_id, ref_month_name='reference_date'))



#create_query_check_last_n_rows('`dataplatform-prd.master_contact.aux_tam_presenca_online`', ref_month_name='reference_date')
