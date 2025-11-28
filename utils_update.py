
import os

CREATE_TABLES_QUERIES_DIR = fr"queries\create tables"

import pandas_gbq

# function to get current date and time
def get_current_time():
    from datetime import datetime
    return "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]"

def read_gbq_(query):
    project_id= 'sfwthr2a4shdyuogrt3jjtygj160rs'
    print(f'{get_current_time()} Getting dataset from BQ...')
    return pandas_gbq.read_gbq(query, progress_bar_type='tqdm',
        project_id=project_id, max_results=1)

from jinja2 import Environment, StrictUndefined, meta, Template

def _template_vars(template_query: str) -> set[str]:
    """Retorna o conjunto de variáveis usadas no template Jinja2."""
    env = Environment()
    ast = env.parse(template_query)
    vars_in_template = meta.find_undeclared_variables(ast)
    # ignora literais/aliases comuns que podem aparecer
    return {v for v in vars_in_template if v.lower() not in {"true", "false", "none"}}

def _validate_template_keys(template_query: str, dict_query: dict, *, allow_extra_keys: bool = False):
    vars_in_template = _template_vars(template_query)
    keys_in_dict = set(dict_query.keys())

    missing_in_dict = vars_in_template - keys_in_dict           # estão no template mas faltam no dict
    extra_in_dict = keys_in_dict - vars_in_template             # estão no dict mas não são usadas no template

    msgs = []
    if missing_in_dict:
        msgs.append(f"faltando no dict_query: {sorted(missing_in_dict)}")
    if not allow_extra_keys and extra_in_dict:
        msgs.append(f"não usadas no template: {sorted(extra_in_dict)}")

    if msgs:
        raise KeyError("Inconsistência entre template e dict_query: " + "; ".join(msgs))

def read_gbq_from_template(template_query, dict_query):
    # 1) valida chaves antes de renderizar
    _validate_template_keys(template_query, dict_query, allow_extra_keys=False)

    # 2) renderização estrita (falha se algo ficar sem valor)
    env = Environment(undefined=StrictUndefined)
    query = env.from_string(template_query).render(dict_query or {})

    return read_gbq_(query)

def read_text(file_name, encoding='utf-8'):
    with open(file_name, 'r', encoding=encoding) as f:
        return f.read()

def run_query(query_name,  file_format='parquet',
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

    print(f'Running query {query_name}...')
    df = read_gbq_from_template(template_query, dict_query)
    print(f'Query {query_name} executed.')
    return df
