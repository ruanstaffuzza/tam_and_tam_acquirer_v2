
from matplotlib import pyplot as plt
import pandas as pd


DATA_DIR = 'data'
RESULTS_DIR = 'results'
DATA_RESULTS_DIR = r"results\data"

def save_data_results(df, file_name, file_format='parquet', **kwargs):
    file_path = fr"{DATA_RESULTS_DIR}\{file_name}.{file_format}"
    eval(f'df.to_{file_format}(file_path,**kwargs)') 
    print(f"File successfully recorded on {file_path}")

def read_data_results(file_name, file_format='parquet', **kwargs):
    file_path = fr"{DATA_RESULTS_DIR}\{file_name}.{file_format}"
    return eval(f'pd.read_{file_format}(file_path,**kwargs)')


def print_column_info(df: pd.DataFrame, to_clipboard: bool = False):
    """
    Prints the column names and their data types for a given pandas DataFrame with equal spacing.

    Parameters:
    df (pd.DataFrame): The DataFrame for which to print column info.
    to_clipboard (bool): If True, the text is copied to the clipboard.
    """
        
    space = max(len(column) for column in df.columns) + 2
    print(f"## {'Column Name':<{space}}{'Data Type':<{space}}")
    for column in df.columns:
        print(f"# {column:<{space}}{str(df[column].dtype):<{space}}")
    if to_clipboard:
        text = []
        text.append(f"## {'Column Name':<{space}}{'Data Type':<{space}}")
        for column in df.columns:
            text.append(f"# {column:<{space}}{str(df[column].dtype):<{space}}")
        
        df = pd.DataFrame(text)
        df.to_clipboard(index=False, header=False)
        print('-'*50)
        print('Text copied to clipboard')



def pretty_qcut(x, bins:int=5, round_to:int=0, break_at_zero:bool=False):
    # returns a pretty quantile cut, with labels roundings to the nearest integer or float
    import string
    import numpy as np
    import pandas as pd
    if isinstance(bins, int):
        bins = x.quantile(np.linspace(0, 1, bins + 1)).round(round_to).drop_duplicates()
        if break_at_zero and (bins[0] < 0) and (bins.values[-1] > 0):
            bins = bins.append(pd.Series(0), ignore_index=True).sort_values().drop_duplicates()
        # bins[0] = x.min()
        # bins[-1] = x.max()
        bins = bins.to_list()
        
    return bins


def download_new():
    import subprocess
    subprocess.call('start /wait python download_new.py', shell=True)


def read_data(filename, file_format='parquet'):
    import os.path
    filename_format = os.path.splitext(filename)[1][1:]
    file_format = file_format.replace('.', '')
    
    if not filename_format in ['', 'sql']:
        file_format=filename_format
    elif filename_format=='sql': 
        filename = filename[:-4]
        filename = fr"{filename}.{file_format}"
    else: 
        filename = fr"{filename}.{file_format}"
    
    filename = fr"{DATA_DIR}\{filename}"
    return eval(f'pd.read_{file_format}(filename)')

def weighted_mean(x, **kws):
    import numpy as np
    return np.sum(np.real(x) * np.imag(x)) / np.sum(np.imag(x))

def despine(axes):
    for ax in axes.flat:
        ax.spines['right'].set_color('none')
        ax.spines['top'].set_color('none')

def despine_ax(ax):
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')

def format_month(g):
  import matplotlib.dates as mdates
  import matplotlib.ticker as ticker
  fmt= lambda x,pos: mdates.DateFormatter('%b/%y')(x,pos).lower()    
  for ax in g.axes.flat:
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(fmt))


def format_month_ax(ax, bymonth=(1, 4, 7, 10)):
  import matplotlib.dates as mdates
  import matplotlib.ticker as ticker
  fmt= lambda x,pos: mdates.DateFormatter('%b/%y')(x,pos).lower()    
  ax.xaxis.set_major_formatter(ticker.FuncFormatter(fmt))
  ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=bymonth))

def make_footnote(notas, x = 0.02, 
                  y = 0, 
                  fontsize=10):
  
  #fig = plt.gcf()
  #_, fig_height = fig.get_size_inches()
 
  len_notes = len(notas)

  notas = [n for n in notas if n]
  notas = [f'{n}' for i, n in enumerate(notas)]
  notas = '\n'.join(notas)

  plt.figtext(x, y, notas, fontstyle='italic', ha="left", fontsize=fontsize,
              va='top')

def wrap_labels(ax, width, break_long_words=False):
    import textwrap
    labels = []
    for label in ax.get_xticklabels():
        text = label.get_text()
        labels.append(textwrap.fill(text, width=width,
                      break_long_words=break_long_words))
    ax.set_xticklabels(labels, rotation=0)


def save_plots(file_name, formato=".png", dpi=300, bbox_inches="tight", close=False):
    import os
    folder_plots = 'results\plots'
    
    print('Salvando: ' + file_name + formato)
    plt.savefig(os.path.join(folder_plots,file_name + formato), dpi=dpi, bbox_inches=bbox_inches)

    if close:
        plt.close(plt.gcf())
    else:
        plt.show()

def high_contrast_color_palette():
    import seaborn as sns
    colors = pal = ['#e6194B', '#3cb44b', '#ffe119', '#4363d8', '#f58231', '#911eb4', '#42d4f4', 
       '#f032e6', '#bfef45', '#fabed4', '#469990', '#dcbeff', '#9A6324', '#fffac8', 
       '#800000', '#aaffc3', '#808000', '#ffd8b1', '#000075', '#a9a9a9', '#ffffff', 
       '#000000'] #Sasha Trubetskoy style
    
    return sns.color_palette(colors)


def conco_to_palette(selected_conco):
  colors = [(0.12156862745098039, 0.4666666666666667, 0.7058823529411765),
  (1.0, 0.4980392156862745, 0.054901960784313725),
  (0.17254901960784313, 0.6274509803921569, 0.17254901960784313),
  (0.8392156862745098, 0.15294117647058825, 0.1568627450980392),
  (0.5803921568627451, 0.403921568627451, 0.7411764705882353),
  (0.5490196078431373, 0.33725490196078434, 0.29411764705882354),
  (0.8901960784313725, 0.4666666666666667, 0.7607843137254902),
  (0.4980392156862745, 0.4980392156862745, 0.4980392156862745),
  '#ffd404', #(0.7372549019607844, 0.7372549019607844, 0.13333333333333333),
  (0.09019607843137255, 0.7450980392156863, 0.8117647058823529),
  '#a6c356',
  '#005360',
  '#00c278',
  '#004033',
  '#0FCC7D',
  '#010101',
  '#64c832',
  '#235158']
  dict_color = dict(zip(['azul',
                        'laranja',
                        'verde',
                        'vermelho',
                        'lilas',
                        'marrom',
                        'rosa',
                        'cinza',
                        'amarelo', 
                        'cyan', 
                         'verde-limao',
                         'dark-cyan',
                         'verde-stone1',
                         'verde-stone2',
                         'verde-stone3',
                         'preto',
                         'verde-sicredi',
                         'verde-sipag'], colors))

  dict_cores_concorrente = {'CIELO': 'azul', 
                            #'PAGSEGURO': 'amarelo',
                            'GRANITO': 'cinza',
                            'REDE': 'laranja', 
                            'GETNET': 'vermelho', 
                            'SAFRAPAY': 'marrom',
                            'SAFRA': 'marrom',
                            'MERCADO PAGO': 'cyan',
                            'SUMUP': 'cinza',
                            'TICKET': 'cinza',
                            'SEM INFO ADQUIRENTE': 'lilas',
                            'INFINITEPAY': 'verde-limao',
                            'INFINITE PAY': 'preto',
                            'SIPAG': 'dark-cyan',
                            'SIPAG/SICREDI': 'dark-cyan',
                            'STONE': 'verde-stone2',
                            'TON': 'verde-stone3',
                            'TON (ULTRA)': 'verde-stone3',
                            'OUTRAS': 'lilas',
                            'SICREDI': 'verde-sicredi',
                            'SICOOB': 'verde-sipag',
                            'SIPAG': 'verde-sipag',
                            'BIN': 'preto',
                            'VERO/BANRISUL': 'cyan',
                            'VERO': 'cyan',
                            'TICKET LOG': 'cinza',
                            'QUALQUER UMA': 'lilas'}
  
  color_dict = {
    'BANCO DO BRASIL': "#fbfb33", #"#F9DD16",  # Already in hex format
    "Banco Inter": "#FF8C00",  # orange3 equivalent
    "Bradesco": "#CC092F",  # Already in hex format
    "BTG": "#000080",  # navy equivalent
    "C6 Bank": "#000000",  # black
    "Caixa": "#1C60AB",  # Already in hex format
    "Itaú": "#F28500",  # Already in hex format
    "Mercado Pago": "#00BFFF",  # deepskyblue2 equivalent
    "Nubank": "#6d2077",  # Already in hex format
    'NeoBanks': "#6d2077",
    "Pagseguro": "#f3c326",  # yellow
    #"Safra": "#8B6914",  # gold4 equivalent
    "Santander": "#EC0000",  # Already in hex format
    "Sicoob e Sicredi": "#00AE9D",  # Already in hex format
    "Outros": "#708090"  # azure4 equivalent
    }
  color_dict_upper = {key.upper(): value for key, value in color_dict.items()}
  
  final_dict = {k: dict_color[v] for k, v in dict_cores_concorrente.items()}
  final_dict.update(color_dict_upper)
  
  return [final_dict[x.upper()] for x in selected_conco]






def create_pallete(init_color, final_color, n):
  from colour import Color
  colors = list(c.hex for c in Color(init_color).range_to(Color(final_color),n))
  return colors


def create_pallete_stone(n, style=1, invert=False):
  if style == 1:
    dark, light = ('#004033', '#0FCC7D')
  if style == 2:
    dark, light = ('#005434', '#32ffb1')
  if style == 3:
    dark, light = ('#00734D', '#42EC9A')

  init, final = (dark, light) if not invert else (light, dark)
  return create_pallete(init, final, n)

def pretty_qcut_v2(x, bins:int=5, round_to:int=0, break_at_zero:bool=False):
    # returns a pretty quantile cut, with labels roundings to the nearest integer or float
    import string
    import numpy as np
    import pandas as pd
    def def_bin(i, bins, round_to):
        if i == 0 and round_to <= 0:
            return f"{string.ascii_uppercase[i]}. [{bins[i]:.0f},{bins[i+1]:.0f}]"
        elif i == 0 and round_to > 0:
            return f"{string.ascii_uppercase[i]}. [{bins[i]:.{round_to}f},{bins[i+1]:.{round_to}f}]"
        elif i > 0 and round_to <= 0:
            return f"{string.ascii_uppercase[i]}. ({bins[i]:.0f},{bins[i+1]:.0f}]"
        else:
            return f"{string.ascii_uppercase[i]}. ({bins[i]:.{round_to}f},{bins[i+1]:.{round_to}f}]"


    if isinstance(bins, int):
        bins = x.quantile(np.linspace(0, 1, bins + 1)).round(round_to).drop_duplicates()
        if break_at_zero and (bins[0] < 0) and (bins.values[-1] > 0):
            bins = bins.append(pd.Series(0), ignore_index=True).sort_values().drop_duplicates()
        # bins[0] = x.min()
        # bins[-1] = x.max()
        bins = bins.to_list()
        
    return bins
    labels = [def_bin(i, bins, round_to) for i,j in enumerate(bins[:-1])]

    return pd.cut(x, bins=bins, labels=labels, duplicates='drop', include_lowest=True)



def create_lags(df, time : str, lags = 1, ident : list =['cpf_cnpj'],  vars=None,
    drop_max_lags=True, fillna:bool=True) -> pd.DataFrame: 
# Creates lags of a variables given in vars. Returns the same dataframe with the new variables.
# time is the column with the time variable (for now, only month is supported)
# lags is the list of lags to create. A single 
# If drop_max_lags is True, dates which are before 
#   add_month(min(date)+ max(lags)) are dropped .
# If fillna is True, the lags will be filled with the 0 values.

    def add_month(date, n):
        return (pd.to_datetime(date) + pd.DateOffset(months=n))

    def drop_lags(df, time, max_lag):
    # drops rows which correspond to the last lags
        min_date = add_month(df[time].min(), max_lag)
        return df.query(f"{time} > @min_date")

    def create_single_lag(df, time:str, lag:int = 1, ident : list =['cpf_cnpj'],  
    vars:list=None, fillna:bool=True) -> pd.DataFrame:
    # function that creates a single lag. Returns the same dataframe with the new variables.
    # variables description are the same as parent function
        if vars is None:
            vars = df.columns.drop(time).drop(ident)
        df_merge = df[ident + [time] + vars].copy()
        df_merge[time] = add_month(df_merge[time], lag)
        to_new_var_names_dict = {var: var+'_L'+str(lag) for var in vars}
        df_merge = df_merge.rename(columns=to_new_var_names_dict)
        df = df.merge(df_merge, on=[time] + ident, how='left')
        if fillna:
            new_var_names = to_new_var_names_dict.values()
            df.loc[:,new_var_names] = df[new_var_names].fillna(0)
        return df

    df[time] = pd.to_datetime(df[time])

    if isinstance(lags,int):
        df = create_single_lag(df, time, lags, ident, vars, fillna)
        if drop_max_lags:
            df = drop_lags(df, time, lags)
        return df
    elif isinstance(lags,list):
        for lag in lags:
            df = create_single_lag(df, time, lag, ident, vars, fillna)
        if drop_max_lags:
            df = drop_lags(df, time, max(lags))
        return df
    else:
        raise ValueError('lags must be int or list')

# function to get current date and time
def get_current_time():
    from datetime import datetime
    return "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]"


def read_gbq_(query):
    import pandas_gbq
    project_id='sfwthr2a4shdyuogrt3jjtygj160rs'
    print(f'{get_current_time()} Getting dataset from BQ...')

    df = pandas_gbq.read_gbq(query, progress_bar_type='tqdm',
        project_id=project_id)
    
    if df is None or len(df) == 0:
        raise ValueError('Query returned no data. Please check the query.')
    
    print(f'{get_current_time()} Dataset successfully retrieved from BQ')
    print('Dataframe shape:', df.shape)
    return df

def read_gbq_from_template(template_query, dict_query=None):
    query = template_query
    if dict_query:
        from jinja2 import Template
        # Reads a query from a template and returns the query with the variables replaced
        # template_query: query as string, may use jinja2 templating
        # dict_query: dictionary of query parameters, to render from the template with jinja2
        query = Template(template_query).render(dict_query)
        return read_gbq_(query)
    return read_gbq_(query)


def read_text(file_name, encoding='utf-8'):
    with open(file_name, 'r', encoding=encoding) as f:
        return f.read()

def download_data(query_name,  file_format='parquet', update=False,
                  dict_query = {}, **kwargs) -> pd.DataFrame:


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

    from os.path import exists
    file_path = fr"{DATA_DIR}\{query_name[:-4]}.{file_format}"
    query_path = fr"queries\{query_name}"

    template_query = read_text(query_path)

    if update or not exists(file_path):
        df = read_gbq_from_template(template_query, dict_query)
            
        for c in df.dtypes[df.dtypes=='dbdate'].index:
            df[c] = df[c].astype('datetime64[ns]')
        
        z = 'index=False' if file_format=='csv' else ''
        file_format = 'excel' if file_format=='xlsx' else file_format
        print(file_format)
        if len(df)>0:
            eval(f'df.to_{file_format}(file_path,{z})') 
            print(f'{get_current_time()} File successfully recorded on ', file_path) 
        return df
    else:
        return eval(f'pd.read_{file_format}(file_path,**kwargs)')

def download_data2(query_name,  file_format='parquet', update=False,
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
    
    x = list(dict_query.values())[0]
    from os.path import exists
    file_path = fr"{DATA_DIR}\{x}_{query_name[:-4]}.{file_format}"
    query_path = fr"queries\{query_name}"

    template_query = read_text(query_path)

    if update or not exists(file_path):
        df = read_gbq_from_template(template_query, dict_query)
            
        for c in df.dtypes[df.dtypes=='dbdate'].index:
            df[c] = df[c].astype('datetime64[ns]')
        
        z = 'index=False' if file_format=='csv' else ''
        file_format = 'excel' if file_format=='xlsx' else file_format
        print(file_format)
        if len(df)>0:
            eval(f'df.to_{file_format}(file_path,{z})') 
            print(f'{get_current_time()} File successfully recorded on ', file_path) 
        return df



def sample_(df, size):
    return df.sample(size)
    
def sample_by_(df, by, size):
    return df[df[by].isin(df[by].sample(size))]



def sample(dataframe=None, size=1000, by=None):
    #if dataframe is None:
        #dataframe = df.copy()
    if not by: 
        return sample_(dataframe, size)
    else:
        return sample_by_(dataframe, by, size)
    




def cross_join(df1, df2):
    df1['key_jgdhgdkihgdhvgfiurgiurh'] = 0 
    df2['key_jgdhgdkihgdhvgfiurgiurh'] = 0
    df = df1.merge(df2, on='key_jgdhgdkihgdhvgfiurgiurh', how='outer')
    del df['key_jgdhgdkihgdhvgfiurgiurh']
    return df


def write_text(file_path, string, encoding='utf-8'):
    with open(file_path, 'w', encoding=encoding) as f:
        f.write(string)
    
    
def save_query(query_name, string):
    if not query_name.endswith('.sql'):
        query_name += '.sql'
    write_text(f'queries\{query_name}', string)
 


def read_query(template_query, dict_query = {}, **kwargs):

    df = read_gbq_from_template(template_query, dict_query)
            
    for c in df.dtypes[df.dtypes=='dbdate'].index:
        df[c] = df[c].astype('datetime64[ns]')
        
    return df


def filter_on_multiple_columns(
        df: pd.DataFrame, 
        df_filter: pd.DataFrame, 
        on=None):
    if not on: 
        on = df_filter.columns
    df_filter = df_filter.drop_duplicates(on)
    df = df.merge(df_filter[on], how='inner', on=on)
    return df


# código idiota que eu preciso salvar para a posteridade
# Talvez ele ajude a descobrir onde estão as credenciais do gsutil
# import google.auth
# credentials, project = google.auth.default()




def download_data_custom(query_name, file_name, file_format='parquet', update=False,
                  dict_query = {}, **kwargs):

    if not query_name.endswith('.sql'):
        query_name += '.sql'
    
    if file_format.startswith('.'):
        file_format = file_format[1:]

    from os.path import exists
    file_path = fr"{DATA_DIR}\{file_name}.{file_format}"
    query_path = fr"queries\{query_name}"

    template_query = read_text(query_path)

    if update or not exists(file_path):
        df = read_gbq_from_template(template_query, dict_query)
            
        for c in df.dtypes[df.dtypes=='dbdate'].index:
            df[c] = df[c].astype('datetime64[ns]')
        
        z = 'index=False' if file_format=='csv' else ''
        file_format = 'excel' if file_format=='xlsx' else file_format
        print(file_format)
        if len(df)>0:
            eval(f'df.to_{file_format}(file_path,{z})') 
            print(f'{get_current_time()} File successfully recorded on ', file_path) 
        return df
