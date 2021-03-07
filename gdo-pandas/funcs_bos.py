import pandas as pd
import numpy as np
import gc, csv


def get_files_names(only_not_imported=False):
    '''retorna uma lista com os nomes dos arquivos de BOS)'''
    import os  

    dir = os.getcwd()
    bos_files_names = os.listdir(dir+'/files/BOS')

    if only_not_imported:
        imported_files_bos = pd.read_sql_table('tbl_imported_files_bos', 'sqlite:///gdo.db')
        bos_files_names = [file_name for file_name in bos_files_names if file_name not in imported_files_bos['0'].values ]
    
    return bos_files_names


def get_classificadores():
    classificadores = pd.read_csv('files/classificadores.csv', sep=';')    
    classificadores.set_index( classificadores['MUNICIPIO'] + " " + classificadores['VALIDADOR_TIPO'] + " " + classificadores['VALIDADOR'], inplace=True)    
    classificadores.fillna('', inplace=True)
    classificadores = classificadores.reset_index().drop_duplicates('index').set_index('index')
    return classificadores

def filter_23(df):
    return df[
        df['MUNICIPIO'].isin([
            'DIVINOPOLIS',
            'ITAUNA',
            'ITATIAIUCU',
            'CARMO DO CAJURU',
            'SAO GONCALO DO PARA',
            'CLAUDIO'
        ])
    ]

def get_nbos_on_db():
    import sqlite3, itertools
    with sqlite3.connect('gdo.db') as conn:
        cursor = conn.cursor()
        nbos_in_db = cursor.execute('SELECT "RAT.NUM_ATIVIDADE" from "tbl_bos"').fetchall()
    return list(itertools.chain(*nbos_in_db))

    
def read_files(files_names):    
    if len( files_names) < 1:
        raise Exception('Não há arquivos csv novos para serem inseridos no banco de dados.')
    for i in range( len(files_names) ):    
        if i == 0:        
            df = pd.read_csv('files/BOS/' + files_names[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
            df = df.applymap(lambda x: x.strip() if type(x) == str else x)
            df['MUNICIPIO'] = df['MUNICIPIO'].astype('category')
            df = filter_23(df)            
        else:
            df_aux = pd.read_csv('files/BOS/' + files_names[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
            df_aux = df_aux.applymap(lambda x: x.strip() if type(x) == str else x)            
            df_aux['MUNICIPIO'] = df_aux['MUNICIPIO'].astype('category')
            df_aux = filter_23(df_aux)            
            df = pd.concat([df, df_aux])
    return df


def data_bos_processing(df_bos):
    df_bos.drop_duplicates(subset='RAT.NUM_ATIVIDADE', keep='last', inplace=True)

    s_dta_in = df_bos['DTA_INICIO'] + " " + df_bos['HRA_INICIO']
    df_bos.loc[:,'DTA_HRA_INICIO_DT'] = pd.to_datetime( s_dta_in, format='%d/%m/%Y %H:%M', errors='coerce')
    del s_dta_in

    s_dta_ter = df_bos.loc[:,'DTA_TERMINO'] + " " + df_bos.loc[:,'HRA_TERMINO']
    df_bos.loc[:,'DTA_HRA_TERMINO_DT'] = pd.to_datetime( s_dta_ter, format='%d/%m/%Y %H:%M', errors='coerce')
    del s_dta_ter

    df_bos.loc[:,'TEMPO_DT'] = df_bos['DTA_HRA_TERMINO_DT'] - df_bos['DTA_HRA_INICIO_DT']

    df_bos.loc[:,'TEMPO_INT'] = df_bos['TEMPO_DT'].dt.total_seconds() / 60

    df_bos.loc[:,'DIA'] = df_bos.loc[:,'DTA_HRA_INICIO_DT'].dt.day
    
    df_bos.loc[:,'MES'] = df_bos.loc[:,'DTA_HRA_INICIO_DT'].dt.month

    df_bos.loc[:,'ANO'] = df_bos.loc[:,'DTA_HRA_INICIO_DT'].dt.year

    df_bos.loc[:,'DEZENA'] = np.select([
        df_bos['DTA_HRA_INICIO_DT'].dt.day <= 10,
        df_bos['DTA_HRA_INICIO_DT'].dt.day <= 20
    ],[1,2], default=3)


    for field in ['TEMPO_INT', 'DIA', 'MES', 'ANO', 'DEZENA']:
        df_bos[field].fillna(0, inplace=True)
        df_bos[field] = df_bos[field].astype('int16')

    cols_classif = [
        'MUNICIPIO',
        'LOGRADOURO',
        'DES_ENDERECO',
        'COMPLEMENTO_ENDERECO',
        'NOME_BAIRRO',
        'LOGRADOURO2',
        'DES_ENDERECO2'
    ]
    df_bos[cols_classif] = df_bos[cols_classif].apply(lambda col: col.str.upper())
    df_bos[cols_classif] = df_bos[cols_classif].fillna('')

    return df_bos


def classifica_setor(row, classificadores):
    mun = row['MUNICIPIO']
    if mun == 'CLAUDIO':        
        return 'CLAUDIO'
    elif mun == 'ITATIAIUCU':
        return 'LOURDES/ITATIAIUCU'
    elif mun in ('CARMO DO CAJURU', 'SAO GONCALO DO PARA'):             
        return 'CARMO DO CAJURU/SAO GONCALO DO PARA'    
    elif ( mun + " N_RAT " + row['RAT.NUM_ATIVIDADE'] ) in classificadores.index:
        return classificadores.loc[mun+" N_RAT "+row['RAT.NUM_ATIVIDADE'], 'SETOR']
    elif mun + ' BAIRRO ' + row['NOME_BAIRRO'] in classificadores.index:       
        return ( classificadores.loc[mun + ' BAIRRO ' + row['NOME_BAIRRO'].upper(), 'SETOR'] ).upper()
    elif mun + ' LOGRADOURO ' + row['LOGRADOURO'] in classificadores.index:
        return classificadores.loc[mun + ' LOGRADOURO ' + row['LOGRADOURO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO ' + row['DES_ENDERECO'] in classificadores.index:
        return classificadores.loc[mun + ' LOGRADOURO ' + row['DES_ENDERECO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'] in classificadores.index:
        return classificadores.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO'] in classificadores.index:
        return classificadores.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'] in classificadores.index:
        return classificadores.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO2'] in classificadores.index:
        return classificadores.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO2'].upper(), 'SETOR']
    elif mun + ' COMPLEMENTO_END ' + row['COMPLEMENTO_ENDERECO'] in classificadores.index:
        return classificadores.loc[mun + ' COMPLEMENTO_END ' + row['COMPLEMENTO_ENDERECO'].upper(), 'SETOR']
    elif ( mun + ' COMPLEMENTO_END ' + row['DES_ENDERECO'] ) in classificadores.index:
        return classificadores.loc[mun + ' COMPLEMENTO_END ' + row['DES_ENDERECO'], 'SETOR']    
    else:
        return 'other'


def classifica_cia(df_bos):
    conds=[
        df_bos['MUNICIPIO'].isin(['ITAUNA','ITATIAIUCU']),
        df_bos['SETOR'].isin(['HIPER CENTRO','BOM PASTOR','ALTO GOIAS']),
        df_bos['SETOR'].isin(['PLANALTO','SAO JOSE','CLAUDIO']),
        df_bos['SETOR'].isin(['NITEROI','PORTO VELHO','CARMO DO CAJURU/SAO GONCALO DO PARA']),
        
    ]
    res=['51 CIA','53 CIA','139 CIA','142 CIA']
    df_bos['CIA'] = np.select(conds,res,default='other')



def get_bos_unclassified():    
    '''Retorna os registros de BOS com SETOR == other'''
    
    query = '''
    SELECT    
        "RAT.NUM_ATIVIDADE",
        "MUNICIPIO",
        "LOGRADOURO",
        "DES_ENDERECO",
        "COMPLEMENTO_ENDERECO",
        "NOME_BAIRRO",
        "LOGRADOURO2",
        "DES_ENDERECO2",
        "SETOR",
        "CIA"
    FROM
        tbl_rat
    WHERE
        (
            "SETOR" == "other" or
            "CIA" == "other"
        )        
    AND "MUNICIPIO" != "ITAUNA"
    AND "NAT.CODIGO" IN ('Y07001', 'Y07003', 'Y07004', 'Y07005', 'Y04012')    
    '''


    return pd.read_sql(query, 'sqlite:///gdo.db')