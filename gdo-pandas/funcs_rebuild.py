import pandas as pd
import numpy as np
import gc, csv


def get_files_names(only_not_imported=False):
    '''retorna uma lista, com 4 listas de nomes de arquivos
    de rat (rat, viatura, efetivo, produtividade)'''
    import os,re    

    dir = os.getcwd()
    rats_files_names = os.listdir(dir+'/files/RAT')
    rat_pattern = re.compile('REDS_RAT_2')
    rat_viatura_pattern = re.compile('RAT_VIATURA')
    rat_efetivo_pattern = re.compile('RAT_EFETIVO')
    rat_produtividade_pattern = re.compile('RAT_Produtividade')

    rats_files = list()
    rats_viatura_files = list()
    rats_efetivo_files = list()
    rats_produtividade_files = list()

    if only_not_imported:
        df_imported_files = pd.read_sql_table('tbl_imported_files', 'sqlite:///gdo.db')
        rats_files_names = [file_name for file_name in rats_files_names if file_name not in df_imported_files['0'].values ]

    for file_name in rats_files_names:
        is_rat = rat_pattern.search(file_name) != None
        is_rat_viatura = rat_viatura_pattern.search(file_name) != None
        is_rat_efetivo = rat_efetivo_pattern.search(file_name) != None
        is_rat_produtividade = rat_produtividade_pattern.search(file_name) != None
        if is_rat:
            rats_files.append(file_name)
        elif is_rat_viatura:
            rats_viatura_files.append(file_name)
        elif is_rat_efetivo:
            rats_efetivo_files.append(file_name)
        elif is_rat_produtividade:
            rats_produtividade_files.append(file_name)
                
    rats_files.sort(key=lambda name: int(name[9:17]))
    rats_viatura_files.sort(key=lambda name: int(name[18:26]))
    rats_efetivo_files.sort(key=lambda name: int(name[18:26]))
    rats_produtividade_files.sort(key=lambda name: int(name[23:31]))
    return {
        'rat':rats_files,
        'ratv':rats_viatura_files,
        'rate':rats_efetivo_files,
        'ratp':rats_produtividade_files
    }


def get_df_classif():
    df_classif = pd.read_csv('files/classificadores.csv')    
    df_classif.set_index( df_classif['MUNICIPIO'] + " " + df_classif['VALIDADOR_TIPO'] + " " + df_classif['VALIDADOR'], inplace=True)    
    df_classif.fillna('', inplace=True)
    df_classif = df_classif.reset_index().drop_duplicates('index').set_index('index')
    return df_classif

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

def get_nrats_on_db():
    import sqlite3, itertools
    with sqlite3.connect('gdo.db') as conn:
        cursor = conn.cursor()
        nrats_in_db = cursor.execute('SELECT "RAT.NUM_ATIVIDADE" from "tbl_rat"').fetchall()
    return list(itertools.chain(*nrats_in_db))

    
def read_files(files_path_names, rat_data=True):
    if not rat_data:
        nrats_on_db = get_nrats_on_db()
    if len( files_path_names) < 1:
        raise Exception('Não há arquivos csv novos para serem inseridos no banco de dados.')
    for i in range( len(files_path_names) ):    
        if i == 0:        
            df = pd.read_csv('files/RAT/' + files_path_names[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
            df = df.applymap(lambda x: x.strip() if type(x) == str else x)    
            if rat_data:
                df['MUNICIPIO'] = df['MUNICIPIO'].astype('category')
                df = filter_23(df)
            else:
                df = df[
                    df.iloc[:,0].isin(nrats_on_db)
                ]
        else:
            df_aux = pd.read_csv('files/RAT/' + files_path_names[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
            df_aux = df_aux.applymap(lambda x: x.strip() if type(x) == str else x)
            if rat_data:
                df_aux['MUNICIPIO'] = df_aux['MUNICIPIO'].astype('category')
                df_aux = filter_23(df_aux)
            else:
                df_aux = df_aux[
                    df_aux.iloc[:,0].isin(nrats_on_db)
                ]
            df = pd.concat([df, df_aux])
    return df


def data_rat_processing(df_rat):
    df_rat.drop_duplicates(subset='RAT.NUM_ATIVIDADE', keep='last', inplace=True)

    s_dta_in = df_rat['DTA_INICIO'] + " " + df_rat['HRA_INICIO']
    df_rat.loc[:,'DTA_HRA_INICIO_DT'] = pd.to_datetime( s_dta_in, format='%d/%m/%Y %H:%M', errors='coerce')
    del s_dta_in

    s_dta_ter = df_rat.loc[:,'DTA_TERMINO'] + " " + df_rat.loc[:,'HRA_TERMINO']
    df_rat.loc[:,'DTA_HRA_TERMINO_DT'] = pd.to_datetime( s_dta_ter, format='%d/%m/%Y %H:%M', errors='coerce')
    del s_dta_ter

    df_rat.loc[:,'TEMPO_DT'] = df_rat['DTA_HRA_TERMINO_DT'] - df_rat['DTA_HRA_INICIO_DT']

    df_rat.loc[:,'TEMPO_INT'] = df_rat['TEMPO_DT'].dt.total_seconds() / 60

    df_rat.loc[:,'DIA'] = df_rat.loc[:,'DTA_HRA_INICIO_DT'].dt.day
    
    df_rat.loc[:,'MES'] = df_rat.loc[:,'DTA_HRA_INICIO_DT'].dt.month

    df_rat.loc[:,'ANO'] = df_rat.loc[:,'DTA_HRA_INICIO_DT'].dt.year

    df_rat.loc[:,'DEZENA'] = np.select([
        df_rat['DTA_HRA_INICIO_DT'].dt.day <= 10,
        df_rat['DTA_HRA_INICIO_DT'].dt.day <= 20
    ],[1,2], default=3)


    for field in ['TEMPO_INT', 'DIA', 'MES', 'ANO', 'DEZENA']:
        df_rat[field].fillna(0, inplace=True)
        df_rat[field] = df_rat[field].astype('int16')

    cols_classif = [
        'MUNICIPIO',
        'LOGRADOURO',
        'DES_ENDERECO',
        'COMPLEMENTO_ENDERECO',
        'NOME_BAIRRO',
        'LOGRADOURO2',
        'DES_ENDERECO2'
    ]
    df_rat[cols_classif] = df_rat[cols_classif].apply(lambda col: col.str.upper())
    df_rat[cols_classif] = df_rat[cols_classif].fillna('')

    return df_rat


def classifica_setor(row, df_classif):
    mun = row['MUNICIPIO']
    if mun == 'CLAUDIO':        
        return 'CLAUDIO'
    elif mun == 'ITATIAIUCU':
        return 'LOURDES/ITATIAIUCU'
    elif mun in ('CARMO DO CAJURU', 'SAO GONCALO DO PARA'):             
        return 'CARMO DO CAJURU/SAO GONCALO DO PARA'    
    elif ( mun + " N_RAT " + row['RAT.NUM_ATIVIDADE'] ) in df_classif.index:
        return df_classif.loc[mun+" N_RAT "+row['RAT.NUM_ATIVIDADE'], 'SETOR']
    elif mun + ' BAIRRO ' + row['NOME_BAIRRO'] in df_classif.index:       
        return ( df_classif.loc[mun + ' BAIRRO ' + row['NOME_BAIRRO'].upper(), 'SETOR'] ).upper()
    elif mun + ' LOGRADOURO ' + row['LOGRADOURO'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO ' + row['LOGRADOURO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO ' + row['DES_ENDERECO'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO ' + row['DES_ENDERECO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['LOGRADOURO2'].upper(), 'SETOR']
    elif mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO2'] in df_classif.index:
        return df_classif.loc[mun + ' LOGRADOURO_NAO_CAD ' + row['DES_ENDERECO2'].upper(), 'SETOR']
    elif mun + ' COMPLEMENTO_END ' + row['COMPLEMENTO_ENDERECO'] in df_classif.index:
        return df_classif.loc[mun + ' COMPLEMENTO_END ' + row['COMPLEMENTO_ENDERECO'].upper(), 'SETOR']
    elif ( mun + ' COMPLEMENTO_END ' + row['DES_ENDERECO'] ) in df_classif.index:
        return df_classif.loc[mun + ' COMPLEMENTO_END ' + row['DES_ENDERECO'], 'SETOR']    
    else:
        return 'other'


def classifica_cia(df_rat):
    conds=[
        df_rat['MUNICIPIO'].isin(['ITAUNA','ITATIAIUCU']),
        df_rat['SETOR'].isin(['HIPER CENTRO','BOM PASTOR','ALTO GOIAS']),
        df_rat['SETOR'].isin(['PLANALTO','SAO JOSE','CLAUDIO']),
        df_rat['SETOR'].isin(['NITEROI','PORTO VELHO','CARMO DO CAJURU/SAO GONCALO DO PARA']),
        
    ]
    res=['51 CIA','53 CIA','139 CIA','142 CIA']
    df_rat['CIA'] = np.select(conds,res,default='other')


def get_rats_gdo():
    import vars    

    ######################### CRIA df_rat
    df_rat_gdo = pd.read_sql_table('tbl_rat', 'sqlite:///gdo.db')

    df_rat_gdo = df_rat_gdo[
        (df_rat_gdo['NAT.CODIGO'].str.contains('Y0700[1345]')) |
        (df_rat_gdo['NAT.CODIGO'] == 'Y04012')  
    ]

    df_rat_gdo.set_index('RAT.NUM_ATIVIDADE', inplace=True)

    ######################### BUSCA DADOS DE VIATURAS E ACRESCENTA EM df_rat
    df_ratv_gdo = pd.read_sql_table('tbl_rat_viaturas', 'sqlite:///gdo.db')
    df_ratv_gdo = df_ratv_gdo['NUM_ATIVIDADE'].value_counts()
    df_ratv_gdo.rename('VIATURAS', inplace=True)
    df_rat_gdo = df_rat_gdo.join(df_ratv_gdo, how='left')
    del df_ratv_gdo
    df_rat_gdo['VIATURAS'].fillna(0, inplace=True)
    df_rat_gdo['VIATURAS'] = df_rat_gdo['VIATURAS'].astype('uint8')

    ######################### BUSCA DADOS DE EFETIVO E ACRESCENTA EM df_rat
    df_rate_gdo = pd.read_sql_table('tbl_rat_efetivo', 'sqlite:///gdo.db')
    df_rate_gdo = df_rate_gdo['NUM_ATIVIDADE'].value_counts()
    df_rate_gdo.rename('EFETIVO', inplace=True)
    df_rat_gdo = df_rat_gdo.join(df_rate_gdo, how='left')
    del df_rate_gdo
    df_rat_gdo['EFETIVO'].fillna(0, inplace=True)
    df_rat_gdo['EFETIVO'] = df_rat_gdo['EFETIVO'].astype('uint8')

    ######################### BUSCA DADOS DE PRODUTIVIDADE E ACRESCENTA EM df_rat
    df_ratp_gdo = pd.read_sql_table('tbl_rat_produtividade', 'sqlite:///gdo.db')
    df_ratp_gdo_efet = df_ratp_gdo[
        df_ratp_gdo['DESCRICAO'].isin(vars.itens_efet)
    ].drop_duplicates('RAT.NUM_ATIVIDADE').set_index('RAT.NUM_ATIVIDADE')['QUANTIDADE'].map(lambda qtd:1).rename('EFETIVIDADE_PARCIAL')
    df_rat_gdo = df_rat_gdo.join(df_ratp_gdo_efet, how='left')
    del df_ratp_gdo_efet
    df_rat_gdo['EFETIVIDADE_PARCIAL'].fillna(0, inplace=True)
    df_rat_gdo['EFETIVIDADE_PARCIAL'] = df_rat_gdo['EFETIVIDADE_PARCIAL'].astype('int8')

    df_itens_ee = df_ratp_gdo[
        df_ratp_gdo['DESCRICAO'].isin([
            'Qde de veiculos fiscalizados',
            'Qde de pessoas abordadas',
            'Qde de locais fiscalizados',
            'Qde de pessoas que sopraram o etilometro'
        ])
    ]
    df_itens_ee = pd.pivot_table(df_itens_ee, columns='DESCRICAO', index='RAT.NUM_ATIVIDADE')
    df_itens_ee.columns = df_itens_ee.columns.droplevel()
    df_rat_gdo = df_rat_gdo.join(df_itens_ee, how='left')
    for i in [
        'Qde de veiculos fiscalizados',
        'Qde de pessoas abordadas',
        'Qde de locais fiscalizados',
        'Qde de pessoas que sopraram o etilometro'
    ]:
        df_rat_gdo[i].fillna(0, inplace=True)
        df_rat_gdo[i] = df_rat_gdo[i].astype('int16')
    del df_ratp_gdo

    cond71 = (
        # Y07001 - OPERACAO DE BATIDA POLICIAL
        (
            (df_rat_gdo['NAT.CODIGO'] == 'Y07001') &
            (df_rat_gdo['TEMPO_INT'] >= 30) &
            (df_rat_gdo['EFETIVO'] >= 2)
        ) &
        (
            (df_rat_gdo['Qde de pessoas abordadas'] >= 5) |
            (df_rat_gdo['Qde de veiculos fiscalizados'] >= 2)
        )
    )
    cond73 = (
        # Y07003 - OPERACAO DE INCURSAO EM ZONA QUENTE DE CRIMINALIDADE
        (
            (df_rat_gdo['NAT.CODIGO'] == 'Y07003') &
            (df_rat_gdo['TEMPO_INT'] >= 30) &
            (df_rat_gdo['EFETIVO'] >= 3) &
            (df_rat_gdo['VIATURAS'] >= 1)
        ) &
        (
            (df_rat_gdo['Qde de pessoas abordadas'] >= 5) |
            (df_rat_gdo['Qde de veiculos fiscalizados'] >= 2) |
            (df_rat_gdo['Qde de locais fiscalizados'] >= 2)
        )
    )
    cond74 = (
        # Y07004 - OPERACAO DE CERCO / BLOQUEIO / INTERCEPTACAO
        (
            (
                (df_rat_gdo['NAT.CODIGO'] == 'Y07004') &            
                (df_rat_gdo['EFETIVO'] >= 3) &
                (df_rat_gdo['VIATURAS'] >= 2)
            ) &
            (
                (df_rat_gdo['Qde de pessoas abordadas'] >= 1) |
                (df_rat_gdo['Qde de veiculos fiscalizados'] >= 1)
            )
        )
    )
    cond75 = (
        # Y07005 - OPERACAO DE OCUPACAO DE PTOS DE ZONA QUENTE DE CRIMINALIDADE
        (
            (df_rat_gdo['NAT.CODIGO'] == 'Y07005') &
            (df_rat_gdo['TEMPO_INT'] >= 60) &
            (df_rat_gdo['EFETIVO'] >= 3) &
            (df_rat_gdo['VIATURAS'] >= 1)
        ) &
        (
            (df_rat_gdo['Qde de pessoas abordadas'] >= 5) |
            (df_rat_gdo['Qde de veiculos fiscalizados'] >= 2) |
            (df_rat_gdo['Qde de locais fiscalizados'] >= 2)
        )    
    )
    cond412 = (
        # Y04012 - OPERACAO LEI SECA
        (
            (df_rat_gdo['NAT.CODIGO'] == 'Y04012') &
            (df_rat_gdo['TEMPO_INT'] >= 30) &
            (df_rat_gdo['EFETIVO'] >= 2) &
            (df_rat_gdo['VIATURAS'] >= 1)
        ) &
        (
            (df_rat_gdo['Qde de pessoas abordadas'] >= 3) |
            (df_rat_gdo['Qde de pessoas que sopraram o etilometro'] >= 3) |
            (df_rat_gdo['Qde de veiculos fiscalizados'] >= 3)
        )
    )

    cond_ee = [
        cond71 |
        cond73 |
        cond74 |
        cond75 |
        cond412
    ]

    df_rat_gdo['EFICIENCIA_E_EFICACIA'] = np.select(cond_ee,[1],default=0)
    df_rat_gdo['EFETIVIDADE'] = np.select(
        [(df_rat_gdo['EFETIVIDADE_PARCIAL'] == 1) & (df_rat_gdo['EFICIENCIA_E_EFICACIA'] == 1)],
        [1],
        default=0
    )

    return df_rat_gdo

def get_unclassified_registers(tipo):
    '''Retorna os registros de RAT ou BOS com SETOR == other'''
    
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
        tbl_{}
    WHERE
        (
            "SETOR" == "other" or
            "CIA" == "other"
        )        
    AND "MUNICIPIO" != "ITAUNA"
    AND (
        ( "NAT.CODIGO" IN ('Y07001', 'Y07003', 'Y07004', 'Y07005', 'Y04012') ) OR
        ( "NAT.CODIGO" LIKE 'A19%' )
    )
    
    '''.format(tipo)

    return pd.read_sql(query, 'sqlite:///gdo.db')