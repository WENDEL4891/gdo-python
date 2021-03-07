import pandas as pd
import numpy as np
import os
import datetime


def get_metas(mes_param, dia_param):
    '''Retorna um dicionário, com as metas '''
    dias_por_mes = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    metas = pd.read_sql_table('tbl_metas', 'sqlite:///gdo.db')
    mes = mes_param
    dia = dia_param
    
    metas_by_cia_indicador_mes = metas[
        metas['MES'] <= mes_param
    ].groupby(['CIA','INDICADOR','MES']).sum()['META'].unstack('MES')
    
    metas = {}
    metas_somar = ['tcv', 'thc', 'ic', 'ols', 'rqv_ee', 'rqv_efet', 'tqf']
    metas_nao_somar = ['ddu_concluido', 'ddu_sucesso', 'iaf', 'tri']    
    for meta in metas_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta.upper(), level=1).copy()
        metas[meta].iloc[:,-1] = metas[meta].iloc[:,-1] / dias_por_mes[mes] * (dia - 1)
        metas[meta].loc[:,'ACUM'] = metas[meta].sum(1)
        metas[meta].loc['23 BPM'] = metas[meta].sum()
        metas[meta] = metas[meta].round(2)
    for meta in metas_nao_somar:
        metas[meta] = metas_by_cia_indicador_mes.xs(meta.upper(), level=1).copy()
        metas[meta].loc[:,'ACUM'] = metas[meta].iloc[:,0]
        metas[meta].loc['23 BPM'] = metas[meta].iloc[1]
        metas[meta] = metas[meta].round(2)
    return metas
    

def read_files(path_file, sheet_name, mes_param_read):
    df = pd.read_excel(path_file, sheet_name=sheet_name)
    df = df[df['Mês Numérico Fato'] <= mes_param_read]
    return df

def get_bd_dados(mes_param):
    bd_dados = dict()

    indicadores = (
        ('tcv', 'BD'),
        ('thc', 'HC VITIMAS'),
        ('tqf', 'BD'),
        ('iaf_armas', 'bd armas'),
        ('iaf_simulacros', 'bd simulacros'),
        ('iaf_crimes', 'bd crimes af'),
        ('tri_presos', 'BD_PRISOES'),
        ('tri_crimes', 'BD_CV')
    )

    file_list = os.listdir('files/Armazem/2020/')
    path_files = 'files/Armazem/2020/'

    for indicador in indicadores:
        bd_dados[indicador[0]] = read_files(
            path_files+list(filter(lambda file: indicador[0][0:3].upper() in file, file_list))[0],
            sheet_name=indicador[1],
            mes_param_read = mes_param
        ).rename(columns={'Mês Numérico Fato': 'MES', 'Dia Numérico Fato': 'Dia'})
    return bd_dados


def get_tables():
    indicadores = ['tcv', 'thc', 'tqf', 'iaf', 'tri']
    tables = {
        indicador:dict() for indicador in indicadores
    }
    for key, value in tables.items():
        value['mes'] = dict()
        value['acum'] = dict()
        value['dados'] = dict()
    return tables

def set_tables_data(bd_dados_param, mes_param):
    tables=get_tables()
    cias = ('53 CIA', '139 CIA', '142 CIA', '51 CIA')
    itens_indicadores = (
            ('tcv','Qtde Ocorrências'),
            ('thc', 'Qtde Envolvidos'),
            ('tqf', 'Qtde Ocorrências'),
            ('iaf_armas', 'Qtde  Armas de Fogo'),
            ('iaf_simulacros', 'Qtde Materiais'),
            ('iaf_crimes', 'Qtde Ocorrências'),
            ('tri_presos', 'Qtde Envolvidos'),
            ('tri_crimes', 'Qtde Ocorrências')

        )

    series_base = pd.Series([0, 0, 0, 0], index = cias)
    series_base_com_cia_invalida = pd.Series([0, 0, 0, 0, 0], index = cias + ('CIA INDEFINIDA',))
    cias = ('')
    
    for item in itens_indicadores:
        indicador = item[0][0:3]
        col_cia = list(filter(lambda cols: '23_CIA' in cols, bd_dados_param[item[0]]))[0]

        dados_table = tables[indicador]['dados']    
        dados_indicador = bd_dados_param[item[0]]
        
        
        dados_table['{}_{}'.format(item[0], 'mes')] = dados_indicador[
            dados_indicador['MES'] == mes_param
        ].groupby(col_cia).sum()[item[1]]
        
        dados_table[item[0]+'_acum'] = dados_indicador[
            dados_indicador['MES'] <= mes_param
        ].groupby(col_cia).sum()[item[1]]
        
        for periodo in ('mes', 'acum'):

            tem_cia_invalida = list(filter(lambda cia: cia in cias, dados_table['{}_{}'.format(item[0], periodo)].index))

            dados_table['{}_{}'.format(item[0], periodo)] = pd.concat([ 
                series_base if not tem_cia_invalida else series_base_com_cia_invalida,
                dados_table['{}_{}'.format(item[0], periodo)]
            ], axis=1, sort=False).fillna(0).astype('uint32').iloc[:,1]
            del tem_cia_invalida

            dados_table['{}_{}'.format(item[0], periodo)].loc['23 BPM'] = dados_table['{}_{}'.format(item[0], periodo)].sum()

       
    dados_armas = tables['iaf']['dados']
    for periodo in ('mes', 'acum'):
        dados_armas['aux_'+periodo] = pd.concat([
            dados_armas['iaf_armas_'+periodo],
            dados_armas['iaf_simulacros_'+periodo]
        ], axis=1, sort=False).fillna(0).astype('int16')
        
        dados_armas['armas_total_'+periodo] = (
            dados_armas['aux_'+periodo]['Qtde  Armas de Fogo'] + dados_armas['aux_'+periodo]['Qtde Materiais']
        )
        dados_armas['armas_total_'+periodo].name = 'AFA'
        
    return tables



def get_populacao():
    pop = pd.read_sql_table('tbl_populacoes', 'sqlite:///gdo.db')    
    pop.set_index('CIA', inplace=True)
    pop.index.name = 'CIA'    
    return pop
    

def get_farol(valor, polaridade):
    feliz = '&#128578'
    normal = '&#x1f610'
    triste = '&#128577'
    if polaridade == 'positiva':
        if valor >= 100:
            return feliz
        elif valor >= 70:
            return normal
        else:
            return triste
    if polaridade == 'negativa':
        if valor <= 0:
            return feliz
        elif valor < 10:
            return normal
        else:
            return triste

    
def set_tables_indicadores_polaridade_negativa(tables_param, metas_param, mes_param):    
    tables = tables_param
    metas = metas_param
    mes = mes_param
    for indicador in ['tcv','thc','tqf']:
        for periodo in ['mes', 'acum']:
                                   
            
            tables[indicador][periodo] = pd.concat(
                [
                    get_populacao(),
                    tables[indicador]['dados'][indicador+'_'+periodo]
                ], axis=1, sort=False
            )

            tables[indicador][periodo].columns = ['POPULACAO', 'ABS']

            tables[indicador][periodo][indicador.upper()] = (
                tables[indicador][periodo]['ABS'].values / tables[indicador][periodo]['POPULACAO'].values * 100000
            ).round(2)    

            tables[indicador][periodo]['META ABS'] = metas[indicador][mes if periodo == 'mes' else 'ACUM'] 

            tables[indicador][periodo]['META '+indicador.upper()] = (
                tables[indicador][periodo]['META ABS'].values / tables[indicador][periodo]['POPULACAO'] * 100000
            ).round(2)

            tables[indicador][periodo]['VAR %'] = (
                ( tables[indicador][periodo][indicador.upper()].values - tables[indicador][periodo]['META '+indicador.upper()] )
                / ( tables[indicador][periodo]['META '+indicador.upper()] ) * 100
            ).round(2)
            tables[indicador][periodo]['VAR %'] = tables[indicador][periodo].apply(
                lambda row: 0 if row['ABS'] == 0 else row['VAR %'], axis = 1
            )
            
            tables[indicador][periodo]['VAR %'].fillna(0, inplace=True)
            tables[indicador][periodo]['PLP'] = '10,00 %'
            tables[indicador][periodo]['FAROL'] = tables[indicador][periodo]['VAR %'].apply(
                lambda var: get_farol(var, 'negativa')
            )            
            tables[indicador][periodo]['VAR %'] = tables[indicador][periodo]['VAR %'].apply(lambda var: str(var)+' %')
                        
            tem_cia_invalida = list(filter(
                lambda cia: cia not in ('51 CIA', '53 CIA', '139 CIA', '142 CIA', '23 BPM'),
                tables[indicador][periodo].index
            ))
                       
            if tem_cia_invalida:
                tables[indicador][periodo].loc['CIA INDEFINIDA'] = [
                    '-',
                    tables[indicador][periodo].loc[ tem_cia_invalida[0], 'ABS' ],
                    '-', '-', '-', '-', '-', '-'
                ]
                tables[indicador][periodo] = tables[indicador][periodo].reindex([
                    '53 CIA', '139 CIA', '142 CIA', '51 CIA', 'CIA INDEFINIDA', '23 BPM'
                ])
            del tem_cia_invalida
            
#             cols = tables[indicador][periodo].columns.to_list()            
#             for i in range(len(cols)):
#                 if i in (0, 1):
#                     tables[indicador][periodo][cols[i]] = (
#                         tables[indicador][periodo][cols[i]].astype('int', errors='ignore')                    
#                     )

            '''no trecho adiante, está sendo usada uma combinação de funções, com apply, pois a função astype(),
            da pandas, não funcionou, para essa parte do código'''
            tables[indicador][periodo]['POPULACAO'] = (
                tables[indicador][periodo]['POPULACAO'].apply(
                    lambda val: int(val) if str(val).replace('.','',1).isnumeric() else val
                )
            )
            
            tables[indicador][periodo].columns = pd.MultiIndex.from_product([
                [indicador.upper()+' - '+periodo.upper()], tables[indicador][periodo].columns
            ])
    return tables
            



def set_tables_iaf(tables_param, metas_param, mes_param):
    pop = get_populacao()
    tables = tables_param
    metas = metas_param
    mes = mes_param
    for periodo in ('mes', 'acum'):
        tables['iaf'][periodo] = pd.concat([
            pop,
            tables['iaf']['dados']['armas_total_'+periodo],
            tables['iaf']['dados']['iaf_crimes_'+periodo]
        ], axis=1, sort=False).fillna(0).astype('int')\
        .rename(columns={'Qtde Ocorrências': 'TCAF'})        

        tables['iaf'][periodo]['TAXA'] = (
            tables['iaf'][periodo]['AFA'] / ( tables['iaf'][periodo]['TCAF'] + tables['iaf'][periodo]['AFA'] )
            * 100
        ).round(2)
        tables['iaf'][periodo]['TAXA'].fillna(0, inplace=True)
        tables['iaf'][periodo]['META'] = metas['iaf'][mes if periodo == 'mes' else 'ACUM']
        tables['iaf'][periodo]['VAR %'] = (
            tables['iaf'][periodo]['TAXA'] / tables['iaf'][periodo]['META'] * 100        
        ).round(2)
        tables['iaf'][periodo]['TAXA']
        tables['iaf'][periodo]['FAROL'] = tables['iaf'][periodo]['VAR %'].apply(
            lambda val: get_farol(val, 'positiva')
        )
        tables['iaf'][periodo]['VAR %'] = tables['iaf'][periodo]['VAR %'].apply(lambda var: str(var) + ' %')
                
        tem_cia_invalida = list(filter(
                lambda cia: cia not in ('51 CIA', '53 CIA', '139 CIA', '142 CIA', '23 BPM'),
                tables['iaf'][periodo].index
            ))
            
        if tem_cia_invalida:
            tables['iaf'][periodo].loc['CIA INDEFINIDA'] = [
                '-',
                tables['iaf'][periodo].loc[ tem_cia_invalida, 'AFA' ].sum(),
                tables['iaf'][periodo].loc[ tem_cia_invalida, 'TCAF' ].sum(),
                '-', '-', '-', '-'
            ]
            tables['iaf'][periodo] = tables['iaf'][periodo].reindex([
                '53 CIA', '139 CIA', '142 CIA', '51 CIA', 'CIA INDEFINIDA', '23 BPM'
            ])
        del tem_cia_invalida
        
        tables['iaf'][periodo].columns = pd.MultiIndex.from_product([
            ['IAF - '+periodo.upper()], tables['iaf'][periodo].columns
        ])
    return tables
        
        
def set_tables_tri(tables_param, metas_param, mes_param):
    pop = get_populacao()
    tables = tables_param
    metas = metas_param
    mes = mes_param
    for periodo in ('mes', 'acum'):
        tables['tri'][periodo] = pd.concat([pop, tables['tri']['dados']['tri_presos_'+periodo]], axis=1, sort=False)
        tables['tri'][periodo].rename(columns={'Qtde Envolvidos': 'NPAA'}, inplace=True)
        tables['tri'][periodo]['TRCV'] = tables['tri']['dados']['tri_crimes_'+periodo]
        tables['tri'][periodo]['TAXA'] = (
            tables['tri'][periodo]['NPAA'].values / tables['tri'][periodo]['TRCV']
            * 100
        ).round(2)
        tables['tri'][periodo]['TAXA'].fillna(0, inplace=True)
        tables['tri'][periodo]['META'] = metas['tri'][mes if periodo == 'mes' else 'ACUM']
        tables['tri'][periodo]['VAR %'] = (
            tables['tri'][periodo]['TAXA'] / tables['tri'][periodo]['META'] * 100        
        ).round(2)
        tables['tri'][periodo]['FAROL'] = tables['tri'][periodo]['VAR %'].apply(
            lambda val: get_farol(val, 'positiva')
        )
        tables['tri'][periodo]['FAROL'] = tables['tri'][periodo].apply(
            lambda row: '&#128578' if row['TRCV'] == 0 else row['FAROL'], axis = 1
        )
        tables['tri'][periodo]['VAR %'] = tables['tri'][periodo]['VAR %'].apply(lambda var: str(var) + ' %')        
        
        tem_cia_invalida = list(filter(
                lambda cia: cia not in ('51 CIA', '53 CIA', '139 CIA', '142 CIA', '23 BPM'),
                tables['tri'][periodo].index
            ))
            
        if tem_cia_invalida:
            tables['tri'][periodo].loc['CIA INDEFINIDA'] = [
                '-',
                tables['tri'][periodo].loc[ tem_cia_invalida, 'NPAA' ].sum(),
                tables['tri'][periodo].loc[ tem_cia_invalida, 'TRCV' ].sum(),
                '-', '-', '-', '-'
            ]
            tables['tri'][periodo] = tables['tri'][periodo].reindex([
                '53 CIA', '139 CIA', '142 CIA', '51 CIA', 'CIA INDEFINIDA', '23 BPM'
            ])
        del tem_cia_invalida
        
        tables['tri'][periodo].columns = pd.MultiIndex.from_product([
            ['TRI - '+periodo.upper()], tables['tri'][periodo].columns
        ])
    return tables
        

def farol_colors(val):
    feliz = '&#128578'
    normal = '&#x1f610'
    triste = '&#128577'
    if val == feliz:
        color = 'green'
    elif val == normal:
        color = 'yellow'
    elif val == triste:
        color = 'red'
    else:
        color = 'orange'
    return (
        '''                
        background-color: {};
        font-size: 17px;
        text-align: center;        
        '''.format(color)
    )

def convert_farol_JKL(val):
    feliz = '&#128578'
    normal = '&#x1f610'
    triste = '&#128577'
    if val == feliz:
        return 'J'
    elif val == normal:
        return 'K'
    elif val == triste:
        return 'L'

def show_tables(tables_param):
    tables = tables_param
    for indicador in ['tcv', 'thc', 'tqf', 'iaf', 'tri']:
        for periodo in ['mes', 'acum']:
            table = tables[indicador][periodo].style\
            .applymap(lambda val: 'text-align: center')\
            .applymap(farol_colors, subset=[(indicador.upper()+' - '+periodo.upper(), 'FAROL')])
            display(table)    
    

# def multiple_dfs(df_list, sheets, file_name, spaces):
#     writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
#     row = 0
#     for dataframe in df_list:
#         dataframe.to_excel(writer,sheet_name=sheets,startrow=row , startcol=0)   
#         row = row + len(dataframe.index) + spaces + 1
#     writer.save()

def multiple_dfs(tables, sheets, file_name, spaces):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    row = 0
    for indicador in ('tcv', 'thc', 'tqf', 'iaf', 'tri'):
        for periodo in ('mes', 'acum'):
            table_copy = tables[indicador][periodo].copy()            
            table_copy[(indicador.upper()+' - '+periodo.upper(), 'FAROL')] =\
            table_copy[(indicador.upper()+' - '+periodo.upper(), 'FAROL')].apply(
                lambda val: convert_farol_JKL(val)
            )
            table_copy.to_excel(writer,sheet_name=sheets,startrow=row , startcol=0)   
            row = row + len(table_copy.index) + spaces + 1
    writer.save()
        
    
def get_gdo_tables(modo, mes_param=None):
    dias_por_mes = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    if modo == 'parcial':
        datetime_now = datetime.datetime.now()
        dia = datetime_now.day
        mes = datetime_now.month
        if dia == 1:
            mes -= 1
            dia = dias_por_mes[mes] + 1
    if modo == 'mes_fechado':        
        mes = mes_param
        dia = dias_por_mes[mes] + 1
    
    bd_dados = get_bd_dados(mes_param = mes)
    tables = get_tables()
    tables = set_tables_data(bd_dados_param = bd_dados, mes_param = mes)
    pop = get_populacao()
    metas = get_metas(mes_param = mes, dia_param = dia)
    tables = set_tables_indicadores_polaridade_negativa(tables_param = tables, metas_param = metas, mes_param = mes)
    tables = set_tables_iaf(tables, metas, mes)
    tables = set_tables_tri(tables, metas, mes)
    multiple_dfs(tables, 'Indicadores_Armazem', 'Indicadores_Armazem.xlsx', 4)
    return bd_dados, tables
    


