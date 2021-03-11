import pandas as pd
import numpy as np
import os
import csv

class tratador_de_arquivos:
    def get_nomes_de_arquivos(self, rat_ou_bos='RAT', apenas_nao_importados=True):
        '''retorna uma lista, com 4 listas de nomes de arquivos
        de rat (rat, viatura, efetivo, produtividade)'''

        rat_ou_bos_lower = rat_ou_bos.lower()
        rat_ou_bos_upper = rat_ou_bos.upper()

        dir = os.getcwd()
        nomes_de_arquivos = os.listdir(dir+'/arquivos/{}'.format(rat_ou_bos))
        
        nomes_de_arquivos_geral = list()
        nomes_de_arquivos_viaturas = list()
        nomes_de_arquivos_efetivo = list()
        nomes_de_arquivos_produtividade = list()

        if apenas_nao_importados:
            df_arquivos_importados = pd.read_sql_table('tbl_imported_files', 'sqlite:///gdo.db')
            nomes_de_arquivos = [nome_de_arquivo for nome_de_arquivo in nomes_de_arquivos if nome_de_arquivo not in df_arquivos_importados['0'].values ]

        for nome_de_arquivo in nomes_de_arquivos:
            is_geral = 'REDS_{}_2'.format(rat_ou_bos_upper) in nome_de_arquivo
            is_viatura = 'REDS_{}_V'.format(rat_ou_bos_upper) in nome_de_arquivo
            is_efetivo = 'REDS_{}_E'.format(rat_ou_bos_upper) in nome_de_arquivo
            is_produtividade = 'REDS_{}_P'.format(rat_ou_bos_upper) in nome_de_arquivo
            if is_geral:
                nomes_de_arquivos_geral.append(nome_de_arquivo)
            elif is_viatura:
                nomes_de_arquivos_viaturas.append(nome_de_arquivo)
            elif is_efetivo:
                nomes_de_arquivos_efetivo.append(nome_de_arquivo)
            elif is_produtividade:
                nomes_de_arquivos_produtividade.append(nome_de_arquivo)
                    
        nomes_de_arquivos_geral.sort(key=lambda name: int(name[9:17]))
        nomes_de_arquivos_viaturas.sort(key=lambda name: int(name[18:26]))
        nomes_de_arquivos_efetivo.sort(key=lambda name: int(name[18:26]))
        nomes_de_arquivos_produtividade.sort(key=lambda name: int(name[23:31]))
        return {
            '{}_geral'.format(rat_ou_bos_lower):nomes_de_arquivos_geral,
            '{}_viaturas'.format(rat_ou_bos_lower):nomes_de_arquivos_viaturas,
            '{}_efetivo'.format(rat_ou_bos_lower):nomes_de_arquivos_efetivo,
            '{}_produtividade'.format(rat_ou_bos_lower):nomes_de_arquivos_produtividade
        }
    
    def ler_arquivos(self, nomes_de_arquivos, rat_ou_bos='RAT', tipo_geral=True):
        ''' 
            Retorna um DataFrame com os dados dos arquivos que estão nos arquivos salvos diretório arquivios/[RAT ou BOS].
            O parâmetro "rat_ou_bos" deve receber o valor RAT ou BOS, para indicar quais arquivos serão lidos.
            O parâmetro "tipo_geral" deve receber True ou False, para indicar se os arquivos a serem lidos são do tipo geral. Caso não sejam,
            serão de viatura, efetivo ou produtividade. Nesse caso, ao invés de filtrar os arquivos com base no município, o método vai selecionar
            os arquivos cujos números de RAT ou BOS estejam presentes no Banco de Dados.
        '''
        if not tipo_geral:
            nrat_ou_nbos_no_bd = self.get_nrat_ou_nbos_no_db()
        if len( nomes_de_arquivos) < 1:
            raise Exception('Não há arquivos csv novos para serem inseridos no banco de dados.')
        for i in range( len(nomes_de_arquivos) ):    
            if i == 0:        
                df = pd.read_csv('arquivos/{}/'.format(rat_ou_bos.upper()) + nomes_de_arquivos[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
                df = df.applymap(lambda x: x.strip() if type(x) == str else x)    
                if tipo_geral:
                    df['MUNICIPIO'] = df['MUNICIPIO'].astype('category')
                    df = self.filtra_23bpm(df)
                else:
                    df = df[
                        df.iloc[:,0].isin(nrat_ou_nbos_no_bd)
                    ]
            else:
                df_aux = pd.read_csv('arquivos/{}/'.format(rat_ou_bos.upper()) + nomes_de_arquivos[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
                df_aux = df_aux.applymap(lambda x: x.strip() if type(x) == str else x)
                if tipo_geral:
                    df_aux['MUNICIPIO'] = df_aux['MUNICIPIO'].astype('category')
                    df_aux = self.filtra_23bpm(df_aux)
                else:
                    df_aux = df_aux[
                        df_aux.iloc[:,0].isin(nrat_ou_nbos_no_bd)
                    ]
                df = pd.concat([df, df_aux])
        return df
    
    def filtra_23bpm(self, df):
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
    
    def get_nrat_ou_nbos_no_db(self, rat_ou_bos='RAT'):
        import sqlite3, itertools
        with sqlite3.connect('gdo.db') as conn:
            cursor = conn.cursor()
            if rat_ou_bos == 'RAT':
                nrat_ou_nbos_no_db = cursor.execute('SELECT "RAT.NUM_ATIVIDADE" from "tbl_rat_geral"').fetchall()
            if rat_ou_bos == 'BOS':
                nrat_ou_nbos_no_db = cursor.execute('SELECT "RAT.NUM_ATIVIDADE" from "tbl_bos_geral"').fetchall()
        return list(itertools.chain(*nrat_ou_nbos_no_db))

    def get_df_classif(self):
        df_classif = pd.read_csv('arquivos/classificadores.csv')    
        df_classif.set_index( df_classif['MUNICIPIO'] + " " + df_classif['VALIDADOR_TIPO'] + " " + df_classif['VALIDADOR'], inplace=True)    
        df_classif.fillna('', inplace=True)
        df_classif = df_classif.reset_index().drop_duplicates('index').set_index('index')
        return df_classif
    
    def processa_dados(self, df):
        '''
            Recebe um Data Frame com dados de RAT ou BOS e retorna, ao final, outro Data Frame, com alguns processamentos realizados,
            conforme descrito adiante:
            1) Apaga duplicatas. 
            2) Calcula o tempo de cada registro e inclui este dado na coluna TEMPO_INTEIRO.
            3) Converte para uppercase os campos necessários par fazer a classificação por setores.
        '''
        df.drop_duplicates(subset='RAT.NUM_ATIVIDADE', keep='last', inplace=True)

        data_hora_inicio = df['DTA_INICIO'] + " " + df['HRA_INICIO']
        df.loc[:,'DATA_HORA_INICIO_DATE_TIME'] = pd.to_datetime( data_hora_inicio, format='%d/%m/%Y %H:%M', errors='coerce')
        del data_hora_inicio

        data_hora_termino = df.loc[:,'DTA_TERMINO'] + " " + df.loc[:,'HRA_TERMINO']
        df.loc[:,'DTA_HRA_TERMINO_DT'] = pd.to_datetime( data_hora_termino, format='%d/%m/%Y %H:%M', errors='coerce')
        del data_hora_termino

        df.loc[:,'TEMPO_DATE_TIME'] = df['DTA_HRA_TERMINO_DT'] - df['DATA_HORA_INICIO_DATE_TIME']

        df.loc[:,'TEMPO_INTEIRO'] = df['TEMPO_DATE_TIME'].dt.total_seconds() / 60

        df.loc[:,'DIA'] = df.loc[:,'DATA_HORA_INICIO_DATE_TIME'].dt.day
        
        df.loc[:,'MES'] = df.loc[:,'DATA_HORA_INICIO_DATE_TIME'].dt.month

        df.loc[:,'ANO'] = df.loc[:,'DATA_HORA_INICIO_DATE_TIME'].dt.year

        df.loc[:,'DEZENA'] = np.select([
            df['DATA_HORA_INICIO_DATE_TIME'].dt.day <= 10,
            df['DATA_HORA_INICIO_DATE_TIME'].dt.day <= 20
        ],[1,2], default=3)


        for field in ['TEMPO_INTEIRO', 'DIA', 'MES', 'ANO', 'DEZENA']:
            df[field].fillna(0, inplace=True)
            df[field] = df[field].astype('int16')

        cols_classif = [
            'MUNICIPIO',
            'LOGRADOURO',
            'DES_ENDERECO',
            'COMPLEMENTO_ENDERECO',
            'NOME_BAIRRO',
            'LOGRADOURO2',
            'DES_ENDERECO2'
        ]
        df[cols_classif] = df[cols_classif].apply(lambda col: col.str.upper())
        df[cols_classif] = df[cols_classif].fillna('')

        return df
    
    def classifica_setor(self, row, df_classif):
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
    
    def classifica_cia(self, df_rat):
        conds=[
            df_rat['MUNICIPIO'].isin(['ITAUNA','ITATIAIUCU']),
            df_rat['SETOR'].isin(['HIPER CENTRO','BOM PASTOR','ALTO GOIAS']),
            df_rat['SETOR'].isin(['PLANALTO','SAO JOSE','CLAUDIO']),
            df_rat['SETOR'].isin(['NITEROI','PORTO VELHO','CARMO DO CAJURU/SAO GONCALO DO PARA']),
            
        ]
        res=['51 CIA','53 CIA','139 CIA','142 CIA']
        df_rat['CIA'] = np.select(conds,res,default='other')
    
    def get_rats_gdo(self):
        import constantes    

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
            df_ratp_gdo['DESCRICAO'].isin(constantes.ITENS_QUE_COMPUTAM_PRODUTIVIDADE)
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
    
    def get_registros_nao_classificados(tipo):
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
    

