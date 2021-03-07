import pandas as pd

class tratador_de_arquivos:
    def get_files_names(self, only_not_imported=False):
        '''retorna uma lista, com 4 listas de nomes de arquivos
        de rat (rat, viatura, efetivo, produtividade)'''
        import os

        dir = os.getcwd()
        rats_files_names = os.listdir(dir+'/arquivos/RAT')
        
        rats_files = list()
        rats_viatura_files = list()
        rats_efetivo_files = list()
        rats_produtividade_files = list()

        if only_not_imported:
            df_imported_files = pd.read_sql_table('tbl_imported_files', 'sqlite:///gdo.db')
            rats_files_names = [file_name for file_name in rats_files_names if file_name not in df_imported_files['0'].values ]

        for file_name in rats_files_names:
            is_rat = 'REDS_RAT_2' in file_name
            is_rat_viatura = 'RAT_VIATURA' in file_name
            is_rat_efetivo = 'RAT_EFETIVO' in file_name
            is_rat_produtividade = 'RAT_Produtividade' in file_name
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
    
    def read_files(self, files_names, rat_data=True):
        if not rat_data:
            nrats_on_db = get_nrats_on_db()
        if len( files_names) < 1:
            raise Exception('Não há arquivos csv novos para serem inseridos no banco de dados.')
        for i in range( len(files_names) ):    
            if i == 0:        
                df = pd.read_csv('arquivos/RAT/' + files_names[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
                df = df.applymap(lambda x: x.strip() if type(x) == str else x)    
                if rat_data:
                    df['MUNICIPIO'] = df['MUNICIPIO'].astype('category')
                    df = self.filter_23(df)
                else:
                    df = df[
                        df.iloc[:,0].isin(nrats_on_db)
                    ]
            else:
                df_aux = pd.read_csv('arquivos/RAT/' + files_names[i], error_bad_lines=False, sep='|', quoting=csv.QUOTE_NONE)
                df_aux = df_aux.applymap(lambda x: x.strip() if type(x) == str else x)
                if rat_data:
                    df_aux['MUNICIPIO'] = df_aux['MUNICIPIO'].astype('category')
                    df_aux = self.filter_23(df_aux)
                else:
                    df_aux = df_aux[
                        df_aux.iloc[:,0].isin(nrats_on_db)
                    ]
                df = pd.concat([df, df_aux])
        return df
    
    def filter_23(self, df):
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