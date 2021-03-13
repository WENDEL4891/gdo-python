import pandas as pd
import gc
from tratamento_de_arquivos_e_dados.tratador_de_arquivos import tratador_de_arquivos
import logs

class dados_para_bd():
    def dados_dos_arquivos_para_bd(self, rat_ou_bos='RAT', tipo='geral', apenas_nao_importados=True):
        '''
            Insere os dados dos arquivos no Banco de Dados. São necessários os parâmetros adiante:
            1) rat_ou_bos -> aceita "rat", "bos", "RAT" ou "BOS".
            2) tipo -> aceita "geral", "viaturas", "efetivo" ou "produtividade".
            3) apenas_nao_importados -> aceita booleano. Define se serão importados todos os arquivos (refazer todo o banco de dados, False),
            ou apenas os que não foram importados ainda (complementar o banco de dados, True)
            Ambos os parâmetros, conjugados, servem para definir quais dados serão importados para o bd.
            O default é rat geral.
        '''
        rat_ou_bos_lower = rat_ou_bos.lower()
        rat_ou_bos_upper = rat_ou_bos.upper()

        logs.log_inicio_fim_metodo('Iniciando método dados_para_bd().dados_dos_arquivos_para_bd.')
        logs.log_inicio_fim_metodo('Dados tipo {} - {}'.format(rat_ou_bos_lower, tipo.lower()))
        logs.log('Iniciando método tratador_de_arquivos().get_nomes_de_arquivos()')
        nomes_de_arquivos = tratador_de_arquivos().get_nomes_de_arquivos(rat_ou_bos=rat_ou_bos, apenas_nao_importados=apenas_nao_importados)
        logs.log('Finalizando método tratador_de_arquivos().get_nomes_de_arquivos()')

        tipo_geral_argumento = True if tipo == 'geral' else False

        logs.log('Iniciando método tratador_de_arquivos().ler_arquivos()')
        df_rat = tratador_de_arquivos().ler_arquivos(nomes_de_arquivos['{}_{}'.format(rat_ou_bos_lower, tipo)], rat_ou_bos=rat_ou_bos_lower, tipo_geral=tipo_geral_argumento)
        logs.log('Finalizando método tratador_de_arquivos().ler_arquivos()')

        if tipo == 'geral':        
            logs.log('Iniciando método tratador_de_arquivos().processa_dados()')
            df_rat = tratador_de_arquivos().processa_dados(df_rat)        
            logs.log('Finalizando método tratador_de_arquivos().processa_dados()')

            logs.log('Iniciando método tratador_de_arquivos().get_classif() e tratador_de_arquivos().classifica_setor()')
            df_classif = tratador_de_arquivos().get_df_classif()
            df_rat['SETOR'] = df_rat.apply(lambda row: tratador_de_arquivos().classifica_setor(row, df_classif), axis=1)        
            logs.log('Finalizando método tratador_de_arquivos().get_classif() e tratador_de_arquivos().classifica_setor()')
            
            logs.log('Iniciando método tratador_de_arquivos().classifica_cia()')
            tratador_de_arquivos().classifica_cia(df_rat=df_rat)
            logs.log('Finalizando método tratador_de_arquivos().classifica_cia()')

        tabelas_bd = {
            'geral' : 'tbl_{}_geral'.format(rat_ou_bos_lower),
            'viaturas' : 'tbl_{}_viaturas'.format(rat_ou_bos_lower),
            'efetivo' : 'tbl_{}_efetivo'.format(rat_ou_bos_lower),
            'produtividade' : 'tbl_{}_produtividade'.format(rat_ou_bos_lower)
        }

        nome_tabela = tabelas_bd[tipo]
        
        df_rat.to_sql(nome_tabela, 'sqlite:///gdo.db', if_exists='replace', index=False)
        print('dados exportados para o db ok')

        del df_rat
        logs.log('df_rat deleted')
        gc.collect()

        acao_se_tabela_existir = 'replace' if ( tipo == 'geral' and apenas_nao_importados == False ) else 'append'

        logs.log('Iniciando a inclusão dos nomes dos arquivos importados no banco de dados')
        pd.DataFrame(nomes_de_arquivos['{}_{}'.format(rat_ou_bos_lower, tipo.lower())]).to_sql('tbl_arquivos_importados_{}'.format(rat_ou_bos_lower), 'sqlite:///gdo.db', if_exists=acao_se_tabela_existir, index=False)        
        logs.log('Finalizando a inclusão dos nomes dos arquivos importados no banco de dados')
        
        logs.log_inicio_fim_metodo('Finalizando método dados_para_bd().dados_dos_arquivos_para_bd.')