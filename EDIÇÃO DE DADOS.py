from tratamento_de_arquivos_e_dados.tratador_de_arquivos import tratador_de_arquivos

df_rqv_23 = tratador_de_arquivos().get_rqv_23()
df_rqv_23_por_mes_ee = pd.pivot_table(df_rqv_23, index='CIA',columns=['ANO', 'MES'],values='EFETIVIDADE_RQV',aggfunc='sum')
df_rqv_23_por_mes_efet = pd.pivot_table(df_rqv_23, index='CIA',columns=['ANO', 'MES'],values='EFICIENCIA_E_EFICACIA_RQV',aggfunc='sum')
    

df_ols_23 = tratador_de_arquivos().get_ols_23()
df_ols_23_por_mes = pd.pivot_table(df_ols_23, index='CIA',columns=['ANO', 'MES'],values='EFETIVIDADE_OLS',aggfunc='sum')

df_pog_23 = tratador_de_arquivos().get_dados_pog_23()
df_pog_23_por_mes = pd.pivot_table(df_pog_23, index='CIA',columns=['ANO', 'MES'],values='NAT.CODIGO',aggfunc='count')

df_ic_23 = tratador_de_arquivos().get_dados_ic_23()
df_ic_23_por_mes = pd.pivot_table(df_ic_23, index='CIA',columns=['ANO', 'MES'],values='NAT.CODIGO',aggfunc='count')



for df in [ df_rqv_23, df_ols_23, df_pog_23, df_ic_23 ]:
    df = df[]