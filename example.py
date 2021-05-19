def insert_df_bd(params, commit=True):
    """ Insere os parametros no tabela do BD de forma otimizada

    Args:
        params (list): Lista dos dados lidos dos arquivos cmarg

    """
    try:
        ssql = f"""INSERT INTO 
                {SCHEMA}.{TABELA}([COLUNA1]
           ,[COLUNA2]
           ,[COLUNA3]
           ,[COLUNA4]
           ,[COLUNA5]
           ,[COLUNA6]
           ,[COLUNA7]
           ,[COLUNA8]
           ,[COLUNA9]
           ,[COLUNA10]
           ,[COLUNA11]) VALUES """
        print("Inserindo dados de PLD")
        con = conexao.get_conn()
        cur = con.cursor()
        cur.fast_executemany = True

        intervalo = 90
        markers = ["(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"]

        params_otimizada = []
        params_intervalo = []
        num = 1
        for i, p in enumerate(params):
            params_intervalo += p

            if num == intervalo:
                params_otimizada.append(params_intervalo)
                params_intervalo = []
                num = 1
            else:
                num += 1
        if params_otimizada:
            prepared_sql = ssql + ", ".join(markers * intervalo)
            try:
                cur.executemany(prepared_sql, params_otimizada)
            except Exception as e:
                print('Erro ao inserir dados no banco')
                print(traceback.format_exc())
                raise e

        if params_intervalo:
            try:
                # Agora faz o insert dos que sobraram e que não completaram o último intervalo
                cur.execute(ssql + ", ".join(markers * (num - 1)), params_intervalo)
            except Exception as e:
                print('Erro ao inserir dados no banco')
                # print(params_intervalo)
                print(traceback.format_exc())
                raise e

        if commit:
            cur.commit()

    except Exception as e:
        print(traceback.format_exc())
        raise e