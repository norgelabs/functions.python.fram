import logging

import azure.functions as func

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from io import StringIO

import pandas as pd
import pyodbc
import re
import numpy as np
import time
import datetime
import math
import os
import json
import base64
import requests

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    pd.set_option('display.max_rows', None)

    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    config_path = '\\'.join([ROOT_DIR, 'config.json'])

    # read config file
    with open(config_path) as config_file:
        config = json.load(config_file)
        config = config['main']

    sharepoint_base_url = config['sharepoint_base_url']
    sharepoint_user = config['sharepoint_user']
    sharepoint_password = config['sharepoint_password']
    folder_in_sharepoint = config['folder_in_sharepoint']
    folder_recon_carteiras = config['folder_recon_carteiras']
    connection_string = config['connection_string']
    security_token = config['security_token']
    endpoint_send_file = config['endpoint_send_file']

    #Constructing Details For Authenticating SharePoint

    auth = AuthenticationContext(sharepoint_base_url)

    auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
    ctx = ClientContext(sharepoint_base_url, auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    #print('Connected to SharePoint: ',web.properties['Title'])

    ### bloco para utilizar dados de data e tempo;

    dt = datetime.datetime.now()

    if dt.weekday() == 0:  #corrige nas segundas feiras, fazendo D-1 ser sexta e não domingo.
        
        dt = dt - datetime.timedelta(3)
        
    else:
        
        dt = dt - datetime.timedelta(1)
        
    d_limit = dt
    dt = dt.strftime('%Y%m%d')
    # dt = '20230322'

    print(dt)

    ### SÓ USAR PARA SELECIONAR DATA ESPECIFICA!!
    #dt = input("Digite a data base desejada (yyyyMMdd): ")

    # variavel con recebe a conexão com o servidor do SQL
    try:
        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()
    except:
        return func.HttpResponse(
            json.dumps({ "status":"error", "message":"Failed to connect database"}),
            status_code=500
        )
    ### funções criadas para o script.

    def ajusta_query(cursor_fetchall, columns):  #função que formata para DataFrame os dados importados da britech
        dados = np.array(cursor_fetchall)
        return pd.DataFrame(dados, columns = columns)
    
    def bri_titulos():   # são os IFS que estão nas tabelas de titulos, sem necessariamente em uma carteira. Direto no banco de Titulos e Papeis.
    
        query = """SELECT trf.IdTitulo, trf.Descricao, trf.DataEmissao, trf.DataVencimento, trf.CodigoCetip, pp.Descricao AS TipoIF
                    From TituloRendaFixa trf
                    JOIN PapelRendaFixa pp on pp.IdPapel = trf.IdPapel
                    WHERE trf.DataVencimento > GETDATE() 
                    AND trf.CodigoCetip != ' ' 
                    AND pp.Descricao not LIKE '%LFT%'
                    AND pp.Descricao not LIKE '%NTN%'
                    AND pp.Descricao not LIKE '%LTN%'
                """
        
        columns = ['IdTitulo_bri', 'Descricao', 'DataEmissao', 'DataVencimento', 'IF', 'Tipo IF bri']
        
        cursor.execute(query)
        return ajusta_query(cursor.fetchall(), columns)

    #################


    def carteirasfundos():
        
        query = """select cli.IdCliente, p.Cpfcnpj
        from cliente cli
        INNER JOIN Pessoa p on p.IdPessoa = cli.IdCliente
        where cli.idgrupoprocessamento in (7, 8, 14, 15, 23)
        and cli.statusativo = 1"""
        
        columns = ['IdCliente', 'Cpfcnpj']
        
        cursor.execute(query)
        return ajusta_query(cursor.fetchall(), columns)


    def clean_IFCODE(array):
        
        clean_list = [IF.replace('#', '') for IF in array]
        return clean_list

    def export_df(df, nome):
        rnd = np.random.randint(100000)
        rnd = str(rnd)
        # edit df.to_excel(r'C:\Users\jrossetto\OneDrive - FramCapital\Documentos\RECON_CETIP\\'+nome+dt+'_random_' +rnd+ '.xlsx')
        df.to_excel(r'.\conciliacao\temp_files\\'+nome+dt+'_random_' +rnd+ '.xlsx')
        

    def formata_data(pd_Serie): # função para jogar dota do formato AAAAmmdd para AAAA/mm/dd  
        novas_datas = []
        for data in pd_Serie:
            data = data[:4]+"/"+data[4:6]+"/"+data[6:]
            novas_datas.append(data)
        
        return novas_datas

    def ClientesAtivosBritech():

        #exceção de carteira que nao tenha posição na britech, mas tenha na cetip. 
        
        query = """Select cli.IdCliente, cli.Nome, p.Cpfcnpj FROM cliente cli 
        JOIN Pessoa p ON p.IdPessoa = cli.IdCliente where StatusAtivo = 1"""
        
        columns = ['IdCliente', 'Nome', 'cpfcnpj']
        
        cursor.execute(query)
        return ajusta_query(cursor.fetchall(), columns)
        

    def sendFileToAutomate(url, fileBase64, name):  #função que envia o excel gerado (base64) para o endpoint do Automate
        body = {
            "fileBase64": fileBase64,
            "fileName": name,
            "sharePointPath": "/caminho/arquivos",
            "securityToken": security_token
        }
        req = requests.post(url, json = body)
        return req

    ### DF para ver se tem titulo parado na conta própria:

    def le_dados_cetip_DPOSCUSTO(caminho):    #caminho == local do arquivo txt. obs: aparece como .tsv ao baixar da CETIP
        # 18020_220927_DPOSICAOCUSTODIA.CETIP
        
        df_b3_custodia = pd.read_csv(caminho, header = None, encoding = 'mbcs', sep = ";", dtype = str, decimal = ',',
        usecols = [1, 3, 4, 8, 9, 13], 
        names = ['Conta', 'Tipo_IF', 'IF', 'DataEmissao', 'DataVencimento', 'Quantidade'])
        
        df_b3_custodia.loc[:,'Quantidade'] = df_b3_custodia.loc[:,'Quantidade'].apply(lambda x: x.replace(',','.'))
        df_b3_custodia.loc[:,'Quantidade'] = df_b3_custodia['Quantidade'].astype(float)
        
        
        return df_b3_custodia


    arq = folder_in_sharepoint+'18020_'
    arq = arq + dt[2:] + "_DPOSICAOCUSTODIA.CETIP"

    try:
        file_response = File.open_binary(ctx, arq)
        s = str(file_response.content, "mbcs")
        _data = StringIO(s)

        df_b3_custodia = le_dados_cetip_DPOSCUSTO(_data)
    except FileNotFoundError as error:
        return func.HttpResponse(
            json.dumps({ "status":"error", "message":"failed to read or file not found: "+arq+" | "+error}),
            status_code=500
        )

    # arq = r"C:\Users\IvoSchettini\Projetos\Repositorio\envio_de_dados_com_python_para_SharePoint-main\temp_files\18020_"
    # arq = arq + dt[2:] + "_DPOSICAOCUSTODIA.CETIP"

    # df_b3_custodia = le_dados_cetip_DPOSCUSTO(arq)

    df_parado_na_propria = df_b3_custodia.where(df_b3_custodia['Conta'] == '18020003').dropna(how = 'all').fillna("NA")
    cff_indx = df_parado_na_propria.where(df_parado_na_propria['Tipo_IF'] == 'CFF').index
    df_parado_na_propria.drop(cff_indx, inplace = True)


    # In[10]:


    ## chamando dataframe com papeis britech (dados gerais de IF).

    papeis_bri = bri_titulos().set_index("IF")
    papeis_bri = papeis_bri['Tipo IF bri']


    # In[12]:


    ## remover o efeito de conta 3os (que repete IF)
    df_b3_custodia_conso = df_b3_custodia.groupby("IF").sum()


    # ### --------

    # ## DataFrame BRITECH (Carteiras Adm e Fundos)

    # In[13]:


    def britech_custodia(dt):
        
        query = """SELECT prf.DataHistorico, prf.IdCliente, p.Nome, p.Cpfcnpj, prf.IdTitulo, sum(prf.Quantidade), prf.DataVencimento,
                trf.Descricao, trf.IdPapel, trf.CodigoCetip
                FROM PosicaoRendaFixaHistorico prf
                JOIN TituloRendaFixa trf ON trf.IdTitulo = prf.IdTitulo
                JOIN Pessoa p ON p.IdPessoa = prf.IdCliente
                WHERE prf.DataHistorico = '""" +dt+ """' AND CodigoCetip not in (' ', '760199', '210100', '100000')
                GROUP BY  prf.DataHistorico, prf.IdCliente, p.Nome, prf.IdTitulo, prf.DataVencimento, trf.Descricao, trf.IdPapel, trf.CodigoCetip, p.Cpfcnpj
                ORDER BY prf.IdCliente
                """


        columns = ['DataHistorico', 'IdCliente', 'Nome', 'cpfcnpj', 'IdTitulo_bri', 'Quantidade', 
                'DataVencimento', 'Descricao', 'IdPapel', 'IF']

        
        cursor.execute(query)
        return ajusta_query(cursor.fetchall(), columns)

    ### cleaning
    df_bri_custodia = britech_custodia(dt)
    df_bri_custodia.loc[:,'IdCliente'] = df_bri_custodia.loc[:,'IdCliente'].astype(str)
    df_bri_custodia.loc[:,'Quantidade'] = df_bri_custodia['Quantidade'].astype(float)
    df_bri_custodia.loc[:,'IF'] = df_bri_custodia.loc[:,'IF'].str.strip()
    df_bri_custodia.loc[:,'DataVencimento'] = df_bri_custodia['DataVencimento'].apply(lambda x: x.date())

    ## remover ids dos fundos com custódia fora: sherman, ravi, olso, etc.
    # RAVI, 520411 
    # SHERMAN FI, 469 
    # AMUNDSEN, 187, 455, 6781, 6782
    # OLSO, 488
    # HANSSEN, 305, 306, 312
    fdos_drop = ['520411', '459', '187', 
                '455', '6781', '6782', 
                '488', '305', '306']

    ## esse dataframe da britech é a base para todos os demais. A partir dele, criaremos o df_bri_custodia_consolidado e o df_bri_custodia_analitico
    df_bri_custodia.loc[:, 'IdTitulo_bri'] = df_bri_custodia['IdTitulo_bri'].astype(str)
    df_bri_custodia = df_bri_custodia.set_index('IdCliente').drop(labels=fdos_drop, axis = 0, errors = 'ignore').reset_index()


    # In[14]:


    idx_drop_cdca = papeis_bri[papeis_bri == "CDCA"].index
    idx_drop_cdca = df_bri_custodia[df_bri_custodia["IF"].apply(lambda x: True if x in idx_drop_cdca else False)].index
    df_bri_custodia.drop(index = idx_drop_cdca, inplace = True)


    # In[ ]:





    # In[15]:


    ### britech consolidado
    df_bri_custodia_consolidado = df_bri_custodia.groupby(['IF']).sum()

    ### britech analitico
    df_bri_custodia_analitico_base = df_bri_custodia.copy()


    # ## Dados da conta de 3os - FDOS

    # In[16]:


    # limpando o CPF de pontuação.
    def fix_CPF(pd_series):

        pd_series= pd_series.apply(lambda x: x.replace(".", ""))
        pd_series= pd_series.apply(lambda x: x.replace(",", ""))
        pd_series= pd_series.apply(lambda x: x.replace("-", ""))
        pd_series= pd_series.apply(lambda x: x.replace("/", ""))
        pd_series= pd_series.str.strip()
        
        return pd_series

    def dadoscadastrais_fundos(arq):
        # arquivo ...25049_221014_DDADOS.CETIP21-SAP
        
        dadoscadastrais_fundos = pd.read_csv(arq, delimiter=";",
            encoding = "ISO-8859-1", dtype = str, header = 0,
            usecols = [0,1,2,3], names = ['conta', 'nome', 'cnpj', 'nome2'])

            
        return dadoscadastrais_fundos
        
    # arq = r"C:\Users\IvoSchettini\Projetos\Repositorio\envio_de_dados_com_python_para_SharePoint-main\temp_files\25049_"
    # arq = arq + dt[2:] + "_DDADOS.CETIP21-SAP"

    # dadoscadastrais_fundos = dadoscadastrais_fundos(arq)

    arq = folder_in_sharepoint+'25049_'
    arq = arq + dt[2:] + "_DDADOS.CETIP21-SAP"

    try:
        file_response = File.open_binary(ctx, arq)
        s = str(file_response.content, "ISO-8859-1")
        _data = StringIO(s)
        dadoscadastrais_fundos = dadoscadastrais_fundos(_data)
    except FileNotFoundError as error:
        return func.HttpResponse(
            json.dumps({ "status":"error", "message":"failed to read or file not found: "+arq+" | "+error}),
            status_code=500
        )

    dadoscadastrais_fundos = dadoscadastrais_fundos.drop_duplicates('cnpj')
    dadoscadastrais_fundos.loc[:,'cnpj'] = fix_CPF(dadoscadastrais_fundos.loc[:,'cnpj'])
    dadoscadastrais_fundos.set_index('nome2', inplace = True)


    # In[17]:


    #arq = 25049_221010_DPOSICAOCUSTODIA.CETIP

    # arq = r"C:\Users\IvoSchettini\Projetos\Repositorio\envio_de_dados_com_python_para_SharePoint-main\temp_files\25049_"
    # arq = arq + dt[2:] + "_DPOSICAOCUSTODIA.CETIP"

    # df_b3_custodia_fundos = pd.read_csv(arq, header = None, sep = ';', encoding = 'mbcs', usecols = [0, 1, 3, 4, 8, 9, 13],
    #             names = ["Nome", "Conta", "Tipo_IF", "IF", "DataEmissao",
    #                      "DataVencimento", "Quantidade"])

    arq = folder_in_sharepoint+'25049_'
    arq = arq + dt[2:] + "_DPOSICAOCUSTODIA.CETIP"

    try:
        file_response = File.open_binary(ctx, arq)
        s = str(file_response.content, "mbcs")
        _data = StringIO(s)
        df_b3_custodia_fundos = pd.read_csv(_data, header = None, sep = ';', encoding = 'mbcs', usecols = [0, 1, 3, 4, 8, 9, 13],
                names = ["Nome", "Conta", "Tipo_IF", "IF", "DataEmissao",
                        "DataVencimento", "Quantidade"])
    except FileNotFoundError as error:
        return func.HttpResponse(
            json.dumps({ "status":"error", "message":"failed to read or file not found: "+arq+" | "+error}),
            status_code=500
        )

    df_b3_custodia_fundos.loc[:, 'Quantidade'] = df_b3_custodia_fundos.loc[:, 'Quantidade'].apply(lambda x: x.replace(',','.')).astype(float)
    df_b3_custodia_fundos.set_index('Nome', inplace = True)
    ###
    df_b3_custodia_fundos = df_b3_custodia_fundos.join(dadoscadastrais_fundos[['cnpj', 'nome']]).reset_index()

    #remover cota de fundo;


    # In[18]:


    df_b3_custodia_fundos.drop(index = df_b3_custodia_fundos[df_b3_custodia_fundos['Tipo_IF'] == "CFF"].index, inplace = True)


    # ## Consolidado dos fundos

    # In[19]:


    ### consolidado dos fundos:
    df_b3_custodia_fundos_consolidado =df_b3_custodia_fundos[["IF", "Quantidade"]].groupby("IF").sum()


    # ## Consolidado B3 (Fdos + Adm)

    # In[20]:


    df_b3_custodia_conso_geral = pd.concat([df_b3_custodia_fundos_consolidado, df_b3_custodia_conso])
    df_b3_custodia_conso_geral = df_b3_custodia_conso_geral.reset_index().groupby("IF").sum()


    # ## Consolidado B3 + Britech

    # In[21]:


    ### aglutinar agora britch com cetip

    df_conso_geral = df_b3_custodia_conso_geral.join(df_bri_custodia_consolidado, lsuffix = '_B3', how = 'outer')
    df_conso_geral['Diferenca (B3 - Britech)'] = df_conso_geral['Quantidade_B3'].fillna(0) - df_conso_geral['Quantidade'].fillna(0)
    df_conso_geral.fillna("NA", inplace = True)
    #df_conso_geral.head()


    # In[22]:


    df_apenas_bri = df_conso_geral.where(df_conso_geral['Quantidade'] == -df_conso_geral['Diferenca (B3 - Britech)']).dropna(how = 'all')


    # In[ ]:





    # ### Ativos que só existem na CETIP:
    # 

    # In[23]:


    df_apenas_cetip = df_conso_geral.where(df_conso_geral['Quantidade_B3'] == df_conso_geral['Diferenca (B3 - Britech)']).dropna()


    # ### Analítico de Custódia

    # In[24]:


    # britech dataframe.

    df_bri_custodia_analitico = df_bri_custodia_analitico_base.copy()


    # In[ ]:





    # In[25]:


    ## DF CETIP ANALITICO.
    def le_dados_cetip(arquivo):    #caminho == local do arquivo txt. obs: aparece como .tsv ao baixar da CETIP
        # arquivo ...DPOSCUSTANALITICO_1.SIC
        headers = ["Sistema", "Conta Cliente", "Nome da Conta", "Tipo Pessoa", "cpfcnpj",
            "Comitente", "Origem", "Tipo do Ativo", "IF", "Código ISIN",
            "Tipo Carteira", "Quantidade", "Status da Conta", "Código Participante",
            "Conta Depósito", "Campo reservado_1", "Campo reservado_2", "Controle Interno",
            "Tipo Regime", "Eventos via Cetip", "Campo reservado_3"]
        
        df_cetip = pd.read_csv(
            arquivo, delimiter=";", encoding = "ISO-8859-1", 
            skiprows=1, names=headers, dtype = str
            )
        
        df_cetip = df_cetip.iloc[:-1,:]
    
        return df_cetip

    # arq = r"C:\Users\IvoSchettini\Projetos\Repositorio\envio_de_dados_com_python_para_SharePoint-main\temp_files\18020_"
    # arq = arq + dt[2:] + "_DPOSCUSTANALITICO.SIC"
    # df_anacust = le_dados_cetip(arq)

    arq = folder_in_sharepoint+'18020_'
    arq = arq + dt[2:] + "_DPOSCUSTANALITICO.SIC"

    try:
        file_response = File.open_binary(ctx, arq)
        s = str(file_response.content, "ISO-8859-1")
        _data = StringIO(s)
        df_anacust = le_dados_cetip(_data)
    except FileNotFoundError as error:
        return func.HttpResponse(
            json.dumps({ "status":"error", "message":"failed to read or file not found: "+arq+" | "+error}),
            status_code=500
        )

    #cleaning.
    df_anacust.loc[:,'IF'] = df_anacust.loc[:,'IF'].str.strip()
    df_anacust_cetip  = df_anacust.loc[:, ['cpfcnpj', 'Comitente', 'Tipo do Ativo', 'IF', 'Quantidade']]
    df_anacust_cetip['Quantidade'] = df_anacust_cetip['Quantidade'].astype(str).apply(lambda x: x.replace(',','.')).astype(float)
    df_anacust_cetip.loc[:,'cpfcnpj'] = fix_CPF(df_anacust_cetip.loc[:,'cpfcnpj']) #aqui m,sm?

    #Copy
    df_anacust_cetip_base = df_anacust_cetip.copy()

    # dropando dados de CFF
    ind_2drop = df_anacust_cetip[df_anacust_cetip["Tipo do Ativo"].apply(lambda x: x.strip()) == "CFF"].index

    df_anacust_cetip.drop(index = ind_2drop.values, inplace = True)
    #
    df_anacust_cetip.sort_values("IF", inplace = True)


    # In[26]:


    ### ja que é analitico, fazer multiIndex com cpfcnpj e codigo IF. Manter DF BASE para coletar dados depois.
    # trabalharemos com df_anacust_cetip e df_bri_custodia_analitico_base


    # In[27]:


    cols = ['cpfcnpj', 'IF', 'Quantidade']
    df_anacust_cetip = df_anacust_cetip[cols].groupby(["cpfcnpj", "IF"]).sum()

    cols3 = ["IF", "Quantidade", "cnpj"]
    df_b3_custodia_fundos_analitico = df_b3_custodia_fundos[cols3].groupby(["cnpj", "IF"]).sum()

    cols2 = ['cpfcnpj', 'IF', 'Quantidade']
    df_bri_custodia_analitico = df_bri_custodia_analitico[cols2].groupby(["cpfcnpj", "IF"]).sum()


    # #### Batimento dos fundos

    # In[28]:


    pj_fdos = df_b3_custodia_fundos['cnpj'].unique()


    # In[29]:


    ### filtrando apenas PJs que estão no arq custódia da B3.

    A = []
    for i in pj_fdos:
        if i in df_bri_custodia_analitico.index:
            A.append(i)
        else:
            None
            
    #__________
    df = df_bri_custodia_analitico.copy()
    df.index.rename(['cnpj', 'IF'], inplace = True)
    #
    df_anacust_geral_fdos = df_b3_custodia_fundos_analitico.join(df.loc[A], how = "outer", lsuffix = '_B3')
    df_anacust_geral_fdos['B3 - Britech'] = df_anacust_geral_fdos['Quantidade_B3'].fillna(0) - df_anacust_geral_fdos['Quantidade'].fillna(0)
    df_anacust_geral_fdos = df_anacust_geral_fdos.fillna("NA")


    # In[ ]:





    # In[ ]:





    # #### Batimento carteiras adm 

    # In[32]:


    ### unir com outer, e dropar id de fundos.

    df_anacust_geral_adm = df_anacust_cetip.join(df_bri_custodia_analitico, lsuffix ="_B3", how = 'outer')
    df_anacust_geral_adm['B3 - britech'] = df_anacust_geral_adm["Quantidade_B3"].fillna(0) - df_anacust_geral_adm['Quantidade'].fillna(0)


    # In[33]:


    ## criando uma coluna apenas com nome do atrivo britech

    p = re.compile('(.+)(?=-)')
    funct = lambda x: p.search(x).group().strip() if p.search(x) else x 
    df_bri_custodia['Descricao2'] = df_bri_custodia['Descricao'].apply(funct)


    # In[ ]:





    # ### SELEÇÃO DE DADOS PARA OS IFs -> BRITECH

    # In[ ]:





    # In[56]:


    # SELEÇÃO DE DADOS PARA OS IFs -> BRITECH

    df_dados_ativos_bri = df_bri_custodia.set_index("IF")[['Descricao2', 'DataVencimento']]
    df_dados_ativos_bri = df_dados_ativos_bri.rename(columns = {'Descricao2': 'Nome do Ativo Britech', 
                                        'DataVencimento': 'DataVencimento na Britech'})
    df_dados_ativos_bri = df_dados_ativos_bri.groupby(df_dados_ativos_bri.index).first()

    ## adcionar os dados complementares agora. Nome Curto, Id britech, tipo papel,  data emissão, data venc, qtds e dif.

    df_dados_ativos_b3 = df_b3_custodia.set_index("IF")[['Tipo_IF', 'DataEmissao', 'DataVencimento']]

    # agora juntando com os IFs em Fundos, pois podem ter ativo lá que não tem em carteira ADM.
    df_dados_ativos_b3 = pd.concat([df_dados_ativos_b3, df_b3_custodia_fundos[['Tipo_IF', 'IF', 'DataEmissao', 'DataVencimento']].set_index("IF")])
    df_dados_ativos_b3 = df_dados_ativos_b3.groupby(df_dados_ativos_b3.index).first()
    df_dados_ativos_b3.head()

    ## b3 + britech
    df_dados_ativos_geral = df_dados_ativos_b3.join(df_dados_ativos_bri, how = 'outer')
    df_dados_ativos_geral.loc[:,'DataEmissao'] = df_dados_ativos_geral['DataEmissao'].astype(str).apply(lambda x: x[0:4]+"-"+x[4:6]+'-'+x[6:] if x!= 'nan' else "NA")
    df_dados_ativos_geral.loc[:,'DataVencimento'] = df_dados_ativos_geral['DataVencimento'].astype(str).apply(lambda x: x[0:4]+"-"+x[4:6]+'-'+x[6:] if x!= 'nan' else "NA")
    df_dados_ativos_geral.fillna("NA")
    df_dados_ativos_geral.head()


    # ### Add Dados nos DataFrames - comitentes e infos dos ativos
    # #### frame para tipo do papel.
    # #### dados de ativos
    # agora q temos dados dos IFs, juntar nos dataframes. Começando pelo analitico.

    df_anacust_geral_adm_f = df_anacust_geral_adm.join(df_dados_ativos_geral).iloc[:,[3,6,4,5,7,1,0,2]].fillna("NA")
    df_anacust_geral_adm_f = df_anacust_geral_adm_f.reset_index().sort_values('IF').reset_index(drop = True)
    df_anacust_geral_adm_f = df_anacust_geral_adm_f.rename(columns = {'Quantidade': 'Quantidade Britech', 
                                                                    'Quantidade_B3': 'Quantidade Cetip',
                                                                    'B3 - britech': 'Cetip - Britech',
                                                                    'Tipo_IF': 'Papel',
                                                                    'DataVencimento': 'DataVencimento Cetip',
                                                                    'DataEmissao': 'DataEmissao Cetip'})
    ##

    df_anacust_geral_adm_f = df_anacust_geral_adm_f.join(papeis_bri, on = 'IF')
    df_anacust_geral_adm_f.loc[:, 'Papel'] = df_anacust_geral_adm_f[['Tipo IF bri']].values
    df_anacust_geral_adm_f.drop(columns = 'Tipo IF bri', inplace = True)

    
    # #### DADOS PARA COMITENTES
    ## seleçao de dados para comitentes:
    # britech

    df_dados_comitentes_bri = df_bri_custodia[['cpfcnpj', 'IdCliente']].set_index('cpfcnpj')
    df_id_fundos = carteirasfundos().rename(columns = {'Cpfcnpj': 'cpfcnpj'}).set_index('cpfcnpj')
    df_dados_comitentes_bri = pd.concat([df_dados_comitentes_bri, df_id_fundos]).drop_duplicates()

    # carteiras adm.

    df_dados_comitentes_b3 = df_anacust_cetip_base[['cpfcnpj', 'Comitente']]
    df_dados_comitentes_b3 = df_dados_comitentes_b3.rename(columns = {'Comitente': 'nome'}).set_index('cpfcnpj')

    # para fundos.
    df_dados_comitentes_b3_fdos = df_b3_custodia_fundos[['nome', 'cnpj']].rename(columns = {'cnpj': 'cpfcnpj'}).set_index('cpfcnpj')
    df_dados_comitentes_b3_fdos.head()

    ## unindo os dados cetip.

    df_dados_comitentes_geral = pd.concat([df_dados_comitentes_b3, df_dados_comitentes_b3_fdos])

    # Linhas sem clienteId. 
    IdCliAtivos = ClientesAtivosBritech()
    IdCliAtivos.dropna(subset = "cpfcnpj", inplace = True)
    idx_drop = IdCliAtivos[IdCliAtivos['cpfcnpj'] == ""].index
    IdCliAtivos.drop(index = idx_drop, inplace = True)
    IdCliAtivos.set_index('cpfcnpj', inplace = True)

    df_dados_comitentes_geral_f = df_dados_comitentes_bri.drop_duplicates().join(df_dados_comitentes_geral.drop_duplicates(), how = 'outer')
    df_dados_comitentes_geral_f = df_dados_comitentes_geral_f.rename(columns = {'nome': 'Nome'})


    # dropar duplicados
    df_dados_comitentes_geral_f.drop_duplicates(inplace = True)



    # na l41, deu um problema. o fundo 316 está no indice do df_dados_comitentes_geral_f, mas não está em IdCliAtivos. Logo, podemos remover se nao estiver como ativo.
    IdsNaB3NaoAtivos = []
    for pjpf in df_dados_comitentes_geral_f.index.unique():
        
        if pjpf not in IdCliAtivos.index:
            
            IdsNaB3NaoAtivos.append(pjpf)        
    df_dados_comitentes_geral_f.drop(IdsNaB3NaoAtivos, inplace = True)



    # pegar valores com NA.
    cpf_dos_semID = df_dados_comitentes_geral_f.where(df_dados_comitentes_geral_f.isna()["IdCliente"] == True).dropna(how = "all").index
    df_dados_comitentes_geral_f.loc[cpf_dos_semID, "IdCliente"] = IdCliAtivos.loc[cpf_dos_semID, "IdCliente"]

    # ## FINALIZAÇÃO - Inserção de dados & correções

    # #### Finalização do DataFrame carteira Adm!

    # finalização do DataFrame carteira Adm!
    df_anacust_geral_adm_f = df_anacust_geral_adm_f.join(df_dados_comitentes_geral_f, on = 'cpfcnpj').fillna("NA").iloc[:, [1, -1, -2, 2, 3, 4, 5, 6, 7, 8, 9]]

    # dropar id de fundos!

    arr_id_fundos = df_id_fundos['IdCliente'].values
    df_anacust_geral_adm_f = df_anacust_geral_adm_f.set_index('IdCliente').drop(index = list(df_id_fundos['IdCliente'].values), errors = 'ignore')

    # selecionar somente quando a diferença de qtdds não for igual a 0.
    df_anacust_geral_adm_f = df_anacust_geral_adm_f[df_anacust_geral_adm_f['Cetip - Britech'] != 0].fillna("NA")

    ## SEM NOME
    ind = df_anacust_geral_adm_f.reset_index()
    ind = ind[ind['Nome'] == 'NA'].index
    MissNames = df_anacust_geral_adm_f.iloc[ind].index.unique()
    df_temp = df_bri_custodia.set_index('IdCliente').loc[MissNames]['Nome']
    df_anacust_geral_adm_f = df_anacust_geral_adm_f.reset_index()
    df_anacust_geral_adm_f.loc[ind, 'Nome'] = df_temp.values


    # In[ ]:





    # In[ ]:





    # #### Finalzição do DataFrame para Fundos!.

    # In[38]:


    df_dados_comitentes_geral_f = df_dados_comitentes_geral_f.groupby(df_dados_comitentes_geral_f.index).first()


    # In[39]:


    ### Finalzição do DataFrame para Fundos!.

    df_anacust_geral_fdos.index.names= ['cpfcnpj', 'IF']  
    #df_anacust_geral_fdos_f = 
    columns_ordem = ['Nome', 'IdCliente', 'Tipo_IF', 'Nome do Ativo Britech', 'DataEmissao', 'DataVencimento', 'DataVencimento na Britech',
                    'Quantidade_B3', 'Quantidade', 'B3 - Britech']

    df_anacust_geral_fdos_f = df_anacust_geral_fdos.join(df_dados_comitentes_geral_f).join(df_dados_ativos_geral).fillna("NA").loc[:,columns_ordem]
    df_anacust_geral_fdos_f = df_anacust_geral_fdos_f.rename(columns = {'Quantidade_B3': 'Quantidade Cetip', 
                                            'Quantidade': 'Quantidade Britech', 
                                            'B3 - Britech': 'Cetip - Britech', 
                                            'Tipo_IF': "Papel"}).reset_index().drop(columns = 'cpfcnpj')

    # remover aqueles que já estão netado.

    df_anacust_geral_fdos_f = df_anacust_geral_fdos_f[df_anacust_geral_fdos_f['Cetip - Britech'] != 0]


    # In[40]:


    datas_compara = df_anacust_geral_fdos_f['DataVencimento'] == df_anacust_geral_fdos_f['DataVencimento na Britech']
    datas_compara = datas_compara.apply(lambda x: 'X' if x == False else 'OK')


    # 
    # ## Adcionar dados aos outros data frames.

    # In[41]:


    ### VERIFICAÇÃO SE O IF SEM REGISTRO B3 ESTÁ EM CARTEIRA FUNDO/ADM.
    lis = {}
    for papel in df_apenas_bri.index:

        fundo_tem = papel in df_anacust_geral_fdos_f["IF"].unique()
        adm_tem = papel in df_anacust_geral_adm_f['IF'].unique()
        #
        lis[papel] = [adm_tem, fundo_tem]
        
    df_apenas_bri_posAdmFundo = pd.DataFrame(lis).T.rename(columns = {0: "Em Carteira Adm?", 1: "Em Carteira Fundo?"})


    # In[43]:


    ### adcionar dados aos outros data frames.

    #____
    #apenas britech
    #1. dropar IFs que estão somente em carteira de fundo!

    df_apenas_bri = df_apenas_bri.join(df_dados_ativos_geral[['Nome do Ativo Britech', 'DataVencimento na Britech']])

    df_apenas_bri = df_apenas_bri[['Nome do Ativo Britech', 'DataVencimento na Britech']]
    df_apenas_bri = df_apenas_bri.join(df_apenas_bri_posAdmFundo)

    #___________
    # apenas cetip
    df_apenas_cetip = df_apenas_cetip.join(df_dados_ativos_geral.iloc[:, [0,1,2]])[['Tipo_IF', 'DataEmissao', 'DataVencimento']]
    df_apenas_cetip.rename(columns = {'Tipo_IF': 'Papel'}, inplace = True)

    #______
    # parado na propria;
    df_parado_na_propria = df_parado_na_propria.set_index("Conta")


    # In[45]:


    def PU_IF(IF_PAPEL):    
        
        query = """select CodigoCetip, IdTitulo, Descricao, DataEmissao, DataVencimento, PUNominal
        FROM TituloRendaFixa
        where CodigoCetip = '"""+IF_PAPEL+"'"

        columns = ['IF', 'IdTitulo', 'Descricao', 'DataEmissao', 'DataVencimento', 'PUNominal']
        
        cursor.execute(query)
        return ajusta_query(cursor.fetchall(), columns)


    # In[ ]:





    # In[46]:


    # colocar PU dos ativos
    new_lis = []
    for IF in df_apenas_bri.index:
        
        dados = PU_IF(IF)
        new_lis.append(dados)

    df_apenas_bri_PU = pd.concat(new_lis).set_index("IF")

    df_apenas_bri = df_apenas_bri.join(df_bri_custodia_consolidado).join(df_apenas_bri_PU).drop(columns = ["DataVencimento na Britech", 'Descricao'])


 
    ## alteração do PUNominal (formatação), batimento data e troca do True/False
    df_apenas_bri.loc[:, "PUNominal"] = df_apenas_bri_PU.loc[:, "PUNominal"].astype(float)
    df_apenas_bri.replace(to_replace = True, value = "Sim", inplace = True)
    df_apenas_bri.replace(to_replace = False, value = "Não", inplace = True)

    ## add coluna: financeiro
    df_apenas_bri['Financeiro'] = df_apenas_bri['PUNominal'] * df_apenas_bri['Quantidade']

    ### remoção de CFF, pois não utilizaremos:

    ## apenas cetip
    df_apenas_cetip = df_apenas_cetip[df_apenas_cetip["Papel"] != 'CFF']

    ## analitico de custodia de fundos
    df_anacust_geral_fdos_f = df_anacust_geral_fdos_f[df_anacust_geral_fdos_f['Papel'] != 'CFF']
    df_anacust_geral_fdos_f.set_index("IF", inplace = True)

    ## outros ajustes fdos
    df_anacust_geral_fdos_f = df_anacust_geral_fdos_f.drop(columns = "Papel")
    df_anacust_geral_fdos_f = df_anacust_geral_fdos_f.rename(columns = {"Nome do Ativo Britech": "Papel"})

    # analitico de custodia de carteiras adm.
    df_anacust_geral_adm_f = df_anacust_geral_adm_f[df_anacust_geral_adm_f['Papel'] != 'CFF']
    df_anacust_geral_adm_f = df_anacust_geral_adm_f.set_index("IF")


    # In[50]:


    # Bloco para remover os Titulos que não estão em carteira alguma (Falso Positivo).

    def check_inNoWallet(IdTitulo):
        
        query = """SELECT IdCliente, IdTitulo, sum(Quantidade) 
                FROM PosicaoRendaFixaHistorico
                WHERE IdTitulo = """+ str(IdTitulo) + """
                and DataHistorico = ' """ + dt + """ ' 
                group by IdCliente, IdTitulo """
        columns = ['IdCliente',
                'IdTitulo',
                'Quantidade']
        
        cursor.execute(query)
        return cursor.fetchall()

    ###
    dici_inAnyWallet = {}
    for IdTitulo in df_apenas_bri['IdTitulo']:
        
        if len(check_inNoWallet(IdTitulo)) == 0:
            dici_inAnyWallet[IdTitulo] = False
            df_apenas_bri = df_apenas_bri[df_apenas_bri["IdTitulo"] != IdTitulo]

        else:
            dici_inAnyWallet[IdTitulo] = True
            
    #print(dici_inAnyWallet)


    # ### Writting in an excel file

    # In[53]:


    # escreve os arquivos em EXCEL;

    rnd = np.random.randint(100000)
    rnd = str(rnd)

    fileDir = r'.\conciliacao\temp_files\recon_carteiras'+dt+'.xlsx'

    with pd.ExcelWriter(fileDir) as writer:

        if not df_anacust_geral_fdos_f.empty:
            df_anacust_geral_fdos_f.to_excel(writer, sheet_name = 'recon analitico - fundos')
            
        if not df_anacust_geral_adm_f.empty:
            df_anacust_geral_adm_f.to_excel(writer, sheet_name = 'recon analitico - carteiras adm')
    
        if not df_parado_na_propria.empty:
            df_parado_na_propria.to_excel(writer, sheet_name = 'IFs_parados_cPropria')

        if not df_apenas_bri.empty:
            df_apenas_bri.to_excel(writer, sheet_name = 'Ativo sem registro B3')
        
        if not df_apenas_cetip.empty:
            df_apenas_cetip.to_excel(writer, sheet_name = 'Ativo sem registro BRITECH')

    # with open(fileDir, 'rb') as content_file:
    #     file_content = content_file.read()
    
    # name = os.path.basename(fileName)
    file_name = 'recon_carteiras'+dt+'.xlsx'

    with open(fileDir, "rb") as _file:
        fileBase64 = base64.b64encode(_file.read())

    try:
        sendFileToAutomate(endpoint_send_file, fileBase64, file_name)
    except:
        return func.HttpResponse(
        json.dumps({ "status":"error", "message":"Error send file"}),
        status_code=500
    )

    # Upload para o SharePoint
    # list_title = "Documents"

    # target_list = ctx.web.lists.get_by_title(list_title)
    # #info = FileCreationInformation()

    # libraryRoot = ctx.web.get_folder_by_server_relative_url(folder_recon_carteiras)

    # libraryRoot.upload_file(name, file_content).execute_query()

    os.remove(fileDir)

    return func.HttpResponse(
        json.dumps({ "status":"success", "message":"'recon_carteiras"+dt+".xlsx' file generated successfully!"}),
        status_code=200
    )