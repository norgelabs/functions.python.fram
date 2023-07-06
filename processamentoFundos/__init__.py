import logging

import pandas as pd
import pyodbc  
import numpy as np
import requests
import json
import os

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    token = "Bearer eyJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJQaXBlZnkiLCJpYXQiOjE2ODMwNTYyMDUsImp0aSI6IjY4N2FmNzZmLTlhYmItNDNkYi1hNDFmLWNmNmM4M2VhYjhlNyIsInN1YiI6MzAyMTM0NDk5LCJ1c2VyIjp7ImlkIjozMDIxMzQ0OTksImVtYWlsIjoicmFuaWVyZS5zYW50b3NAbm9yZ2VsYWJzLmNvbSIsImFwcGxpY2F0aW9uIjozMDAyNDcyMzIsInNjb3BlcyI6W119LCJpbnRlcmZhY2VfdXVpZCI6bnVsbH0.qxZLyCxvlt_o3qV0rJ9F2J58paJ0WbB2_HHLumLjZuscC_Nbp_tg349Xrk5MqfI0-XmXhZzbspjDG372qvSniw"
    url = "https://api.pipefy.com/graphql"
    destination_phase_id = 318519205
    phase_processamento = 318519204
    phase_concluido = 318519206
    connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=10.1.231.106,1433;Database=FINANCIAL_FRAMCAPITAL;UID=userfram;PWD=Help123!@#;"
    # connection_string = "Driver={Devart ODBC Driver for SQL Server};Server=10.1.231.106;Database=FINANCIAL_FRAMCAPITAL;UID=userfram;PWD=Help123!@#;"



    data_JSON =  """
        {
            "data":{
                "action":"card.move",
                "from":{
                    "id":318519969,
                    "name":"Abertura"
                },
                "to":{
                    "id":318519204,
                    "name":"Processamento"
                },
                "moved_by":{
                    "id":120,
                    "name":"Pipebot",
                    "username":"pipebot",
                    "email":"pipebot@pipefy.com",
                    "avatar_url":"https://gravatar.com/avatar/18df131953ca09a802848bf3f8dbf83b.png?s=144&d=https://pipestyle.staticpipefy.com/v2-temp/illustrations/avatar.png"
                },
                "card":{
                    "id":726989094,
                    "title":"187",
                    "pipe_id":"187"
                }
            }
        }
        """

    req_body = req.get_json()
    # config = json.loads(data_JSON)
    print(req_body["data"]["to"]["id"])

    # dados_fundo = config["data"]
    dados_fundo = req_body["data"]

    def ajusta_query(cursor_fetchall, columns): 
    
        dados = np.array(cursor_fetchall)
    
        return pd.DataFrame(dados, columns = columns)

    def ativo(dt):

        query = """Select p.Idcotista, p.Idcarteira From PosicaoCotista p
    join cliente c on c.idcliente = p.idcotista
    where c.idtipo in (510,500) and p.quantidade <> 0 and p.idcarteira NOT IN (437100, 438100,6782) and p.idcotista = """+dt+"""
    group by p.idcotista, p.idcarteira

        """
        columns = ['Cotista','Carteira']
    
        cursor.execute(query)
        return ajusta_query(cursor.fetchall(), columns)

    def moveCardPipefy(url, id):  # função que move um card de fase (coluna) no Pipefy
        body = {"query":"mutation {\nmoveCardToPhase(input: {\nclientMutationId: \"123\", \ncard_id: "+str(id)+", \ndestination_phase_id: "+str(destination_phase_id)+"\n}) {\ncard {\ntitle\n}\n}\n}","variables":{}}
        header = {
            "accept": "application/json", 
            "content-type": "application/json", 
            "Authorization": token
        }
        req = requests.post(url, json = body, headers = header)
        return req.text

    def getItensPhasePipefy(url, phase_id): # função que busca todos os cards de uma fase (coluna) no Pipefy
        body = {"query":"query MyQuery {\n phase(id: "+str(phase_id)+") {\n cards {\n edges {\n node {\n id\n title\n }\n}\n}\n}\n}","variables":{}}
        header = {
            "accept": "application/json", 
            "content-type": "application/json", 
            "Authorization": token
        }
        req = requests.post(url, json = body, headers = header)
        return req.text

    if dados_fundo["to"]["id"] == phase_processamento:
        print(">>>>> CARD movido para Processamento")

        cnxn = pyodbc.connect(connection_string)
        cursor = cnxn.cursor()

        Fundo = dados_fundo["card"]

        try:
            At = ativo(f"{int(Fundo['title'])}")
        except ValueError:
            At = pd.DataFrame(columns = ['Cotista','Carteira'])
        
        if len(At) == 0:

            print('processa (move card)')
            moveCardPipefy(url, Fundo['id'])

            print('testa fundos q estão parado em processamento')

            dados = json.loads(getItensPhasePipefy(url, phase_processamento)) #buscando cards na coluna 'Processamento'
            array_ret = dados['data']['phase']['cards']['edges']

            # criando array de OBJs dos cards que estão na coluna 'Processamento'
            if len(array_ret) > 0:
                new_array = []
                for k in range(len(array_ret)):
                    new_array.append(array_ret[k]['node'])

                # print(new_array)
                up = pd.DataFrame(new_array, columns=['id', 'title'])

                # array com fundos da coluna 'Concluído'
                # se vier vazia, fundo não processa
                dados_validacao = json.loads(getItensPhasePipefy(url, phase_concluido))
                array_aux = dados_validacao['data']['phase']['cards']['edges']
                # array_comp = ['313', '53421', '560']

                if len(array_aux) > 0:
                    array_comp = []
                    for k in range(len(array_aux)):
                        array_comp.append(array_aux[k]['node']['title'])

                    for index, row in up.iterrows():
                        At = ativo(f"{int(row['title'])}")
                        
                        for i in array_comp:
                            num = 0
                            for x in range(len(At)):
                                if At.loc[num, 'Carteira'] == int(i):
                                    At.drop([num], axis=0, inplace=True)
                                    At.reset_index(inplace=True)
                                    At = At.drop("index", axis='columns')
                                    print(At)
                                    num = 0
                                else:
                                    num=num+1

                        if len(At) == 0:
                            print('processa')
                            moveCardPipefy(url, row['id']) # PAREI AQUI! TRAZER O ID DO CARD
                            print('testa fundos q estão parado em processamento')
                        else:
                            print('faz nada (mantem o fundo na coluna processamento)')

            
        else:
            print('compara arrays e testa novamente len(array) = 0')

            # array com fundos da coluna 'Concluído'
            dados_validacao = json.loads(getItensPhasePipefy(url, phase_concluido))
            array_aux = dados_validacao['data']['phase']['cards']['edges']
            # array_comp = ['313', '53421', '560']

            print(At)

            # se vier vazia, fundo não processa
            if len(array_aux) > 0:
                array_comp = []
                for k in range(len(array_aux)):
                    array_comp.append(array_aux[k]['node']['title'])

                for i in array_comp:
                    num = 0
                    for x in range(len(At)):
                        if At.loc[num, 'Carteira'] == int(i):
                            At.drop([num], axis=0, inplace=True)
                            At.reset_index(inplace=True)
                            At = At.drop("index", axis='columns')
                            print(At)
                            num = 0
                        else:
                            num=num+1

                if len(At) == 0:
                    print('processa')
                    moveCardPipefy(url, Fundo['id'])
                    
                    print('testa fundos q estão parado em processamento')
                else:
                    print('faz nada (mantem o fundo na coluna processamento)')
    

    return func.HttpResponse(
            "This HTTP triggered function executed successfully",
            status_code=200
    )
