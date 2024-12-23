import requests
import pandas as pd
import os
import matplotlib.pyplot as plt
import json
from tqdm import tqdm
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv('../keys.env')

def get_data(deputados):
    url_base = 'https://dadosabertos.camara.leg.br/api/v2/'
    url_complete =  url_base + deputados
    answer = requests.get(url_complete, params={"datainicio": '2024-08-01',
                                                "datafim": '2024-08-31'})
    #Salvando retorno da API
    deputados_json = answer.json()

    #ASalvando retorno de URL em um DataFrame
    df_deputados = pd.DataFrame(deputados_json['dados'])

    #Salvando DataFrame em um arquivo parquet
    df_deputados.to_parquet('../data/deputados.parquet')

    return df_deputados

#Gerando arquivo deputados.parquet e carregando df_deputados
df_deputados = get_data('deputados')


#Função para gerar gráfico de pizza
def generate_pie():
    #Lendo dados do arquivo parquet
    #df_deputados = pd.read_parquet('../data/deputados.parquet')
    prompt = f"""
Você é um cientista de dados trabalhando em um projeto de análise de dados para a Câmara dos Deputados. Escreva um código Python que faça o seguinte:

- Leia os dados do dataframe do caminho '../data/deputados.parquet' e armazene-os em um DataFrame chamado 'df_deputados'.
- Utilize a biblioteca matplotlib.pyplot para criar um gráfico de pizza que mostre o total e a porcentagem de deputados por partido, utilizando exclusivamente os dados do DataFrame 'df_deputados'. Inclua título e legendas no gráfico para facilitar a compreensão.
- A coluna 'siglaPartido' contém a sigla do partido de cada deputado.
- Salve o gráfico como uma imagem no caminho '../docs/distribuicao_deputados.png'.
- Após gerar o código, execute-o e verifique se o gráfico foi salvo no caminho especificado.
- Retorne somente o código Python que você escreveu para gerar o gráfico de pizza. Parte do princípio que todos os pacotes necessários já foram importados.
"""
    # Definir a chave de API do Gemini (use a chave fornecida pela sua conta)
    genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
    model = genai.GenerativeModel("gemini-1.5-flash")
    response = model.generate_content(prompt)
    analysis_code = response.text.replace("```python\n",'').replace("\n```",'')
    print(analysis_code)
    return analysis_code

grafico_pizza = generate_pie()
#exec(grafico_pizza)

#Função para gerar insights
def generate_insights():
    prompt = f"""
Você é um reporter que cobre a Câmara dos Deputados e está trabalhando em uma matéria sobre a distribuição de deputados por partido.
Crie insights com base nos dados do dataframe do caminho '../data/deputados.parquet'. A coluna 'siglaPartido' contém a sigla do partido de cada deputado.

###
Exemplo de insights:
O partido X (insira a sigla real do partido) é o partido com o maior número de deputados, representando 30% do total de deputados.
O partido Y (insira a sigla real do partido) é o partido com o menor número de deputados, representando 5% do total de deputados.
###

Salve pelo menos 5 insights em um arquivo JSON no caminho '../data/insights_distribuicao_deputados.json'. Eles devem conter pelo menos 2 parágrafos. Garanta que o arquivo json foi salvo corretamente no caminho especificado.
Importante: Não quero o código Python para gerar os insights, apenas os insights em si.

"""

    # Definir a chave de API do Gemini (use a chave fornecida pela sua conta)
    genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
    model = genai.GenerativeModel("gemini-1.5-flash")
    response = model.generate_content(prompt)
    print(response.text)
    insights = response.text.replace("```json\n",'').replace("\n```",'')
    with open ('../data/insights_distribuicao_deputados.json', 'w') as f:
        json.dump(insights, f)

generate_insights()

#Função para gerar despesas
def generate_expenses():
  # Lendo dados do arquivo parquet
    df_deputados = pd.read_parquet('../data/deputados.parquet')

    url_expenses = 'https://dadosabertos.camara.leg.br/api/v2/deputados/{id}/despesas'
    expenses = []  # Garantir que seja uma lista

    for id in df_deputados['id']:
        url_complete = url_expenses.format(id=id)
        answer = requests.get(url_complete, params={"ano": 2024, "mes": 8})
        expenses_json = answer.json()
        
        # Transformar a resposta em DataFrame e adicionar à lista
        df_expenses = pd.DataFrame(expenses_json['dados'])
        if not df_expenses.empty:  # Verificar se o DataFrame não está vazio
            df_expenses['id'] = id
            expenses.append(df_expenses)

        # Link para próxima página
        df_links = pd.DataFrame(expenses_json['links']).set_index('rel').href

        while 'next' in df_links.index:
            answer = requests.get(df_links['next'])
            expenses_json = answer.json()
            df_expenses = pd.DataFrame(expenses_json['dados'])
            
            if not df_expenses.empty:  # Verificar se o DataFrame não está vazio
                df_expenses['id'] = id
                expenses.append(df_expenses)

            # Atualizar os links para a próxima página
            df_links = pd.DataFrame(expenses_json['links']).set_index('rel').href

    # Concatenar os DataFrames na lista
    df_expenses = pd.concat(expenses, ignore_index=True)
    df_expenses = df_expenses[['id', 'tipoDespesa','dataDocumento', 'valorLiquido']]


    # Merge para trazer as informações de sigla e afins do deputado
    df_expenses = df_expenses.merge(df_deputados, on='id')

    # Agrupar por nome, tipo de despesa e data do documento
    df_expenses = df_expenses.groupby(['nome', 'tipoDespesa','dataDocumento'], as_index=False).sum()

    #Salvando em parquet
    df_expenses.to_parquet('../data/serie_despesas_diarias_deputados.parquet')

    
#generate_expenses()

#Gera 3 Insights sobre as despesas dos deputados

def generate_expenses_insights():
    prompt = f"""
Voê é um cientista de dados que está analisando as despesas dos deputados da Câmara dos Deputados. Crie insights somente com base nos dados do dataframe do caminho '../data/serie_despesas_diarias_deputados.parquet'.
Crie três analises gráficas usando python. 
Regras:
1 - Gere somente o código Python para gerar os gráficos.
2 - mMrque qualquer texto que não seja código como comentário.
3 - Salve os gráficos em arquivos no caminho '../docs/'.
4 - Garanta que os gráficos sejam claros e informativos.
5 - Garanta que todos os gráficos sejam salvos

Dados disponíveis no DataFrame:
- index: nome, tipoDespesa e dataDocumento
- valorLiquido: valor da despesa em reais

Além disso, salve pelo menos três insights em um arquivo JSON no caminho '../data/insights_despesas_deputados_raw.json'. Eles devem conter pelo menos 2 parágrafos. Não é necessário incluir o código Python utilizado para gerar os insights.
"""
    
    # Definir a chave de API do Gemini (use a chave fornecida pela sua conta)
    genai.configure(api_key=os.environ["GOOGLE_API_KEY"])
    model = genai.GenerativeModel("gemini-1.5-flash")
    response = model.generate_content(prompt)
    analysis = response.text.replace("```python\n",'').replace("\n```",'')
    print(analysis)
    return analysis


analysis = generate_expenses_insights()
#exec(analysis)

#Função para obter proposições

def get_proposicoes():
    import json

url_base = 'https://dadosabertos.camara.leg.br/api/v2/'
url_complete =  url_base + 'proposicoes'
codTema = [40, 46, 62]
df_proposicoes = None

for cod in codTema:
    answer = requests.get(url_complete, params={"dataInicio": '2024-08-01',
                                                "dataFim": '2024-08-31',
                                                "codTema": cod,
                                                "itens": 10})
    if df_proposicoes is None:
        df_proposicoes = pd.DataFrame().from_dict(json.loads(answer.text)['dados'])
    else:
        df_proposicoes = pd.concat([df_proposicoes, pd.DataFrame().from_dict(json.loads(answer.text)['dados'])])

df_proposicoes.to_parquet('../data/proposicoes.parquet')

#Chamando função para gerar arquivo parquet
get_proposicoes()

#Gerando sumarização por chucks
def generate_sumarizacao():
    pass 