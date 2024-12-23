import streamlit as st
import yaml
import json
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd

# Carrega o arquivo YAML
with open("../data/config.yaml", 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)

# Carrega e processa o arquivo JSON com tratamento de erros
json_file_path = Path("../data/insights_distribuicao_deputados.json")
insights_data = []

if json_file_path.exists():
    try:
        with json_file_path.open('r', encoding='utf-8') as f:
            insights_data = json.load(f)
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar o arquivo JSON: {e}")
    except FileNotFoundError as e:
        st.error(f"Arquivo JSON não encontrado: {e}")
else:
    st.error(f"Arquivo JSON não encontrado: {json_file_path}")


st.title("Vigilantes")
st.write("Esse app foi criado para acompanhar as ações que os deputados brasileiros têm tomado")

tab1, tab2, tab3 = st.tabs(["Overview", "Despesas", "Proposições"])

with tab1:
    st.subheader("O que é Câmara dos Deputados?")
    st.write(config['overview_summary'])
    st.markdown("---")  # Divisor
    st.image("../docs/distribuicao_deputados.png", use_column_width=True)
    st.markdown("---")  # Divisor

    # if insights_data:
    #     for insight in insights_data:
    #         try:
    #             st.subheader(insight['insight']) # Acessa o insight diretamente
    #             st.write(insight['detalhe']) # Acessa o detalhe diretamente
    #         except KeyError as e:
    #             st.error(f"Erro ao processar um insight: Chave '{e}' não encontrada.")

import streamlit as st
import json
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# ... (código anterior para carregar config e insights_data) ...

with tab2:
    st.subheader("Despesas")

    # 6 - Ler e exibir dados do arquivo JSON
    json_despesas_path = Path("../data/insights_despesas_deputados_raw.json")
    if json_despesas_path.exists():
        try:
            with json_despesas_path.open('r', encoding='utf-8') as f:
                despesas_insights = json.load(f)
                st.markdown(f"## {despesas_insights['titulo']}") # Usando markdown para formatação
                st.write(despesas_insights['descricao'])
                st.markdown("### Recomendações:")
                for rec in despesas_insights['recomendacoes'].split("."):
                    st.write(f"- {rec.strip()}")

        except (json.JSONDecodeError, KeyError) as e:
            st.error(f"Erro ao processar o arquivo JSON de insights de despesas: {e}. Verifique se o arquivo JSON está no formato correto.")
        except Exception as e:
            st.exception(f"Um erro inesperado ocorreu ao processar o arquivo JSON: {e}")
    else:
        st.error(f"Arquivo JSON de insights de despesas não encontrado: {json_despesas_path}. Certifique-se de que o arquivo existe no caminho especificado.")


    # 7 - Criar gráfico de despesas por deputado
    parquet_file_path = Path("../data/serie_despesas_diarias_deputados.parquet")
    if parquet_file_path.exists():
        try:
            df_despesas = pd.read_parquet(parquet_file_path)

            deputados = df_despesas['nome'].unique()
            if not deputados.size:
                st.error("O arquivo Parquet não contém dados de deputados.")
            else:
                deputado_selecionado = st.selectbox("Selecione um Deputado:", deputados)

                df_deputado = df_despesas[df_despesas['nome'] == deputado_selecionado]

                try:
                    df_deputado['dataDocumento'] = pd.to_datetime(df_deputado['dataDocumento'])
                except ValueError as e:
                    st.error(f"Erro ao converter a coluna 'dataDocumento' para datetime: {e}. Verifique o formato das datas no arquivo Parquet.")
                    st.stop()

                df_agrupado = df_deputado.groupby('dataDocumento')['valorLiquido'].sum()

                plt.figure(figsize=(12, 6))
                plt.bar(df_agrupado.index, df_agrupado.values)
                plt.xlabel("Data")
                plt.ylabel("Valor Líquido (R$)")
                plt.title(f"Despesas de {deputado_selecionado} ao Longo do Tempo")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()
                st.pyplot(plt)

        except (pd.errors.EmptyDataError, pd.errors.ParserError, KeyError) as e:
            st.error(f"Erro ao processar o arquivo Parquet: {e}. Verifique se o arquivo Parquet está no formato correto e contém as colunas esperadas.")
        except Exception as e:
            st.exception(f"Um erro inesperado ocorreu ao processar o arquivo Parquet ou criar o gráfico: {e}")
    else:
        st.error(f"Arquivo Parquet não encontrado: {parquet_file_path}. Certifique-se de que o arquivo existe no caminho especificado.")


with tab3:
    st.subheader("Proposições")
    # 8 - Exibir dados do arquivo parquet de proposições
    parquet_proposicoes_path = Path("../data/proposicoes.parquet")
    if parquet_proposicoes_path.exists():
        try:
            df_proposicoes = pd.read_parquet(parquet_proposicoes_path)
            st.dataframe(df_proposicoes) # Exibe o DataFrame como uma tabela
        except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            st.error(f"Erro ao ler ou processar o arquivo parquet de proposições: {e}")
        except Exception as e:
            st.exception(f"Um erro inesperado ocorreu ao processar o arquivo de proposições: {e}")
    else:
        st.error(f"Arquivo parquet de proposições não encontrado: {parquet_proposicoes_path}")