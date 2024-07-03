![Header Logo](images/header.png)
# Data Engines Benchmark

Como voluntário engenheiro de dados Jr na **SouJunior**, fui encarregado de uma tarefa crucial: comparar diferentes engines de processamento de dados para identificar a melhor opção para nossa pipeline de ETL (Extração, Transformação e Carga).

Nossa pipeline processa dados extraídos da página do LinkedIn da SouJunior, com extrações ocorrendo a cada 15 dias. Em cada rotina de extração, são gerados quatro arquivos no formato .xls / .xlsx, correspondentes às categorias de `Conteúdo`, `Visitantes`, `Seguidores` e `Concorrentes`.

Esse readme contém detalhes técnicos, informações do código e como executar. Para conferir o artigo que explora os resultados de cada engine de processamento, acesse o [artigo](article.md).

## ⚙️ Engines Utilizadas

- [**DuckDB**](https://duckdb.org/docs/api/python/overview.html)
- [**Pandas**](https://pandas.pydata.org/docs/getting_started/index.html#getting-started)
- [**Polars**](https://docs.pola.rs/user-guide/installation/)

## 📦 Instalação

1. Clone o repositório no seu computador.
```bash
git clone https://github.com/moscarde/data-engines-benchmark.git
cd data-engines-benchmark
```


2. Para instalar as bibliotecas, execute o seguinte código:

```bash
pip install pandas
pip install openpyxl
pip install polars # ou polars-lts-cpu para processadores mais antigos
pip install duckdb

```

💡 **Notas**: 

1. Dependendo do seu processador, pode ser necessário instalar a biblioteca `polars-lts-cpu`. 

2. Os resultados desse estudo foram gerados a partir das seguintes versões das bibliotecas: `pandas==2.2.2`, `polars-lts-cpu==0.20.30` e `duckdb==1.0.0`.

## Estrutura do código

```
├── data/linkedin/
        ├── clean/ # Local onde será salvo os dados limpos e resultados dos testes
        ├── raw_1y/ # 1 ano de dados brutos
            ├── Concorrentes/
                ├── *Anos/
                    ├── *Meses/
            ├── Conteúdo/
                ├── *Anos/
                    ├── *Meses/
            ├── Seguidores/
                ├── *Anos/
                    ├── *Meses/
            ├── Visitantes/
                ├── *Anos/
                    ├── *Meses/
        ├── raw_2y/ # 2 anos de dados brutos 
        ├── raw_6y/ # 6 anos de dados brutos
        ├── raw_unique_extraction/ # Arquivos de extração única (Utilizado na segunda abordagem)
├── engines/
    ├── method_1/ # Classes de engines programadas para executar o método 1
    ├── method_2/ # Classes de engines programadas para executar o método 2
├── plots/ # Gráficos gerados no script de análise de resultados
├── engines_test_m1.py # Script de teste para o método 1
├── engines_test_m2.py # Script de teste para o método 2
├── performance_analysis.py # Notebook para exploração dos resultados

```

## ▶️ Como executar

Adicione arquivos de extrações no diretório `data/linkedin/raw_1y/*` e `data/linkedin/unique_extraction/` e replique para os demais ambientes fictícios (2 e 6 anos).

Execute o script de teste `engines_test_m1.py` ou `engines_test_m2.py` de acordo com o método escolhido.

Resultados das engines e métodos são armazenados em `data/linkedin/clean/`.

Estes scripts iteram por todas as 3 engines e para cada engine, testam os 3 ambientes fictícios.

Para executar individualmente cada engine, execute os respectivos scripts localizados em `engines/method_1` e `engines/method_2`.

💡 **Nota**: O fluxo de processamento de dados trabalhado não é o mais performático, por ser o que estamos utilizando na etapa de validação e testes de desenvolvimento. Porém o mesmo fluxo foi replicado para ambas as engines


## 🙏 Agradecimentos

Agradecimentos para o time de dados da [SouJunior](https://soujunior.tech) que colaboram com o projeto: [Pedro Fogaça](https://github.com/hdind), [Bruna Krobel](https://github.com/Bruna-Krobel) e [Karine Cristina](https://github.com/karinnecristina).

## 💡 Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests com melhorias e correções.

## 🌟 Esmola Pill

Apoie este projeto deixando uma **Estrelinha**⭐ aqui no repositório.