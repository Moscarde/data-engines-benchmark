![Header Logo](images/header.png)
# Data Engines Benchmark

Como voluntário engenheiro de dados Jr na [SouJunior](https://soujunior.tech), fui encarregado de uma tarefa crucial: comparar diferentes engines de processamento de dados para identificar a melhor opção para nossa pipeline de ETL (Extração, Transformação e Carga).

Nossa pipeline processa dados extraídos da página do LinkedIn da SouJunior, com extrações ocorrendo a cada 15 dias. Em cada rotina de extração, são gerados quatro arquivos no formato .xls / .xlsx, correspondentes às categorias de `Conteúdo`, `Visitantes`, `Seguidores` e `Concorrentes`.

Esse readme contém detalhes técnicos, informações do código e como executar. Para conferir o artigo que explora os resultados de cada engine de processamento, acesse o [artigo](article.md).

## 📝 Sumário

- [Data Engines Benchmark](#data-engines-benchmark)
  - [📝 Sumário](#-sumário)
  - [⚙️ Engines Utilizadas](#️-engines-utilizadas)
  - [📦 Instalação](#-instalação)
  - [📁 Estrutura do código](#-estrutura-do-código)
  - [📝 Detalhes técnicos](#-detalhes-técnicos)
    - [Geração de Métricas](#geração-de-métricas)
      - [Funcionamento](#funcionamento)
      - [Exemplo de uso](#exemplo-de-uso)
      - [Salvando Métricas](#salvando-métricas)
    - [Peculiaridades de cada engine](#peculiaridades-de-cada-engine)
      - [Pandas](#pandas)
      - [Polars](#polars)
      - [DuckDB](#duckdb)
  - [▶️ Como executar](#️-como-executar)
  - [🙏 Agradecimentos](#-agradecimentos)
  - [💡 Contribuições](#-contribuições)
  - [🌟 Esmola Pill](#-esmola-pill)

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

## 📁 Estrutura do código

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

## 📝 Detalhes técnicos

### Geração de Métricas

Para medir o tempo de execução das funções críticas no processo de ETL, foi criado um decorador `@timer`.

```python	
def timer(func):
    """
    Função para medir o tempo de execução de uma função.

    Parâmetros:
    func (function): Função a ser medida.

    Retorno:
    function: Função com o tempo de execução medido.
    """
    def wrapper(*args, **kwargs):
        """
        Função que mede o tempo de execução de uma função.

        Parâmetros:
        *args: Lista de parâmetros passados para a função.
        **kwargs: Dicionário de parâmetros passados para a função.

        Retorno:
        function: Função com o tempo de execução medido.
        """
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        print(f"[{args[0].engine}] {func.__name__}: {elapsed_time:.2f} seconds")
        args[0].engine_metrics[func.__name__] = elapsed_time.__round__(2)
        return result

    return wrapper
```

#### Funcionamento

1. **Definição do Decorador:** A função `timer` é um decorador que mede o tempo de execução de outra função. Ela utiliza o módulo `time` para capturar o tempo inicial antes da execução e o tempo final após a execução da função decorada.

2. **Wrapper:** Dentro do decorador, a função `wrapper` é definida para envolver a função original. Ela captura o tempo de início, executa a função original, calcula o tempo decorrido e imprime o tempo de execução.

3. **Armazenamento das Métricas:** O tempo de execução é armazenado no atributo `engine_metrics` da instância da classe. Isso permite que as métricas de desempenho sejam salvas e analisadas posteriormente.

#### Exemplo de uso

O decorador `@timer` é usado na função `extract_data` dentro da classe responsável pelo processamento ETL:

```python
    def steps_etl(self):
        """
        Função para iniciar fluxo de processamento da engine.
        """
        data = self.extract_data()
        data = self.transform_data(data)
        self.load_to_clean(data)
        monthly_data = self.concatenate_monthly_data(data)
        self.export_monthly_data(monthly_data)
        category_data = self.concatenate_category_data(monthly_data)
        self.export_category_data(category_data)

    @timer
    def extract_data(self):
        """
        Função para iniciar o processo de extração de dados da engine.
        """
        data = self.etl.extract_data()
        if self.engine == "duckdb":
            return self.etl.convert_dataframes_to_duckdb(data)
        else:
            return data
```

#### Salvando Métricas

Após a execução das funções decoradas, as métricas de tempo podem ser salvas em um arquivo CSV para análise posterior:

```python
def save_metrics_to_csv(self, metrics_file="data/linkedin/clean/m1/engines.csv"):
    file_exists = os.path.isfile(metrics_file)
    if file_exists:
        pd.DataFrame(self.engine_metrics, index=[0]).to_csv(
            metrics_file, mode="a", index=False, header=False
        )
    else:
        pd.DataFrame(self.engine_metrics, index=[0]).to_csv(
            metrics_file, index=False
        )
```

### Peculiaridades de cada engine

#### Pandas

Eu já tinha um pouco de experiência com a biblioteca Pandas, mas trabalhar com uma grande quantidade de dados me proporcionou uma afinidade maior com a ferramenta.

O pandas não teve dificuldade com a leitura dos arquivos `.xls` e `.xlsx`. Embora não estejam padronizados, com alguns ajustes no código a base de dados é carregada facilmente.

```python
    df = pd.read_excel(
        file["file_path"],
        sheet_name=sheet["sheet_pos"],
        skiprows=sheet["skiprows"],
    )

```

Após o tratamento dos dados, o Pandas é usado para concatenar os arquivos em um dataframe, que é então exportado para um arquivo CSV.

```python
    dataframe["concatenated_df"].to_csv(
        full_path, index=False, quoting=csv.QUOTE_ALL
    )
```

#### Polars

A biblioteca Polars foi a mais performática, porém a que mais me trouxe dificuldades. Além da documentação oficial não explorar profundamente cada funcionalidade (principalmente sobre a leitura de arquivos), outras fontes de referência que encontrei acabavam trocando termos com os do Pandas por conta da sua similaridade.

```python
    df = pl.read_excel(
        source=file["file_path"],
        sheet_id=sheet["sheet_pos"],
        read_options={"skip_rows": sheet["skiprows"]},
    )
```

```python
    dataframe["concatenated_df"].write_csv(full_path, quote_style="always")
```

#### DuckDB

Esse resultado se deve à leitura dos arquivos .xls e .xlsx, que precisam ser carregados primeiramente para um dataframe Pandas e, em seguida, convertidos para tabelas DuckDB. A curva de aprendizado com a biblioteca DuckDB foi ótima, por conta de utilizar a lógica de tabelas e queries SQL.

```python
    df = pd.read_excel(
                file["file_path"],
                sheet_name=sheet["sheet_pos"],
                skiprows=sheet["skiprows"],
    )

    # ...

    db_table_name = (
            f"{dataframe['dataframe_name']}_{dataframe['extraction_period']}"
        )
        table_attribute = table_attributes.get(dataframe["dataframe_name"])

        translated_columns = table_attribute.keys()
        dataframe["df"].columns = list(translated_columns)

        self.con.register("temp_table", dataframe["df"])

        columns_definition = ", ".join(
            [f'"{col}" {dtype}' for col, dtype in table_attribute.items()]
        )
        create_table_query = f"CREATE TABLE {db_table_name} ({columns_definition});"
        self.con.execute(create_table_query)

        insert_query = f"INSERT INTO {db_table_name} SELECT "
        for col, dtype in table_attribute.items():
            if dtype == "DATE":
                insert_query += f'CASE WHEN "{col}" IS NULL OR "{col}" = \'\' THEN NULL ELSE STRPTIME(CAST("{col}" AS VARCHAR), \'%m/%d/%Y\') END AS "{col}", '
            else:
                insert_query += f'"{col}", '

        insert_query = insert_query.rstrip(", ") + " FROM temp_table;"

        self.con.execute(insert_query)

        table_dict = {
            "dataframe_name": dataframe["dataframe_name"],
            "extraction_period": dataframe["extraction_period"],
            "db_table_name": db_table_name,
            "export_dir": os.path.join(self.clean_directory, *dataframe["dir"]),
        }

```
_Por conta da quantidade de tabelas sendo processadas simultaneamente para seguir a mesma lógica das outras engines, precisei utilizar uma lógica um pouco mais complexa para gerar as queries. O trecho acima resulta em queries assim:_

```SQL

INSERT INTO visitors_location_2024_Mar_2 SELECT "Location", "Total Views" FROM temp_table;

```

💡 **Nota**: Devido à lógica de processamento diferente das demais, o DuckDB poderia performar melhor caso fosse recompilado e otimizado seguindo seus próprios princípios de processamento. A implementação atual foi adaptada para manter a consistência com as outras engines, o que pode não aproveitar todo o potencial de performance do DuckDB.


## ▶️ Como executar

Adicione arquivos de extrações no diretório `data/linkedin/raw_1y/*` e `data/linkedin/unique_extraction/` e replique para os demais ambientes fictícios (2 e 6 anos).

Execute o script de teste `engines_test_m1.py` ou `engines_test_m2.py` de acordo com o método escolhido.

Resultados das engines e métodos são armazenados em `data/linkedin/clean/`.

Estes scripts iteram por todas as 3 engines e para cada engine, testam os 3 ambientes fictícios.

Para executar individualmente cada engine, execute os respectivos scripts localizados em `engines/method_1` e `engines/method_2`.

💡 **Nota**: O fluxo de processamento de dados trabalhado não é o mais performático, por ser o que estamos utilizando na etapa de validação e testes de desenvolvimento. Porém o mesmo fluxo foi replicado para ambas as engines


## 🙏 Agradecimentos

Agradecimentos para o time de dados da [SouJunior](https://soujunior.tech) que colaboraram com o projeto: [Pedro Fogaça](https://github.com/hdind), [Bruna Krobel](https://github.com/Bruna-Krobel) e [Karine Cristina](https://github.com/karinnecristina).

## 💡 Contribuições

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests com melhorias e correções.

## 🌟 Esmola Pill

Apoie este projeto deixando uma **Estrelinha**⭐ aqui no repositório.
