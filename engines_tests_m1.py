import shutil
import os
import time
import pandas as pd
from engines.method_1.etl_linkedin_duckdb import EtlLinkedinDuckDb
from engines.method_1.etl_linkedin_pandas import EtlLinkedinPandas
from engines.method_1.etl_linkedin_polars import EtlLinkedinPolars
import gc


def clear_directory(directory):
    """
    Função para limpar o diretório de dados limpos.

    Parâmetros:
    directory (str): Diretório de dados limpos.
    """
    print("Cleaning clean directory...")
    if os.path.exists(directory):
        shutil.rmtree(directory)


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


def save_environment_metrics(
    environment,
    environment_dir,
    environment_data="data/linkedin/clean/m1/environments.csv",
):
    """
    Função para coletar os dados de ambiente.

    Parâmetros:
    raw_directory (str): Diretório de dados brutos.

    Retorno:
    dict: Dicionário contendo os dados de ambiente.
    """
    print("Collecting environment metrics...")
    etl = EtlLinkedinPandas(environment_dir, "_")
    files = etl.get_raw_files(environment_dir)
    data = etl.extract_data()

    num_files = len(files)
    num_tables = len(data)
    total_columns = 0
    total_rows = 0

    for dataframe in data:
        total_columns += dataframe["df"].shape[1]
        total_rows += dataframe["df"].shape[0]

    environment_metrics = {
        "environment": environment,
        "num_files": num_files,
        "num_tables": num_tables,
        "total_columns": total_columns,
        "total_rows": total_rows,
    }

    if os.path.exists(environment_data):
        pd.DataFrame([environment_metrics]).to_csv(
            environment_data, mode="a", index=False, header=False
        )
    else:
        pd.DataFrame([environment_metrics]).to_csv(environment_data, index=False)


class EtlLinkedin:
    """
    Classe para teste de processamento ETL (Extração, Transformação e Carga) de dados do LinkedIn.
    """

    def __init__(self, raw_directory, clean_directory, engine, environment):
        """
        Inicializa a classe EtlLinkedin com os diretórios de dados brutos e limpos e o motor de processamento.

        Parâmetros:
        raw_directory (str): Diretório contendo os dados brutos.
        clean_directory (str): Diretório onde os dados limpos serão armazenados.
        engine (str): Motor de processamento (duckdb, pandas, polars).
        """
        self.engine = engine
        self.raw_directory = raw_directory
        self.clean_directory = clean_directory
        self.etl = self.get_etl_instance(engine)
        self.engine_metrics = {}
        self.engine_metrics["environment"] = environment
        self.engine_metrics["engine"] = engine

    def get_etl_instance(self, engine):
        """
        Função para obter a instância do motor de processamento (duckdb, pandas, polars).

        Parâmetros:
        engine (str): Motor de processamento (duckdb, pandas, polars).

        Retorno:
        EtlLinkedinDuckDb, EtlLinkedinPandas ou EtlLinkedinPolars: Instância do motor de processamento (duckdb, pandas, polars).
        """
        if engine == "duckdb":
            return EtlLinkedinDuckDb(self.raw_directory, self.clean_directory)
        elif engine == "pandas":
            return EtlLinkedinPandas(self.raw_directory, self.clean_directory)
        elif engine == "polars":
            return EtlLinkedinPolars(self.raw_directory, self.clean_directory)
        else:
            raise ValueError("Invalid engine specified")

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

    @timer
    def transform_data(self, data):
        """
        Função para iniciar o processo de transformação de dados da engine.
        """
        return self.etl.transform_data(data)

    @timer
    def load_to_clean(self, data):
        """
        Função para iniciar o processo de carregamento dos dados limpos na engine.
        """
        self.etl.load_to_clean(data)

    @timer
    def concatenate_monthly_data_duckdb(self, data):
        """
        Função para iniciar o processo de concatenação dos dados da engine DuckDb.
        """
        return self.etl.concatenate_monthly_tables(data)

    @timer
    def concatenate_monthly_data(self, data):
        """
        Função para iniciar o processo de concatenação dos dados da engine.
        """
        if self.engine == "duckdb":
            return self.etl.concatenate_monthly_tables(data)
        else:
            return self.etl.concatenate_monthly_dataframes(data)

    @timer
    def export_monthly_data(self, data):
        """
        Função para iniciar o processo de exportação dos dados da engine.
        """
        if self.engine == "duckdb":
            self.etl.export_tables(data, "month")
        else:
            self.etl.export_dataframes(data, file_prefix="month")

    @timer
    def concatenate_category_data(self, data):
        """
        Função para iniciar o processo de concatenação dos dados da engine.
        """
        if self.engine == "duckdb":
            return self.etl.concatenate_category_tables(data)
        else:
            return self.etl.concatenate_category_dataframes(data)

    @timer
    def export_category_data(self, data):
        """
        Função para iniciar o processo de exportação dos dados concatenados da engine.
        """
        if self.engine == "duckdb":
            self.etl.export_tables(data, "all_extractions")
        else:
            self.etl.export_dataframes(data, file_prefix="all_extractions")

    def process_data(self):
        """
        Função para iniciar fluxo de processamento da engine.
        """
        clear_directory(self.clean_directory)
        print("Starting ETL process using", self.engine)

        total_start_time = time.time()

        self.steps_etl()

        total_elapsed_time = time.time() - total_start_time
        self.engine_metrics["total_etl_time"] = total_elapsed_time.__round__(2)
        print(f"[{self.engine}] Total ETL time: {total_elapsed_time:.2f} seconds")

        self.save_metrics_to_csv()

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


if __name__ == "__main__":
    dir_raw = "data/linkedin/raw"
    environments_tests = ["1y", "2y", "6y"]

    for environment in environments_tests:
        dir_environment = "_".join([dir_raw, environment])

        save_environment_metrics(environment, dir_environment)

        engines = ["duckdb", "polars", "pandas"]
        for engine in engines:
            dir_clean = f"data/linkedin/clean/m1/{engine}/{environment}"
            etl = EtlLinkedin(dir_environment, dir_clean, engine, environment)
            etl.process_data()
            del etl
            gc.collect()
