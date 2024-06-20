import shutil
import os
import time
from engines.method_1.etl_linkedin_duckdb import EtlLinkedinDuckDb
from engines.method_1.etl_linkedin_pandas import EtlLinkedinPandas
from engines.method_1.etl_linkedin_polars import EtlLinkedinPolars
import csv


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


def collect_scenarios_metrics(raw_directory):
    """
    Função para coletar os dados de ambiente.

    Parâmetros:
    raw_directory (str): Diretório de dados brutos.

    Retorno:
    dict: Dicionário contendo os dados de ambiente.
    """
    etl = EtlLinkedinPolars(raw_directory, "_")
    files = etl.get_raw_files(raw_directory)
    data = etl.extract_data()

    num_files = len(files)
    num_tables = len(data)
    total_columns = 0
    total_rows = 0

    for dataframe in data:
        total_columns += dataframe["df"].shape[1]
        total_rows += dataframe["df"].shape[0]

    scenarios_metrics = {
        "num_files": num_files,
        "num_tables": num_tables,
        "total_columns": total_columns,
        "total_rows": total_rows,
    }
    return scenarios_metrics


def save_scenarios_metrics(
    scenarios_metrics, filename="data/linkedin/clean/m1/scenarios.csv"
):
    """
    Função para salvar os dados de ambiente em um arquivo CSV.

    Parâmetros:
    scenarios_metrics (dict): Dicionário contendo os dados de ambiente.
    filename (str): Nome do arquivo CSV. O padrão é 'scenarios_metrics.csv'.

    Retorno:
    int: Índice do registro no arquivo CSV.
    """
    if not os.path.exists("data/linkedin/clean/m1/"):
        os.makedirs("data/linkedin/clean/m1/")

    fieldnames = ["index", *scenarios_metrics.keys()]
    file_exists = os.path.isfile(filename)

    if file_exists:
        with open(filename, mode="r") as file:
            reader = csv.DictReader(file)
            index = sum(1 for row in reader)
    else:
        index = 0

    scenarios_metrics["index"] = index

    with open(filename, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(scenarios_metrics)

    return index


class EtlLinkedin:
    """
    Classe para teste de processamento ETL (Extração, Transformação e Carga) de dados do LinkedIn.
    """

    def __init__(self, raw_directory, clean_directory, engine, scenarios_index):
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
        self.scenarios_index = scenarios_index

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
        return self.etl.extract_data()

    @timer
    def extract_data_to_duckdb(self):
        """
        Função para iniciar o processo de conversão de dados da engine.
        """
        data = self.etl.extract_data()
        return self.etl.convert_dataframes_to_duckdb(data)

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
    def export_monthly_data_duckdb(self, data):
        """
        Função para iniciar o processo de exportação dos dados da engine DuckDb.
        """
        self.etl.export_tables(data, "month")

    @timer
    def concatenate_category_data_duckdb(self, data):
        """
        Função para iniciar o processo de concatenação dos dados da engine DuckDb.
        """
        return self.etl.concatenate_category_tables(data)

    @timer
    def export_category_data_duckdb(self, data):
        """
        Função para iniciar o processo de exportação dos dados da engine DuckDb.
        """
        self.etl.export_tables(data, "all_extractions")

    @timer
    def concatenate_monthly_data(self, data):
        """
        Função para iniciar o processo de concatenação dos dados da engine.
        """
        return self.etl.concatenate_monthly_dataframes(data)

    @timer
    def export_monthly_data(self, data):
        """
        Função para iniciar o processo de exportação dos dados da engine.
        """
        self.etl.export_dataframes(data, file_prefix="month")

    @timer
    def concatenate_category_data(self, data):
        """
        Função para iniciar o processo de concatenação dos dados da engine.
        """
        return self.etl.concatenate_category_dataframes(data)

    @timer
    def export_category_data(self, data):
        """
        Função para iniciar o processo de exportação dos dados concatenados da engine.
        """
        self.etl.export_dataframes(data, file_prefix="all_extractions")

    def process_data(self):
        """
        Função para iniciar fluxo de processamento da engine.
        """
        clear_directory(self.clean_directory)
        print("Starting ETL process using", self.engine)

        total_start_time = time.time()

        if self.engine == "duckdb":
            self.steps_duckdb()
        elif self.engine == "pandas":
            self.steps_pandas()
        elif self.engine == "polars":
            self.steps_polars()
        else:
            raise ValueError("Invalid engine specified")

        total_elapsed_time = time.time() - total_start_time
        self.engine_metrics["total_etl_time"] = total_elapsed_time.__round__(2)
        print(f"[{self.engine}] Total ETL time: {total_elapsed_time:.2f} seconds")

        self.save_metrics_to_csv()

    def steps_duckdb(self):
        """
        Função para iniciar fluxo de processamento da engine DuckDb.
        """
        data = self.extract_data_to_duckdb()
        data = self.transform_data(data)
        self.load_to_clean(data)
        monthly_data = self.concatenate_monthly_data_duckdb(data)
        self.export_monthly_data_duckdb(monthly_data)
        category_data = self.concatenate_category_data_duckdb(monthly_data)
        self.export_category_data_duckdb(category_data)

    def steps_pandas(self):
        """
        Função para iniciar fluxo de processamento da engine Pandas.
        """
        data = self.extract_data()
        data = self.transform_data(data)
        self.load_to_clean(data)
        monthly_data = self.concatenate_monthly_data(data)
        self.export_monthly_data(monthly_data)
        category_data = self.concatenate_category_data(monthly_data)
        self.export_category_data(category_data)

    def steps_polars(self):
        """
        Função para iniciar fluxo de processamento da engine Polars.
        """
        data = self.extract_data()
        data = self.transform_data(data)
        self.load_to_clean(data)
        monthly_data = self.concatenate_monthly_data(data)
        self.export_monthly_data(monthly_data)
        category_data = self.concatenate_category_data(monthly_data)
        self.export_category_data(category_data)

    def save_metrics_to_csv(self):
        file_exists = os.path.isfile("data/linkedin/clean/m1/metrics.csv")
        with open(
            "data/linkedin/clean/m1/metrics.csv", mode="a", newline=""
        ) as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["scenarios_index", "engine", "step", "time_seconds"])
            for step, time_seconds in self.engine_metrics.items():
                writer.writerow(
                    [self.scenarios_index, self.engine, step, time_seconds]
                )


if __name__ == "__main__":
    dir_raw = "data/linkedin/raw_2030"

    env_metrics = collect_scenarios_metrics(dir_raw)
    env_index = save_scenarios_metrics(env_metrics)

    engines = ["duckdb", "polars", "pandas"]
    for engine in engines:
        dir_clean = f"data/linkedin/clean/m1/{engine}"
        EtlLinkedin(dir_raw, dir_clean, engine, env_index).process_data()
