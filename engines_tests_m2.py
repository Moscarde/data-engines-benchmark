import shutil
import os
import time
from engines.method_2.etl_linkedin_duckdb_2 import EtlLinkedinDuckDb
from engines.method_2.etl_linkedin_pandas_2 import EtlLinkedinPandas
from engines.method_2.etl_linkedin_polars_2 import EtlLinkedinPolars
import csv


def clear_directory(engine):
    """
    Função para limpar o diretório de dados limpos.

    Parâmetros:
    directory (str): Diretório de dados limpos.
    """
    print("Cleaning engine directory...")
    if os.path.exists(f"data/linkedin/clean/m2/{engine}"):
        shutil.rmtree(f"data/linkedin/clean/m2/{engine}")


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

    files = os.listdir(raw_directory)
    dataframe_size = 0
    for file in files:
        dataframe_size += os.path.getsize(raw_directory + "/" + file)

    num_files = len(files)
    num_lines = 0
    for file in files:
        with open(raw_directory + "/" + file, "r", encoding="utf-8") as file:
            num_lines += sum(1 for line in file)
    # num_tables = len(data)
    # total_columns = 0
    # total_rows = 0

    # for dataframe in data:
    #     total_columns += dataframe["df"].shape[1]
    #     total_rows += dataframe["df"].shape[0]

    scenarios_metrics = {
        "num_files": num_files,
        "clean_dataframe_size": f"{(dataframe_size / 1000000).__round__(2)} MB",
        "num_lines": num_lines,
        # "total_columns": total_columns,
        # "total_rows": total_rows,
    }
    return scenarios_metrics


def save_scenarios_metrics(
    scenarios_metrics, filename="data/linkedin/clean/m2/scenarios.csv"
):
    """
    Função para salvar os dados de ambiente em um arquivo CSV.

    Parâmetros:
    scenarios_metrics (dict): Dicionário contendo os dados de ambiente.
    filename (str): Nome do arquivo CSV. O padrão é 'scenarios_metrics.csv'.

    Retorno:
    int: Índice do registro no arquivo CSV.
    """

    if not os.path.exists("data/linkedin/clean/m2/"):
        os.makedirs("data/linkedin/clean/m2/")

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

    def __init__(
        self,
        engine,
        scenarios_index,
        m1_directory="data/linkedin/clean/m1",
        unique_extraction_directory="data/linkedin/raw_unique_extraction",
    ):
        """
        Inicializa a classe EtlLinkedin com os diretórios de dados brutos e limpos e o motor de processamento.

        Parâmetros:
        raw_directory (str): Diretório contendo os dados brutos.
        clean_directory (str): Diretório onde os dados limpos serão armazenados.
        engine (str): Motor de processamento (duckdb, pandas, polars).
        """
        self.engine = engine
        self.m1_directory = m1_directory
        self.clean_concatenated_directory = (
            f"{m1_directory}/{engine}/concatenated_dataframes"
        )
        self.export_directory = f"data/linkedin/clean/m2/{engine}"
        self.unique_extraction_directory = unique_extraction_directory
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
            return EtlLinkedinDuckDb(
                self.clean_concatenated_directory,
                self.unique_extraction_directory,
                self.export_directory,
            )
        elif engine == "pandas":
            return EtlLinkedinPandas(
                self.clean_concatenated_directory,
                self.unique_extraction_directory,
                self.export_directory,
            )
        elif engine == "polars":
            return EtlLinkedinPolars(
                self.clean_concatenated_directory,
                self.unique_extraction_directory,
                self.export_directory,
            )
        else:
            raise ValueError("Invalid engine specified")

    @timer
    def transform_data(self, data):
        """
        Função para iniciar o processo de transformação de dados da engine.
        """
        return self.etl.transform_data(data)

    @timer
    def get_clean_concatenated_data(self):
        """
        Função para obter os dados concatenados limpos da engine.
        """

        return self.etl.get_clean_concatenated_data()

    @timer
    def get_raw_unique_extraction_data(self):
        """
        Função para obter os dados de extração unicos.
        """
        if self.engine == "duckdb":
            data_pandas = self.etl.get_raw_unique_extraction_data()
            return self.etl.convert_dataframes_to_duckdb(data_pandas)
        else:
            return self.etl.get_raw_unique_extraction_data()

    @timer
    def concatenate_unique_extraction_data(self, clean_dataframes, extraction_data):
        """
        Função para iniciar o processo de concatenação dos dados de extração unicos da engine.
        """
        return self.etl.concatenate_unique_extraction_data(
            clean_dataframes, extraction_data
        )

    @timer
    def export_dataframes(self, data):
        """
        Função para iniciar o processo de exportação dos dados da engine."""

        return self.etl.export_dataframes(data)

    def process_data(self):
        """
        Função para iniciar fluxo de processamento da engine.
        """

        print("Starting ETL process using", self.engine)

        clear_directory(self.engine)
        total_start_time = time.time()

        if self.engine == "duckdb":
            self.steps_duckdb()
        elif self.engine == "pandas":
            self.steps_polars_pandas()
        elif self.engine == "polars":
            self.steps_polars_pandas()
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
        clean_tables = self.get_clean_concatenated_data()
        extraction_tables = self.get_raw_unique_extraction_data()
        self.transform_data(extraction_tables)
        concatenated_tables = self.concatenate_unique_extraction_data(
            clean_tables, extraction_tables
        )

        self.export_dataframes(concatenated_tables)

    def steps_polars_pandas(self):
        """
        Função para iniciar fluxo de processamento da engine Polars ou Pandas.
        """
        clean_dataframes = self.get_clean_concatenated_data()
        extraction_data = self.get_raw_unique_extraction_data()
        extraction_data = self.transform_data(extraction_data)

        concatenated_data = self.concatenate_unique_extraction_data(
            clean_dataframes, extraction_data
        )
        self.export_dataframes(concatenated_data)

    def save_metrics_to_csv(self):
        file_exists = os.path.isfile("data/linkedin/clean/m2/metrics.csv")
        with open(
            "data/linkedin/clean/m2/metrics.csv", mode="a", newline=""
        ) as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["scenarios_index", "engine", "step", "time_seconds"])
            for step, time_seconds in self.engine_metrics.items():
                writer.writerow(
                    [self.scenarios_index, self.engine, step, time_seconds]
                )


if __name__ == "__main__":
    engines = ["polars", "pandas", "duckdb"]

    env_metrics = collect_scenarios_metrics(
        f"data\linkedin\clean\m1\{engines[0]}\concatenated_dataframes"
    )
    env_index = save_scenarios_metrics(env_metrics)

    for engine in engines:
        EtlLinkedin(engine, env_index).process_data()
