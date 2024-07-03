import shutil
import os
import time
from engines.method_2.etl_linkedin_duckdb_2 import EtlLinkedinDuckDb
from engines.method_2.etl_linkedin_pandas_2 import EtlLinkedinPandas
from engines.method_2.etl_linkedin_polars_2 import EtlLinkedinPolars
import pandas as pd
import gc


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


def save_environment_metrics(
    env, env_clean_dir, environment_data="data/linkedin/clean/m2/environments.csv"
):
    """
    Função para coletar os dados de ambiente.

    Parâmetros:
    raw_directory (str): Diretório de dados brutos.

    Retorno:
    dict: Dicionário contendo os dados de ambiente.
    """

    files = os.listdir(env_clean_dir)
    num_files = len(files)
    dataframe_size = 0
    num_lines = 0
    for file in files:
        dataframe_size += os.path.getsize(env_clean_dir + "/" + file)
        with open(env_clean_dir + "/" + file, "r", encoding="utf-8") as file:
            num_lines += sum(1 for line in file)

    environment_metrics = {
        "environment": env,
        "initial_clean_dataframe_size_MB": f"{(dataframe_size / 1000000).__round__(2)}",
        "num_files": num_files,
        "num_lines": num_lines,
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

    def __init__(
        self,
        engine,
        environment,
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
            f"{m1_directory}/{engine}/{environment}/concatenated_dataframes"
        )
        self.export_directory = f"data/linkedin/clean/m2/{engine}"
        self.unique_extraction_directory = unique_extraction_directory
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
    def get_clean_concatenated_data(self):
        """
        Função para obter os dados concatenados limpos da engine.
        """

        return self.etl.get_clean_concatenated_data()

    @timer
    def get_raw_unique_extraction_data(self):
        """
        Função para obter os dados de extração únicos.
        """
        if self.engine == "duckdb":
            data_pandas = self.etl.get_raw_unique_extraction_data()
            return self.etl.convert_dataframes_to_duckdb(data_pandas)
        else:
            return self.etl.get_raw_unique_extraction_data()

    @timer
    def transform_data(self, data):
        """
        Função para iniciar o processo de transformação de dados da engine.
        """
        return self.etl.transform_data(data)

    @timer
    def concatenate_unique_extraction_data(self, clean_dataframes, extraction_data):
        """
        Função para iniciar o processo de concatenação dos dados de extração únicos da engine.
        """
        return self.etl.concatenate_unique_extraction_data(
            clean_dataframes, extraction_data
        )

    @timer
    def export_dataframes(self, data):
        """
        Função para iniciar o processo de exportação dos dados da engine.
        """

        return self.etl.export_dataframes(data)

    def process_data(self):
        """
        Função para iniciar fluxo de processamento da engine.
        """

        print("Starting ETL process using", self.engine)

        clear_directory(self.engine)
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
        clean_dataframes = self.get_clean_concatenated_data()
        extraction_data = self.get_raw_unique_extraction_data()
        extraction_data = self.transform_data(extraction_data)

        concatenated_data = self.concatenate_unique_extraction_data(
            clean_dataframes, extraction_data
        )
        self.export_dataframes(concatenated_data)

    def save_metrics_to_csv(self, metrics_file="data/linkedin/clean/m2/engines.csv"):
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
    environments = ["1y", "2y", "6y"]
    engines = ["polars", "pandas", "duckdb"]

    for environment in environments:
        env_clean_dir = f"data\linkedin\clean\m1\{engines[0]}\{environment}\concatenated_dataframes"
        save_environment_metrics(env=environment, env_clean_dir=env_clean_dir)
        
        for engine in engines:
            etl = EtlLinkedin(engine, environment)
            etl.process_data()
            
            del etl
            gc.collect()
