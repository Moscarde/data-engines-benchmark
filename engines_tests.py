import shutil
import os
import time
from engines.etl_linkedin_duckdb import EtlLinkedinDuckDb
from engines.etl_linkedin_pandas import EtlLinkedinPandas
from engines.etl_linkedin_polars import EtlLinkedinPolars

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
        return result
    return wrapper

class EtlLinkedin:
    """
    Classe para teste de processamento ETL (Extração, Transformação e Carga) de dados do LinkedIn.
    """
    def __init__(self, raw_directory, clean_directory, engine):
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
    def convert_dataframes_to_duckdb(self, data):
        """
        Função para iniciar o processo de conversão de dados da engine.
        """
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

        print(f"[{self.engine}] Total ETL time: {time.time() - total_start_time:.2f} seconds")

    def steps_duckdb(self):
        """
        Função para iniciar fluxo de processamento da engine DuckDb.
        """
        data = self.extract_data()
        data = self.convert_dataframes_to_duckdb(data)
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

if __name__ == "__main__":
    dir_raw = "data/linkedin/raw"
    dir_clean = "data/linkedin/clean"

    engines = ["duckdb", "polars", "pandas"]
    for engine in engines:
        EtlLinkedin(dir_raw, dir_clean, engine).process_data()
