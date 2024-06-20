import pandas as pd
import os
import duckdb
import calendar

import warnings
import logging

# Suprimir avisos específicos da openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Configurar logging
logging.basicConfig(level=logging.ERROR)


class EtlLinkedinDuckDb:
    """
    Classe responsável pelo processamento ETL (Extração, Transformação e Carga) de dados do LinkedIn.
    """

    def __init__(
        self, clean_concatenated_directory, unique_extraction_directory, export_dir
    ):
        """
        Inicializa a classe LinkedInETLProcessor com os diretórios de dados brutos e limpos.

        Parâmetros:
        raw_directory (str): Diretório contendo os dados brutos.
        clean_directory (str): Diretório onde os dados limpos serão armazenados.
        """
        self.clean_concatenated_directory = clean_concatenated_directory
        self.unique_extraction_directory = unique_extraction_directory
        self.export_dir = export_dir
        self.con = duckdb.connect(database=":memory:")

    def detect_file_category(self, file):
        """
        Detecta a categoria de um arquivo com base em seu nome.

        Parâmetros:
        file (str): Nome do arquivo.

        Retorno:
        str: Categoria do arquivo (competitor, content, followers, visitors) ou 0 se não identificado.
        """
        if "competitor" in file:
            return "competitor"
        elif "content" in file:
            return "content"
        elif "followers" in file:
            return "followers"
        elif "visitors" in file:
            return "visitors"
        return 0

    def read_excel_file(self, file):
        """
        Lê um arquivo Excel e retorna seus dados como uma lista de DataFrames.

        Parâmetros:
        file (dict): Dicionário com informações sobre o arquivo, incluindo categoria, caminho e período de extração.

        Retorno:
        list: Lista de dicionários contendo o nome do DataFrame, diretório, período de extração e o DataFrame.
        """
        category_keys = {
            "competitor": [{"sheet_name": "competitor", "sheet_pos": 0, "skiprows": 1}],
            "content": [
                {"sheet_name": "content_metrics", "sheet_pos": 0, "skiprows": 1},
                {"sheet_name": "content_posts", "sheet_pos": 1, "skiprows": 1},
            ],
            "followers": [
                {"sheet_name": "followers_new", "sheet_pos": 0, "skiprows": 0},
                {"sheet_name": "followers_location", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "followers_function", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "followers_experience", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "followers_industry", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "followers_company_size", "sheet_pos": 5, "skiprows": 0},
            ],
            "visitors": [
                {"sheet_name": "visitors_metrics", "sheet_pos": 0, "skiprows": 0},
                {"sheet_name": "visitors_location", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "visitors_function", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "visitors_experience", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "visitors_industry", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "visitors_company_size", "sheet_pos": 5, "skiprows": 0},
            ],
        }

        sheets_to_read = category_keys[file["category"]]

        dataframes = []
        for sheet in sheets_to_read:

            df = pd.read_excel(
                file["file_path"],
                sheet_name=sheet["sheet_pos"],
                skiprows=sheet["skiprows"],
            )

            dataframes.append(
                {
                    "dataframe_name": sheet["sheet_name"],
                    "dir": file["dir"],
                    "extraction_period": file["extraction_period"],
                    "df": df,
                }
            )

        return dataframes

    def convert_dataframes_to_duckdb(self, data):
        """
        Converte dataframes pandas para tabelas em DuckDB.

        Parâmetros:
        data (list): Lista de dicionários contendo os dados extraídos.

        Retorno:
        tables: lista de dicionários contendo dados das tabelas.
        """
        tables = []
        for dataframe in data:
            table_dict = self.register_dataframe_in_duckdb(dataframe)
            tables.append(table_dict)
        return tables

    def register_dataframe_in_duckdb(self, dataframe):
        table_attributes = {
            "content_metrics": {
                "Date": "DATE",  # inferir data diretamente
                "Impressions (organic)": "INT",
                "Impressions (sponsored)": "INT",
                "Impressions (total)": "INT",
                "Unique impressions (organic)": "INT",
                "Clicks (organic)": "INT",
                "Clicks (sponsored)": "INT",
                "Clicks (total)": "INT",
                "Reactions (organic)": "INT",
                "Reactions (sponsored)": "INT",
                "Reactions (total)": "INT",
                "Comments (organic)": "INT",
                "Comments (sponsored)": "INT",
                "Comments (total)": "INT",
                "Shares (organic)": "INT",
                "Shares (sponsored)": "INT",
                "Shares (total)": "INT",
                "Engagement rate (organic)": "DOUBLE",
                "Engagement rate (sponsored)": "DOUBLE",
                "Engagement rate (total)": "DOUBLE",
            },
            "content_posts": {
                "Post Title": "VARCHAR",
                "Post Link": "VARCHAR",
                "Post Type": "VARCHAR",
                "Campaign Name": "VARCHAR",
                "Published by": "VARCHAR",
                "Date": "DATE",  # inferir data diretamente
                "Campaign Start Date": "DATE",  # inferir data diretamente
                "Campaign End Date": "DATE",  # inferir data diretamente
                "Audience": "VARCHAR",
                "Impressions": "INT",
                "Views (excluding off-site video views)": "INT",
                "Off-site Views": "INT",
                "Clicks": "INT",
                "Click-Through Rate (CTR)": "FLOAT",
                "Likes": "INT",
                "Comments": "INT",
                "Shares": "INT",
                "Followers": "INT",
                "Engagement Rate": "FLOAT",
                "Content Type": "VARCHAR",
            },
            "followers_new": {
                "Date": "DATE",  # inferir data diretamente
                "Followers Sponsored": "INT",
                "Followers Organic": "INT",
                "Total Followers": "INT",
            },
            "followers_location": {"Location": "VARCHAR", "Total Followers": "INT"},
            "followers_function": {"Function": "VARCHAR", "Total Followers": "INT"},
            "followers_experience": {
                "Experience Level": "VARCHAR",
                "Total Followers": "INT",
            },
            "followers_industry": {"Industry": "VARCHAR", "Total Followers": "INT"},
            "followers_company_size": {
                "Company Size": "VARCHAR",
                "Total Followers": "INT",
            },
            "visitors_metrics": {
                "Date": "DATE",  # inferir data diretamente
                "Page Views Overview (Desktop)": "INT",
                "Page Views Overview (Mobile Devices)": "INT",
                "Page Views Overview (Total)": "INT",
                "Unique Visitors Overview (Desktop)": "INT",
                "Unique Visitors Overview (Mobile Devices)": "INT",
                "Unique Visitors Overview (Total)": "INT",
                "Page Views Day by Day (Desktop)": "INT",
                "Page Views Day by Day (Mobile Devices)": "INT",
                "Page Views Day by Day (Total)": "INT",
                "Unique Visitors Day by Day (Desktop)": "INT",
                "Unique Visitors Day by Day (Mobile Devices)": "INT",
                "Unique Visitors Day by Day (Total)": "INT",
                "Page Views Jobs (Desktop)": "INT",
                "Page Views Jobs (Mobile Devices)": "INT",
                "Page Views Jobs (Total)": "INT",
                "Unique Visitors Jobs (Desktop)": "INT",
                "Unique Visitors Jobs (Mobile Devices)": "INT",
                "Unique Visitors Jobs (Total)": "INT",
                "Total Page Views (Desktop)": "INT",
                "Total Page Views (Mobile Devices)": "INT",
                "Total Page Views (Total)": "INT",
                "Total Unique Visitors (Desktop)": "INT",
                "Total Unique Visitors (Mobile Devices)": "INT",
                "Total Unique Visitors (Total)": "INT",
            },
            "visitors_location": {"Location": "VARCHAR", "Total Views": "INT"},
            "visitors_function": {"Function": "VARCHAR", "Total Views": "INT"},
            "visitors_experience": {
                "Experience Level": "VARCHAR",
                "Total Views": "INT",
            },
            "visitors_industry": {"Industry": "VARCHAR", "Total Views": "INT"},
            "visitors_company_size": {"Company Size": "VARCHAR", "Total Views": "INT"},
            "competitor": {
                "Page": "VARCHAR",
                "Total Followers": "INT",
                "New Followers": "INT",
                "Total Post Engagements": "FLOAT",
                "Total Posts": "INT",
            },
        }

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
        }

        return table_dict

    def process_content_metrics(self, table):
        """
        Processa a tabela conteúdo_métrica.

        Parâmetros:
        table (str): Nome da tabela a ser processada.

        Retorno:
        int: Retorna 1 se o processamento for bem-sucedido.
        """

        self.con.execute(
            f"""
            CREATE TABLE {table}_temp AS
            SELECT *,
                CASE WHEN "Reactions (total)" >= 0 THEN "Reactions (total)" ELSE 0 END AS "Reactions (positive)",
                CASE WHEN "Comments (total)" >= 0 THEN "Comments (total)" ELSE 0 END AS "Comments (positive)",
                CASE WHEN "Shares (total)" >= 0 THEN "Shares (total)" ELSE 0 END AS "Shares (positive)",
                CASE WHEN "Clicks (total)" >= 0 THEN "Clicks (total)" ELSE 0 END AS "Clicks (positive)"
            FROM {table}
        """
        )

        # Adicionar colunas para média móvel na tabela temporária auxiliar
        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Reactions (moving average)" DOUBLE"""
        )

        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Comments (moving average)" DOUBLE"""
        )
        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Shares (moving average)" DOUBLE"""
        )
        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Clicks (moving average)" DOUBLE"""
        )

        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Reactions (final)" DOUBLE"""
        )
        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Comments (final)" DOUBLE"""
        )
        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Shares (final)" DOUBLE"""
        )
        self.con.execute(
            f"""
            ALTER TABLE {table}_temp
            ADD COLUMN "Clicks (final)" DOUBLE"""
        )

        # Calculando média móvel na tabela temporária auxiliar
        self.con.execute(
            f"""
            UPDATE {table}_temp
            SET "Reactions (moving average)" = (SELECT AVG("Reactions (positive)") FROM {table}_temp t2 WHERE t2."Date" BETWEEN t1."Date" - 2 AND t1."Date"),
                "Comments (moving average)" = (SELECT AVG("Comments (positive)") FROM {table}_temp t2 WHERE t2."Date" BETWEEN t1."Date" - 2 AND t1."Date"),
                "Shares (moving average)" = (SELECT AVG("Shares (positive)") FROM {table}_temp t2 WHERE t2."Date" BETWEEN t1."Date" - 2 AND t1."Date"),
                "Clicks (moving average)" = (SELECT AVG("Clicks (positive)") FROM {table}_temp t2 WHERE t2."Date" BETWEEN t1."Date" - 2 AND t1."Date")
            FROM {table}_temp t1
        """
        )

        # Colunas finais na tabela temporária auxiliar
        self.con.execute(
            f"""
            UPDATE {table}_temp
            SET "Reactions (final)" = CASE WHEN "Reactions (total)" >= 0 THEN "Reactions (total)" ELSE "Reactions (moving average)" END,
                "Comments (final)" = CASE WHEN "Comments (total)" >= 0 THEN "Comments (total)" ELSE "Comments (moving average)" END,
                "Shares (final)" = CASE WHEN "Shares (total)" >= 0 THEN "Shares (total)" ELSE "Shares (moving average)" END,
                "Clicks (final)" = CASE WHEN "Clicks (total)" >= 0 THEN "Clicks (total)" ELSE "Clicks (moving average)" END
        """
        )

        # Copiar os valores finais da tabela temporária auxiliar para a tabela original
        self.con.execute(
            f"""
            UPDATE {table}
            SET "Reactions (total)" = t."Reactions (final)",
                "Comments (total)" = t."Comments (final)",
                "Shares (total)" = t."Shares (final)",
                "Clicks (total)" = t."Clicks (final)"
            FROM {table}_temp t
            WHERE {table}."Date" = t."Date"
        """
        )

        # Deletar a tabela temporária auxiliar
        self.con.execute(f"DROP TABLE {table}_temp")

        return 1

    def add_final_date(self, table):
        """
        Adiciona uma data final a tabela com base no período de extração.

        Parâmetros:
        table: Dicionário contendo o informações da tabela.

        Retorno:
        dict: O mesmo dicionário com a data final adicionada.
        """

        extraction_period = table["extraction_period"]
        year, month, period = extraction_period.split("_")

        month_order_pt = {
            "Jan": 1,
            "Fev": 2,
            "Mar": 3,
            "Abr": 4,
            "Maio": 5,
            "Jun": 6,
            "Jul": 7,
            "Ago": 8,
            "Set": 9,
            "Out": 10,
            "Nov": 11,
            "Dez": 12,
        }
        month = month_order_pt[month]

        if period == "2":
            day = calendar.monthrange(int(year), int(month))[1]
        else:
            day = 15

        final_date = f"{year}-{month}-{day}"

        self.con.execute(
            f"""
            ALTER TABLE {table["db_table_name"]} ADD COLUMN IF NOT EXISTS "Extraction Range" DATE
        """
        )
        self.con.execute(
            f"""
            UPDATE {table["db_table_name"]} SET "Extraction Range" = '{final_date}'
        """
        )

        return 1

    def transform_data(self, tables):
        """
        Aplica uma série de transformações aos dados extraídos.

        Parâmetros:
        tables (list): Lista de dicionários contendo os dados extraídos.

        Retorno:
        list: Lista de dicionários contendo os dados transformados.
        """
        for table in tables:
            if table["dataframe_name"] == "content_metrics":
                self.process_content_metrics(table["db_table_name"])

            self.add_final_date(table)
        return tables

    def get_clean_concatenated_data(self, concatenated_file_prefix="all_extractions_"):
        clean_data_tables = []
        for filename in os.listdir(self.clean_concatenated_directory):
            file_path = os.path.join(self.clean_concatenated_directory, filename)

            dataframe_name = filename.replace(concatenated_file_prefix, "").replace(
                ".csv", ""
            )
            dataframe_name = f"clean_{dataframe_name}"

            create_query = f"""
                CREATE TABLE "{dataframe_name}" AS
                SELECT * FROM read_csv_auto('{file_path}')
            """
            self.con.execute(create_query)

            clean_data_tables.append(dataframe_name)

        return clean_data_tables

    def get_raw_unique_extraction_data(self, extraction_period="2035_Jan_1"):
        files = []
        for file in os.listdir(self.unique_extraction_directory):
            files.append(
                {
                    "filename": file,
                    "file_path": os.path.join(self.unique_extraction_directory, file),
                    "category": self.detect_file_category(file),
                    "dir": ["-"],
                    "extraction_period": extraction_period,  # f"{year}-{month}-{i+1}"
                }
            )

        extraction_data = [obj for file in files for obj in self.read_excel_file(file)]
        return extraction_data

    def concatenate_unique_extraction_data(self, clean_tables, extraction_tables):
        concatenated_tables = []
        for table in extraction_tables:
            table_name = table["dataframe_name"]

            query = f"""
                CREATE OR REPLACE TABLE "{table_name}" AS
                SELECT * FROM "clean_{table_name}"
                UNION ALL
                SELECT * FROM "{table["db_table_name"]}"
            """

            self.con.execute(query)

            concatenated_tables.append(table_name)
        return concatenated_tables

    def export_dataframes(self, tables):
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)

        for table in tables:
            export_filename = f"{table}.csv"

            query = f"""
                COPY {table} TO '{os.path.join(self.export_dir, export_filename)}' (HEADER, DELIMITER ';')
"""

            self.con.execute(query)

        return 1


def main():
    clean_concatenated_directory = "data/linkedin/clean/duckdb/concatenated_dataframes"
    unique_extraction_directory = "data/linkedin/raw_unique_extraction"
    export_dir = "data/linkedin/clean/m2/duckdb"

    etl = EtlLinkedinDuckDb(clean_concatenated_directory, unique_extraction_directory, export_dir)

    clean_tables = etl.get_clean_concatenated_data(clean_concatenated_directory)

    extraction_data_pandas = etl.get_raw_unique_extraction_data()

    extraction_tables = etl.convert_dataframes_to_duckdb(extraction_data_pandas)
    etl.transform_data(extraction_tables)
    concatenated_tables = etl.concatenate_unique_extraction_data(
        clean_tables, extraction_tables
    )

    etl.export_dataframes(concatenated_tables)


if __name__ == "__main__":
    # temp - debug
    # delete clean_dir
    import shutil

    if os.path.exists("data/linkedin/clean/m2/duckdb"):
        shutil.rmtree("data/linkedin/clean/m2/duckdb")

    main()
