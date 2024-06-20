import pandas as pd
import os
import csv
import calendar

import warnings

warnings.simplefilter("ignore")


class EtlLinkedinPandas:
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

    def translate_cols(self, dataframe):
        """
        Traduza os nomes das colunas de um DataFrame para o inglês.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com os nomes das colunas traduzidos.
        """
        translated_columns = {
            "content_metrics": [
                "Date",
                "Impressions (organic)",
                "Impressions (sponsored)",
                "Impressions (total)",
                "Unique impressions (organic)",
                "Clicks (organic)",
                "Clicks (sponsored)",
                "Clicks (total)",
                "Reactions (organic)",
                "Reactions (sponsored)",
                "Reactions (total)",
                "Comments (organic)",
                "Comments (sponsored)",
                "Comments (total)",
                "Shares (organic)",
                "Shares (sponsored)",
                "Shares (total)",
                "Engagement rate (organic)",
                "Engagement rate (sponsored)",
                "Engagement rate (total)",
            ],
            "content_posts": [
                "Post Title",
                "Post Link",
                "Post Type",
                "Campaign Name",
                "Published by",
                "Date",
                "Campaign Start Date",
                "Campaign End Date",
                "Audience",
                "Impressions",
                "Views (excluding off-site video views)",
                "Off-site Views",
                "Clicks",
                "Click-Through Rate (CTR)",
                "Likes",
                "Comments",
                "Shares",
                "Followers",
                "Engagement Rate",
                "Content Type",
            ],
            "followers_new": [
                "Date",
                "Followers Sponsored",
                "Followers Organic",
                "Total Followers",
            ],
            "followers_location": ["Location", "Total Followers"],
            "followers_function": ["Function", "Total Followers"],
            "followers_experience": ["Experience Level", "Total Followers"],
            "followers_industry": ["Industry", "Total Followers"],
            "followers_company_size": ["Company Size", "Total Followers"],
            "visitors_metrics": [
                "Date",
                "Page Views Overview (Desktop)",
                "Page Views Overview (Mobile Devices)",
                "Page Views Overview (Total)",
                "Unique Visitors Overview (Desktop)",
                "Unique Visitors Overview (Mobile Devices)",
                "Unique Visitors Overview (Total)",
                "Page Views Day by Day (Desktop)",
                "Page Views Day by Day (Mobile Devices)",
                "Page Views Day by Day (Total)",
                "Unique Visitors Day by Day (Desktop)",
                "Unique Visitors Day by Day (Mobile Devices)",
                "Unique Visitors Day by Day (Total)",
                "Page Views Jobs (Desktop)",
                "Page Views Jobs (Mobile Devices)",
                "Page Views Jobs (Total)",
                "Unique Visitors Jobs (Desktop)",
                "Unique Visitors Jobs (Mobile Devices)",
                "Unique Visitors Jobs (Total)",
                "Total Page Views (Desktop)",
                "Total Page Views (Mobile Devices)",
                "Total Page Views (Total)",
                "Total Unique Visitors (Desktop)",
                "Total Unique Visitors (Mobile Devices)",
                "Total Unique Visitors (Total)",
            ],
            "visitors_location": ["Location", "Total Views"],
            "visitors_function": ["Function", "Total Views"],
            "visitors_experience": ["Experience Level", "Total Views"],
            "visitors_industry": ["Industry", "Total Views"],
            "visitors_company_size": ["Company Size", "Total Views"],
            "competitor": [
                "Page",
                "Total Followers",
                "New Followers",
                "Total Post Engagements",
                "Total Posts",
            ],
        }

        dataframe["df"].columns = translated_columns.get(dataframe["dataframe_name"])
        return dataframe

    def add_final_date(self, dataframe):
        """
        Adiciona uma data final ao DataFrame com base no período de extração.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com a data final adicionada.
        """
        extraction_period = dataframe["extraction_period"]
        year, month, period = extraction_period.split("-")

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

        dataframe["df"]["Extraction Range"] = final_date
        return dataframe

    def convert_column_types(self, dataframe):
        """
        Converte colunas específicas do DataFrame para o tipo de dado adequado.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com os tipos de dados das colunas convertidos.
        """
        date_columns = {
            "content_metrics": ["Date"],
            "content_posts": ["Date", "Campaign Start Date", "Campaign End Date"],
            "followers_new": ["Date"],
            "visitors_metrics": ["Date"],
        }

        columns_to_convert = date_columns.get(dataframe["dataframe_name"], [])
        columns_to_convert.append("Extraction Range")

        for column in columns_to_convert:
            dataframe["df"][column] = pd.to_datetime(dataframe["df"][column])

        return dataframe

    def clean_content_metrics_data(self, dataframe):
        """
        Limpa e processa os dados de conteúdo metricas.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com os dados de métricas de conteúdo limpos.
        """
        df = dataframe["df"][
            [
                "Date",
                "Impressions (total)",
                "Clicks (total)",
                "Reactions (total)",
                "Comments (total)",
                "Shares (total)",
                "Engagement rate (total)",
                "Extraction Range",
            ]
        ]

        df["Reactions (positive)"] = df["Reactions (total)"][
            df["Reactions (total)"] >= 0
        ]
        df["Comments (positive)"] = df["Comments (total)"][df["Comments (total)"] >= 0]
        df["Shares (positive)"] = df["Shares (total)"][df["Shares (total)"] >= 0]
        df["Clicks (positive)"] = df["Clicks (total)"][df["Clicks (total)"] >= 0]

        df["Reactions (positive)"] = df["Reactions (positive)"].fillna(0)
        df["Comments (positive)"] = df["Comments (positive)"].fillna(0)
        df["Shares (positive)"] = df["Shares (positive)"].fillna(0)
        df["Clicks (positive)"] = df["Clicks (positive)"].fillna(0)

        window = 3

        df["Reactions (moving average)"] = (
            df["Reactions (positive)"].rolling(window=window).mean()
        )
        df["Comments (moving average)"] = (
            df["Comments (positive)"].rolling(window=window).mean()
        )
        df["Shares (moving average)"] = (
            df["Shares (positive)"].rolling(window=window).mean()
        )
        df["Clicks (moving average)"] = (
            df["Clicks (positive)"].rolling(window=window).mean()
        )

        df["Reactions (total)"] = df.apply(
            lambda row: (
                row["Reactions (moving average)"]
                if row["Reactions (total)"] < 0
                else row["Reactions (total)"]
            ),
            axis=1,
        )

        df["Comments (total)"] = df.apply(
            lambda row: (
                row["Comments (moving average)"]
                if row["Comments (total)"] < 0
                else row["Comments (total)"]
            ),
            axis=1,
        )

        df["Shares (total)"] = df.apply(
            lambda row: (
                row["Shares (moving average)"]
                if row["Shares (total)"] < 0
                else row["Shares (total)"]
            ),
            axis=1,
        )

        df["Clicks (total)"] = df.apply(
            lambda row: (
                row["Clicks (moving average)"]
                if row["Clicks (total)"] < 0
                else row["Clicks (total)"]
            ),
            axis=1,
        )

        df["Engagement Rate (total)"] = df.apply(
            lambda row: (
                row["Reactions (total)"]
                + row["Comments (total)"]
                + row["Clicks (total)"]
                + row["Shares (total)"]
            )
            / row["Impressions (total)"],
            axis=1,
        )

        dataframe["df"] = df[
            [
                "Date",
                "Impressions (total)",
                "Clicks (total)",
                "Reactions (total)",
                "Comments (total)",
                "Shares (total)",
                "Engagement Rate (total)",
                "Extraction Range",
            ]
        ]

        return dataframe

    def transform_data(self, data):
        """
        Aplica uma série de transformações aos dados extraídos.

        Parâmetros:
        data (list): Lista de dicionários contendo os dados extraídos.

        Retorno:
        list: Lista de dicionários contendo os dados transformados.
        """
        for dataframe in data:

            dataframe = self.translate_cols(dataframe)
            dataframe = self.add_final_date(dataframe)
            dataframe = self.convert_column_types(dataframe)
            if dataframe["dataframe_name"] == "content_metrics":
                dataframe = self.clean_content_metrics_data(dataframe)

        return data

    def export_dataframes(self, data):
        """
        Exporta dataframes concatenados para um arquivo CSV.

        Parâmetros:
        data (dict): Dicionário com os DataFrames concatenados.
        file_prefix (str): Tipo de exportação (e.g., 'month', 'clean').

        Retorno:
        int: Retorna 1 se a exportação for bem-sucedida.
        """
        for key, dataframe in data.items():
            export_dir = self.export_dir
            export_filename = f"{dataframe['category']}.csv"

            if os.path.exists(export_dir) == False:
                os.makedirs(export_dir)

            full_path = os.path.join(export_dir, export_filename)
            dataframe["concatenated_df"].to_csv(
                full_path, index=False, quoting=csv.QUOTE_ALL
            )
        return 1

    def get_clean_concatenated_data(self, concatenated_file_prefix="all_extractions_"):
        clean_data = {}
        for filename in os.listdir(self.clean_concatenated_directory):

            # creating obj to send in convert_column_types
            dataframe = {}
            dataframe["dataframe_name"] = filename.replace(
                concatenated_file_prefix, ""
            ).replace(".csv", "")
            dataframe["df"] = pd.read_csv(
                os.path.join(self.clean_concatenated_directory, filename)
            )
            dataframe = self.convert_column_types(dataframe)

            # adding dataframe to clean_data
            clean_data[dataframe["dataframe_name"]] = dataframe["df"]
        return clean_data

    def get_raw_unique_extraction_data(self, extraction_period="2035-Jan-1"):
        """
        Função que le os arquivos de extração unica e retorna uma lista de dicionários contendo as informações extraídas.

        Parâmetros:
        extraction_period (str): Período de extração (ano-mês-dia) para o arquivo. (default: '2030-Jan-1' para fins dedebug)

        Retorno:
        list: Lista de dicionários contendo os dados extraídos.
        """
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

    def concatenate_unique_extraction_data(self, clean_dataframes, extraction_data):
        concatenated_data = {}
        for data in extraction_data:
            concatenated_data[data["dataframe_name"]] = {}
            concatenated_data[data["dataframe_name"]]["category"] = data[
                "dataframe_name"
            ]

            df1 = clean_dataframes[data["dataframe_name"]]
            df2 = data["df"]
            df_merged = pd.concat([df1, df2])
            concatenated_data[data["dataframe_name"]]["concatenated_df"] = df_merged

        return concatenated_data


def main():
    """
    Função principal que executa as operações ETL Linkedin.
    """
    clean_concatenated_directory = "data/linkedin/clean/pandas/concatenated_dataframes"
    unique_extraction_directory = "data/linkedin/raw_unique_extraction"
    export_dir = "data/linkedin/clean/m2/pandas"

    etl = EtlLinkedinPandas(clean_concatenated_directory, unique_extraction_directory, export_dir)

    clean_dataframes = etl.get_clean_concatenated_data()
    extraction_data = etl.get_raw_unique_extraction_data()
    extraction_data = etl.transform_data(extraction_data)
    merged_data = etl.concatenate_unique_extraction_data(
        clean_dataframes, extraction_data
    )
    etl.export_dataframes(merged_data)


if __name__ == "__main__":
    import shutil

    if os.path.exists("data/linkedin/clean/m2/pandas"):
        shutil.rmtree("data/linkedin/clean/m2/pandas")
    main()
