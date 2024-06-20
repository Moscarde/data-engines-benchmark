import polars as pl
import os
import calendar
import re


class EtlLinkedinPolars:
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

    # def get_raw_files(self, raw_directory):
    #     """
    #     Detecta e retorna uma lista de arquivos brutos a serem processados.

    #     Parâmetros:
    #     raw_directory (str): Diretório contendo os dados brutos.

    #     Retorno:
    #     list: Lista de dicionários com informações sobre os arquivos brutos.
    #     """
    #     extraction_files = []
    #     for category in os.listdir(raw_directory):
    #         category_path = os.path.join(raw_directory, category)

    #         for year in os.listdir(category_path):
    #             year_path = os.path.join(category_path, year)

    #             for month in os.listdir(year_path):
    #                 month_path = os.path.join(year_path, month)

    #                 monthly_files = os.listdir(month_path)
    #                 if not monthly_files:
    #                     continue

    #                 for i, file in enumerate(monthly_files):
    #                     file_path = os.path.join(month_path, file)
    #                     df_category = self.detect_file_category(file)
    #                     extraction_files.append(
    #                         {
    #                             "category": df_category,
    #                             "file_path": file_path,
    #                             "dir": [category, year, month],
    #                             "extraction_period": f"{year}-{month}-{i+1}",
    #                         }
    #                     )
    #     return extraction_files

    def read_excel_file(self, file):
        """
        Lê um arquivo Excel e retorna seus dados como uma lista de DataFrames.

        Parâmetros:
        file (dict): Dicionário com informações sobre o arquivo, incluindo categoria, caminho e período de extração.

        Retorno:
        list: Lista de dicionários contendo o nome do DataFrame, diretório, período de extração e o DataFrame.
        """
        category_keys = {
            "competitor": [{"sheet_name": "competitor", "sheet_pos": 1, "skiprows": 1}],
            "content": [
                {"sheet_name": "content_metrics", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "content_posts", "sheet_pos": 2, "skiprows": 0},
            ],
            "followers": [
                {"sheet_name": "followers_new", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "followers_location", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "followers_function", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "followers_experience", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "followers_industry", "sheet_pos": 5, "skiprows": 0},
                {"sheet_name": "followers_company_size", "sheet_pos": 6, "skiprows": 0},
            ],
            "visitors": [
                {"sheet_name": "visitors_metrics", "sheet_pos": 1, "skiprows": 0},
                {"sheet_name": "visitors_location", "sheet_pos": 2, "skiprows": 0},
                {"sheet_name": "visitors_function", "sheet_pos": 3, "skiprows": 0},
                {"sheet_name": "visitors_experience", "sheet_pos": 4, "skiprows": 0},
                {"sheet_name": "visitors_industry", "sheet_pos": 5, "skiprows": 0},
                {"sheet_name": "visitors_company_size", "sheet_pos": 6, "skiprows": 0},
            ],
        }

        sheets_to_read = category_keys[file["category"]]

        dataframes = []
        for sheet in sheets_to_read:
            # Original Pandas
            # df = pd.read_excel(
            #     file["file_path"],
            #     sheet_name=sheet["sheet_pos"],
            #     skiprows=sheet["skiprows"],
            # )

            df = pl.read_excel(
                source=file["file_path"],
                sheet_id=sheet["sheet_pos"],
                read_options={"skip_rows": sheet["skiprows"]},
            )

            if file["category"] == "content":
                first_row = df.row(0)
                df.columns = first_row
                df = df.slice(1, df.height)

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

        final_date = f"{month}/{day}/{year}"

        dataframe["df"] = dataframe["df"].with_columns(
            pl.lit(final_date).alias("Extraction Range")
        )
        return dataframe

    def convert_column_types(
        self,
        dataframe,
    ):
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
            first_value = (
                dataframe["df"][column][0] if len(dataframe["df"][column]) > 0 else None
            )
            pattern = (
                "%m/%d/%Y"
                if re.match(r"\d{1,2}/\d{1,2}/\d{4}", str(first_value))
                else "%Y-%m-%d"
            )
            dataframe["df"] = (
                dataframe["df"]
                # .filter(pl.col(column).is_not_null())
                .with_columns(pl.col(column).str.to_date(pattern, strict=False))
            )

        return dataframe

    def clean_content_metrics_data(self, dataframe):
        """
        Limpa e processa os dados de conteúdo metricas.

        Parâmetros:
        dataframe (dict): Dicionário contendo o DataFrame e suas informações.

        Retorno:
        dict: O mesmo dicionário com os dados de métricas de conteúdo limpos.
        """
        df = dataframe["df"]

        column_type = {
            "Date": pl.Date,  # temp
            "Impressions (organic)": pl.Int64,
            "Impressions (sponsored)": pl.Int64,
            "Impressions (total)": pl.Int64,
            "Unique impressions (organic)": pl.Int64,
            "Clicks (organic)": pl.Int64,
            "Clicks (sponsored)": pl.Int64,
            "Clicks (total)": pl.Int64,
            "Reactions (organic)": pl.Int64,
            "Reactions (sponsored)": pl.Int64,
            "Reactions (total)": pl.Int64,
            "Comments (organic)": pl.Int64,
            "Comments (sponsored)": pl.Int64,
            "Comments (total)": pl.Int64,
            "Shares (organic)": pl.Int64,
            "Shares (sponsored)": pl.Int64,
            "Shares (total)": pl.Int64,
            "Engagement rate (organic)": pl.Float64,
            "Engagement rate (sponsored)": pl.Float64,
            "Engagement rate (total)": pl.Float64,
        }
        df = df.cast(column_type)

        df = df.with_columns(
            pl.when(pl.col("Reactions (total)") >= 0)
            .then(pl.col("Reactions (total)"))
            .otherwise(pl.lit(0))
            .alias("Reactions (positive)"),
            pl.when(pl.col("Comments (total)") >= 0)
            .then(pl.col("Comments (total)"))
            .otherwise(pl.lit(0))
            .alias("Comments (positive)"),
            pl.when(pl.col("Shares (total)") >= 0)
            .then(pl.col("Shares (total)"))
            .otherwise(pl.lit(0))
            .alias("Shares (positive)"),
            pl.when(pl.col("Clicks (total)") >= 0)
            .then(pl.col("Clicks (total)"))
            .otherwise(pl.lit(0))
            .alias("Clicks (positive)"),
        )

        df = df.with_columns(
            (pl.col("Reactions (positive)"))
            .rolling_mean(window_size=3)
            .alias("Reactions (moving average)"),
            (pl.col("Comments (positive)"))
            .rolling_mean(window_size=3)
            .alias("Comments (moving average)"),
            (pl.col("Shares (positive)"))
            .rolling_mean(window_size=3)
            .alias("Shares (moving average)"),
            (pl.col("Clicks (positive)"))
            .rolling_mean(window_size=3)
            .alias("Clicks (moving average)"),
        )

        df = df.with_columns(
            pl.when(pl.col("Reactions (total)") >= 0)
            .then(pl.col("Reactions (total)"))
            .otherwise("Reactions (moving average)")
            .alias("Reactions (final)"),
            pl.when(pl.col("Comments (total)") >= 0)
            .then(pl.col("Comments (total)"))
            .otherwise("Comments (moving average)")
            .alias("Comments (final)"),
            pl.when(pl.col("Shares (total)") >= 0)
            .then(pl.col("Shares (total)"))
            .otherwise("Shares (moving average)")
            .alias("Shares (final)"),
            pl.when(pl.col("Clicks (total)") >= 0)
            .then(pl.col("Clicks (total)"))
            .otherwise("Clicks (moving average)")
            .alias("Clicks (final)"),
        )

        engagement_sum = (
            pl.col("Reactions (final)")
            + pl.col("Comments (final)")
            + pl.col("Clicks (final)")
            + pl.col("Shares (final)")
        )

        df = df.with_columns(
            (engagement_sum / pl.col("Impressions (total)")).alias(
                "Engagement rate (calculed)"
            )
        )

        df_final = df.select(
            [
                "Date",
                "Impressions (total)",
                "Reactions (final)",
                "Comments (final)",
                "Clicks (final)",
                "Shares (final)",
                "Engagement rate (calculed)",
                "Extraction Range",
            ]
        )
        df_final.columns = [
            "Date",
            "Impressions (total)",
            "Reactions (total)",
            "Comments (total)",
            "Clicks (total)",
            "Shares (total)",
            "Engagement Rate (total)",
            "Extraction Range",
        ]

        dataframe["df"] = df_final

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
            # dataframe["concatenated_df"].to_csv(
            #     full_path, index=False, quoting=csv.QUOTE_ALL
            # )
            dataframe["concatenated_df"].write_csv(full_path, quote_style="always")
        return 1

    def get_clean_concatenated_data(self, concatenated_file_prefix="all_extractions_"):
        clean_data = {}
        for filename in os.listdir(self.clean_concatenated_directory):

            # creating obj to send in convert_column_types
            dataframe = {}
            dataframe["dataframe_name"] = filename.replace(
                concatenated_file_prefix, ""
            ).replace(".csv", "")
            dataframe["df"] = pl.read_csv(
                os.path.join(self.clean_concatenated_directory, filename),
            )

            dataframe = self.convert_column_types(dataframe)

            # force conversion in content_metrics
            if dataframe["dataframe_name"] == "content_metrics":
                column_type = {
                    "Date": pl.Date,  # temp
                    "Impressions (total)": pl.Int64,
                    "Clicks (total)": pl.Float64,
                    "Reactions (total)": pl.Float64,
                    "Comments (total)": pl.Float64,
                    "Shares (total)": pl.Float64,
                    "Engagement Rate (total)": pl.Float64,
                }
                dataframe["df"] = dataframe["df"].cast(column_type, strict=False)

            if dataframe["dataframe_name"] == "content_posts":
                column_type = {
                    "Post Title": pl.String,
                    "Post Link": pl.String,
                    "Post Type": pl.String,
                    "Campaign Name": pl.String,
                    "Published by": pl.String,
                    "Date": pl.Date,
                    "Campaign Start Date": pl.Date,
                    "Campaign End Date": pl.Date,
                    "Audience": pl.String,
                    "Impressions": pl.String,
                    "Views (excluding off-site video views)": pl.String,
                    "Off-site Views": pl.String,
                    "Clicks": pl.String,
                    "Click-Through Rate (CTR)": pl.String,
                    "Likes": pl.String,
                    "Comments": pl.String,
                    "Shares": pl.String,
                    "Followers": pl.String,
                    "Engagement Rate": pl.String,
                    "Content Type": pl.String,
                    "Extraction Range": pl.Date,
                }
                dataframe["df"] = dataframe["df"].cast(column_type, strict=False)

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
            if data["df"].height == 0:
                continue

            concatenated_data[data["dataframe_name"]] = {}
            concatenated_data[data["dataframe_name"]]["category"] = data[
                "dataframe_name"
            ]

            df1 = clean_dataframes[data["dataframe_name"]]
            df2 = data["df"]

            if df1.columns != df2.columns:
                # Reordenar as colunas do segundo DataFrame para que correspondam à ordem do primeiro DataFrame
                df2 = df2.select(df1.columns)

            df_merged = pl.concat([df1, df2])
            concatenated_data[data["dataframe_name"]]["concatenated_df"] = df_merged

        return concatenated_data


def main():
    clean_concatenated_directory = "data/linkedin/clean/polars/concatenated_dataframes"
    unique_extraction_directory = "data/linkedin/raw_unique_extraction"
    export_dir = "data/linkedin/clean/m2/polars"

    etl = EtlLinkedinPolars(
        clean_concatenated_directory, unique_extraction_directory, export_dir
    )

    clean_dataframes = etl.get_clean_concatenated_data()

    extraction_data = etl.get_raw_unique_extraction_data()

    extraction_data = etl.transform_data(extraction_data)

    concatenated_data = etl.concatenate_unique_extraction_data(
        clean_dataframes, extraction_data
    )
    etl.export_dataframes(concatenated_data)


if __name__ == "__main__":
    # debug
    # delete clean_dir
    import shutil

    if os.path.exists("data/linkedin/clean/m2/polars"):
        shutil.rmtree("data/linkedin/clean/m2/polars")

    main()
