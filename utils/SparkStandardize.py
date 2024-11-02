from datetime import datetime
from minio import Minio
from pyspark.sql import SparkSession
from utils.SendMinio import SendMinio
from pyspark.sql.functions import current_timestamp, lit, to_timestamp
import pandas as pd
import traceback
import tempfile
import os
from enum import Enum

class RAWFileTypes(Enum):
    CSV = 'csv'
    JSON = 'json'

class SparkStandardize:
    """
    Classe contendo funções para realizar normalizações em tabelas extraídas do cliente e disponíveis na camada RAW.
    Ao final, as tabelas normalizadas devem ir para a camada STANDARDIZED.
    Attributes:
        ingestion_date (str): Data de ingestão no formato YYYYMMDD. Por padrão é a data atual.
                            Somente arquivos dessa data serão normalizados.
        check_table_integrity (bool): Realiza verificação se foram recebidos registros duplicados (todas as colunas exatamente iguais). 
                                Aumenta um pouco o tempo para normalização.
        
    Methods:
        normalize_table_files_to_parquet: Normaliza tabelas em formato file_type da camada RAW para tabelas na camada STANDARDIZED em formato Parquet.
    """
    
    def __init__(self, 
                 #ingestion_date = "20240131",
                 ingestion_date = datetime.today().strftime("%Y%m%d"),
                 check_table_integrity = True
                ):
        """
        Inicializa as variáveis e componentes que serão usados
        """
        self.__ingestion_date = ingestion_date
        self.__check_table_integrity = check_table_integrity
        self.__file_types_allowed = [file_type.value for file_type in list(RAWFileTypes)]

        self.__origem_bucket_name = 'raw'
        self.__destination_bucket_name = 'standardized'

        self.__start_minion_client()
        self.__init_origin_bucket()
        self.__init_destination_bucket()

    def __load_spark_session(self):
        """
        Iniializa a sessão do Spark
        """
        self.__spark = SparkSession.builder \
                                    .appName(f"Convert {self.__file_type} - From Raw to Standardized") \
                                    .config("spark.driver.memory", "20g") \
                                    .config("spark.executor.memory", "20g") \
                                    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
                                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                                    .config("spark.databricks.delta.schema.autoMerge.enabled", True)\
                                    .getOrCreate()
        

    def __start_minion_client(self):
        """
        Inicializa o cliente do MinIO. Gera uma conexão com o MinIO (Data Lake da solução)
        """        
        self.__minio_client = Minio(
            os.getenv('MINIO_HOST'),
            access_key = os.getenv('MINIO_ACESS_KEY'),
            secret_key = os.getenv('MINIO_SECRET_KEY'),
            secure = False
        )

    def __init_origin_bucket(self):
        """
        Inicializa o bucket da camada de origem (RAW)
        """
        self.__origin_bucket = SendMinio(self.__minio_client, self.__origem_bucket_name, "", "")

    def __init_destination_bucket(self):
        """
        Inicializa o bucket da camada de destino (STANDARDIZED)
        """
        self.__destination_bucket = SendMinio(self.__minio_client, self.__destination_bucket_name, "", "")

    def __load_files_from_raw(self):
        """
        Procura pelos arquivos da camada RAW que possuem a extensão fornecida.
        """
        if self.__file_type not in self.__file_types_allowed:
            raise Exception(f"Tipo de arquivo não informado! Valores válidos: {self.__file_types_allowed}")
            
        files = self.__origin_bucket.findFilesInBucket(prefix="")

        self.__files_to_convert = []
        
        for file in files:
            file_name = file.object_name
            
            if self.__file_type in file.object_name:
                path_arr = file.object_name.split('/')
                
                if len(path_arr) == 5:
                    business_area = path_arr[0]
                    source_system = path_arr[1]
                    endpoint = path_arr[2]
                    date_extraction = path_arr[3]
                    file_name = path_arr[4]
                    
                    if date_extraction == self.__ingestion_date:
                        self.__files_to_convert.append({
                            'business_area': business_area,
                            'source_system': source_system,
                            'endpoint': endpoint,
                            'date_extraction': date_extraction,
                            'file_name': file_name,
                            'file_path': file.object_name
                        })
                        
        if not self.__files_to_convert:
            raise Exception("Nenhum arquivo foi encontrado")

        print("Arquivos encontrados:")
        print([file["file_path"] for file in self.__files_to_convert])
    

    def __spark_read_raw_file(self, file):
        """
        Carrega o arquivo através do Spark
        """
        if self.__file_type == RAWFileTypes.CSV.value:
            return self.__spark.read\
                                .option("inferSchema", True)\
                                .option("header", True)\
                                .csv(file)

        if self.__file_type == RAWFileTypes.JSON.value:
            return self.__spark.read\
                                .option("multiline", "true")\
                                .json(file)


    def __add_metadata_to_dataframe(self, df):
        """
        Adiciona os metadados padrão no dataframe

        Args:
            df (pyspark.sql.dataframe.DataFrame): DataFrame Spark que será enriquecido com os metadados.

        Returns:
            pyspark.sql.dataframe.DataFrame: DataFrame Spark com as novas colunas adicionadas.
        """
        df = df.withColumn('dt_carga', current_timestamp())
        df = df.withColumn('etl_versao', lit(1))
        df = df.withColumn('etl_dt_inicio', lit('1900-01-01 00:00:00.000'))
        df = df.withColumn('etl_dt_fim', lit('2200-01-01 00:00:00.000'))
        df = df.withColumn('etl_dt_inicio', to_timestamp(df['etl_dt_inicio'], 'yyyy-MM-dd HH:mm:ss.SSS'))
        df = df.withColumn('etl_dt_fim', to_timestamp(df['etl_dt_fim'], 'yyyy-MM-dd HH:mm:ss.SSS'))
        
        return df

    def __send_files_to_minio(self, df, new_file_path):
        """
        Envia o arquivo parquet para a camada de destino (STANDARDIZED) no MinIO.
        
        Args:
            df (pyspark.sql.dataframe.DataFrame): Dataframe do Spark que será salvo para parquet e enviado para o destino.
            new_file_path (str): Caminho do arquivo na cadama de origem.
        """
        with tempfile.TemporaryDirectory(suffix='_parquet_folder_') as temp_dir:
            print("Convertendo o Dataframe do Spark para Parquet e salvando localmente")
            df.write \
                 .format("parquet") \
                 .mode("overwrite") \
                 .save(temp_dir)
            print("Salvo com sucesso")

            print(f"Removendo arquivos pré-existentes da STANDARDIZED em:\t{new_file_path}")
            self.__destination_bucket.removeFiles(new_file_path)

            print("Fazendo o upload dos arquivos Parquet locais para a camada destino no MinIO")
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    remote_object_name = os.path.join(new_file_path, file)
                    
                    with open(local_file_path, "rb") as file_data:
                        self.__destination_bucket = SendMinio(self.__minio_client, self.__destination_bucket_name, remote_object_name, local_file_path)
                        self.__destination_bucket.send2bucket()
                print("Enviado com sucesso")

    def __send_files_to_minio_as_delta(self, df, new_file_path):
        """
        Envia o arquivo parquet para a camada de destino em formato DELTA
        
        Args:
            df (pyspark.sql.dataframe.DataFrame): Dataframe do Spark que será salvo para parquet e enviado para o destino.
            new_file_path (str): Caminho do arquivo na cadama de origem.
        """
        with tempfile.TemporaryDirectory(suffix='_parquet_folder_') as temp_dir:
            print("Convertendo o Dataframe do Spark para Parquet...")
            df.write \
                 .format("delta") \
                 .mode("overwrite") \
                 .save(temp_dir)
            #df.to_parquet(temp_dir)

            print(f"Removendo arquivos pré-existentes da STANDARDIZED em:\n{new_file_path}")
            self.__destination_bucket.removeFiles(new_file_path)

            print("Fazendo o upload dos arquivos Parquet para a camada destino...")
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    local_file_path = os.path.join(root, file)
                    remote_object_name = os.path.join(new_file_path, file)
                    
                    with open(local_file_path, "rb") as file_data:
                        self.__destination_bucket = SendMinio(self.__minio_client, self.__destination_bucket_name, remote_object_name, local_file_path)
                        self.__destination_bucket.send2bucket()   
                        
    def __integrity_check(self,df):
        """Realiza verificação se foram recebidos registros duplicados (todas as colunas exatamente iguais) em um DataFrame. 
        Aumenta um pouco o tempo para normalização.
        """
        df_no_duplicates = df.dropDuplicates()
        if df.count() > df_no_duplicates.count():
            return False
        else:
            return True
    
    def normalize_table_files_to_parquet(self, file_type=None, business_area=None):
        """
        Normaliza tabelas em formato file_type da camada RAW para tabelas na camada STANDARDIZED em formato Parquet.

        Args:
            file_type (str): Tipo de arquivo para ser localizado na camada de origem. Exemplo "csv", "json".
            business_area (str): Área de negócios para filtrar os arquivos a serem processados.

        Exemplo:
            >>> normalize_table_files_to_parquet('csv', 'orcamentos')
        """
        self.__file_type = file_type
        self.__load_files_from_raw()

        # Filtra os arquivos pela área de negócios
        if business_area:
            self.__files_to_convert = [file for file in self.__files_to_convert if file['business_area'] == business_area]

        duplicated_data_files = []
        
        for file in self.__files_to_convert:
            try:
                print(f"\n----- Processando arquivo {file['file_name']} -----")
                print(f"Path: {file['file_path']}")
                
                self.__load_spark_session()

                with tempfile.NamedTemporaryFile(delete=True, suffix='_raw_file_') as temp_file:
                    print("Fazendo download da tabela da camada RAW...")
                    self.__origin_bucket.downloadFile(file['file_path'], temp_file.name)         
                                
                    print("Carregando tabela para o Spark...")
                    df = self.__spark_read_raw_file(temp_file.name)         

                    if self.__check_table_integrity:
                        print("--- VERIFICAÇÃO DE INTEGRIDADE ---")
                        if not self.__integrity_check(df):
                            print(f"O arquivo {file['file_path']} pode conter registros duplicados!")
                            duplicated_data_files.append(file)
                        else:
                            print("OK!")
                        print("--- ---")
                        
                    print('Adicionando metadados')
                    df = self.__add_metadata_to_dataframe(df)                                       

                    print("Enviando tabela normalizada para o MinIO")
                    new_file_path = file['file_path'].replace(self.__file_type, 'parquet')        
                    self.__send_files_to_minio(df, new_file_path)
                                
            except Exception as e:
                print("Erro ao converter o arquivo: ", file['file_name'])                
                traceback.print_exc()
            finally:
                self.__spark.stop()
        
        print("----- Processamento Finalizado! -----")
        return duplicated_data_files        

    def normalize_table_files_to_delta(self, file_type = None):
        """
        Converte os arquivos da camada RAW para Parquet.

        Args:
            file_type (str): Tipo de arquivo para ser localizado na camada de origem. Exemplo "csv", "json".
        
        Exemplo:
            >>> normalize_table_files_to_parquet('csv')
        """
        self.__file_type = file_type
        self.__load_files_from_raw()

        for file in self.__files_to_convert:
            try:
                print(f"\n----- Processando arquivo {file['file_name']} -----")
                print(f"Path: {file['file_path']}")
                
                self.__load_spark_session()

                with tempfile.NamedTemporaryFile(delete=True, suffix='_raw_file_') as temp_file:
                    print("Fazendo download do arquivo da camada RAW...")
                    self.__origin_bucket.downloadFile(file['file_path'], temp_file.name)         
                                
                    print("Carregando o arquivo para o Spark...")
                    df = self.__spark_read_raw_file(temp_file.name)         
                    # print(F"Quantidade de linhas no df {}")


                    print('Adicionando metadados')
                    df = self.__add_metadata_to_dataframe(df)                                       
                    # print(df.printSchema())

                    new_file_path = file['file_path'].replace(self.__file_type, 'delta')        
                    self.__send_files_to_minio_as_delta(df, new_file_path)
                                
            except Exception as e:
                print("Erro ao converter o arquivo: ", file['file_name'])                
                traceback.print_exc()
            finally:
                self.__spark.stop()
        
        print("----- Processamento Finalizado! -----")