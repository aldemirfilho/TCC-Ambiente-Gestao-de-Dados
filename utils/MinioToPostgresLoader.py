from pyspark.sql import SparkSession
from minio import Minio
import tempfile
import os
import sys
sys.path.append('/home/jovyan/work')

class MinioToPostgresLoader:
    """Classe dedicada a realizar carga de tabelas disponíveis no MinIO para o Data Warehouse"""

    def __init__(self,writing_mode, business_area, business_context, table_dw_name):
        """
        Attributes:
            writing_mode (str): Modo de escrita na tabela. 'overwrite' irá sobrescrever caso já exista uma tabela, 'append' irá adicionar
                        os registros ao final da tabela.
            business_area (str): Nome do Data Mart.
            business_context (Str): 'operacional' indica uma tabela comum do DW. 'producao' indica uma tabela de produção.
            table_dw_name (str): Nome da tabela no DW.
        
        """
    
        self.writing_mode = writing_mode
        self.business_area = business_area
        self.business_context = business_context
        self.table_dw_name = table_dw_name
        
        # Parâmetros de conexão com o MinIO
        self.minio_host = os.getenv('MINIO_HOST')
        self.minio_access_key = os.getenv('MINIO_ACESS_KEY')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY')

        # Parâmetros de conexão com o PostgreSQL
        self.db_name = "aplicacao"
        self.db_schema = "dw"
        self.db_port = 5432
        self.db_host = os.getenv("DB_PG_HOST")
        self.db_user = os.getenv("DB_PG_USER")
        self.db_pass = os.getenv("DB_PG_PASS")
        self.db_url = f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"
        self.db_properties = {
            "user": self.db_user,
            "password": self.db_pass,
            "driver": "org.postgresql.Driver"
        }

        # Configuração do MinIO
        self.client = Minio(
            self.minio_host,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False
        )

        # Configuração do Spark
        self.spark = SparkSession.builder \
            .appName(f"{self.business_area} - From Refined to Consume") \
            .config("spark.driver.extraClassPath", "/home/jovyan/work/jars/postgresql-42.6.0.jar") \
            .config("spark.driver.memory", "20g") \
            .config("spark.executor.memory", "20g") \
            .getOrCreate()

    def load_data(self):
        """Obtém os dados da camada REFINED no MinIO e envia para o DW."""
        
        # Path dos buckets
        bucket_refined = 'refined'
        bucket_subpath_refined = f"{self.business_area}/{self.business_context}"
        
        # Nome do arquivo
        file_name = self.table_dw_name   
    
        # Path do arquivo no bucket REFINED e pasta temp
        input_file = f"{bucket_subpath_refined}/{file_name}.parquet"
        temp_input_file_name = f"_refined_{self.business_area}_{file_name}.parquet"

        print("-- Iniciando processamento --")
        print(f"Área de Negócio: {self.business_area}")    
        print(f"Contexto: {self.business_context}")
    
        # Faz o download do arquivo do Minio para um diretório temporário
        # Carrega o arquivo via Spark
        # O arquivo é removido automaticamente após o término da execução
        with tempfile.TemporaryDirectory(suffix=temp_input_file_name) as temp_dir_parquet:
            print("Fazendo download do arquivo da camada REFINED...")
            objects_remote = self.client.list_objects(bucket_refined, prefix=input_file, recursive=True)
            for obj in objects_remote:
                local_file_path = os.path.join(temp_dir_parquet, obj.object_name)
                self.client.fget_object(bucket_refined, obj.object_name, local_file_path)
        
            parquet_folder = f"{temp_dir_parquet}/{input_file}"
            parquet_df = self.spark.read.parquet(parquet_folder)       
    
            # Salva no banco de dados
            # Recria a tabela caso já exista
            print("Inserindo no banco de dados...")
            # parquet_df.write \
            #     .option("truncate", True) \
            #     .jdbc(self.db_url, f"{self.db_schema}.{self.table_dw_name}", 
            #           mode=f"{self.writing_mode}", properties=self.db_properties)   
            parquet_df.write \
                .jdbc(self.db_url, f"{self.db_schema}.{self.table_dw_name}", 
                      mode=f"{self.writing_mode}", properties=self.db_properties)    
        
        print("-- Processamento finalizado --")   
        
        self.spark.stop()        