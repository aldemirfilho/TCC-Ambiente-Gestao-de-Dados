from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from minio import Minio
import tempfile
import os
from datetime import datetime
import shutil
from utils.SendMinio import SendMinio

class SparkExtract:
    __dataframe = None

    def __init__(self,ingestion_date = datetime.today().strftime("%Y%m%d")):
        """
        Inicializa as variáveis e componentes que serão usados
        """
        self.__temp_dir_list = []

        self.ingestion_date = ingestion_date
        
        self.__start_minion_client()
        self.__start_spark_session()
    
    def __start_minion_client(self):
        """
        Inicializa o cliente do MinIO
        """        
        self.__minio_client = Minio(
            os.getenv('MINIO_HOST'),
            access_key = os.getenv('MINIO_ACESS_KEY'),
            secret_key = os.getenv('MINIO_SECRET_KEY'),
            secure = False
        )

    def __start_spark_session(self):
        """
        Inicializa a sessão do Spark
        """
        self.__spark = SparkSession.builder \
            .appName("Standardized to Refine") \
            .config("spark.driver.memory", "16g") \
            .config("spark.executor.memory", "16g") \
            .getOrCreate()

    def __remove_temp_dirs(self):
        """
        Remove os diretórios temporários.
        """
        for temp_dir in self.__temp_dir_list:
            shutil.rmtree(temp_dir, ignore_errors=False, onerror=None)

    def __remove_bucket_files(self, bucket_path, minio_folder):
        sm = SendMinio(client=self.__minio_client, 
                       bucket_name=bucket_path, 
                       object_name=minio_folder, 
                       file_path='')
        found_files =  sm.findFilesInBucket(minio_folder)
        for file in found_files:
            print(file.object_name)
            client.remove_object(bucket_path, file.object_name)
            print(f"objeto {file.object_name} removido.")
            
    def appendToDataFrame(self, data, schema):
        if self.__dataframe == None:
            self.__dataframe = self.__spark.createDataFrame(data, schema=schema)
        else:
            linecount = self.__dataframe.count()
            # repartition no dataframe a cada 300000 itens
            if int(linecount/300000) == linecount/300000: # verifica se é um numero inteiro
                self.__dataframe.repartition(int(linecount/300000))
            # append new data to old dataframe
            self.__dataframe = self.__dataframe.union(self.__spark.createDataFrame(data, schema=schema))

    def show(self):
        print(f"Linhas no dataframe: {self.__dataframe.count()}")
        self.__dataframe.show(5)

    def sendToMinio(self, minio_bucket, minio_folder, minio_file_name):
        self.__remove_bucket_files(minio_bucket, minio_folder)
        
        self.__dataframe_local_path = tempfile.mkdtemp(prefix='json_standardized_')        
        self.__temp_dir_list.append(self.__dataframe_local_path)
        
        self.__dataframe.write.mode("overwrite").json(self.__dataframe_local_path)
        print(f'Numero de linhas no df: {self.__dataframe.count()}')
        self.__dataframe.printSchema()
        self.__dataframe.show(5)

        sm = SendMinio(client=self.__minio_client, 
                       bucket_name=minio_bucket, 
                       object_name=minio_file_name, 
                       file_path=self.__dataframe_local_path)
        
        sm.sendParquet2bucket()
        
        self.__remove_temp_dirs()
        
        self.__spark.stop()
        
        pass


