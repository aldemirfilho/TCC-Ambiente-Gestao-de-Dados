from minio import Minio
import os
from datetime import datetime

class Utils:
    """Classe com funções úteis para manipular dados ou o próprio MinIO"""
    
    def __init__(self):
        self.__start_minion_client()

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

    def find_files_in_bucket(self, bucket_name,  prefix, recursive = True):
        """Lista os arquivos presentes em um determinado caminho de um bucket"""
        result = []
        if self.__minio_client.bucket_exists(bucket_name):
            found_items =  self.__minio_client.list_objects(bucket_name,  recursive= recursive, prefix=prefix)
            for file in found_items:
                result.append(file.object_name)
        return result
    
    def get_recent_data_path(self, bucket_name: str, business_area:str, context: str, tablename: str):
        """Retorna o caminho para o arquivo de tabela ingerido mais recentemente."""
        inspect_path = f"{business_area}/{context}/{tablename}/"
        files = self.find_files_in_bucket(bucket_name, inspect_path)

        if not bucket_name=='refined':
            dates = []
            for file in files:
                part = file.split(inspect_path)[1]
                extension = part.split('.')[-1]
                date = part.split("/")[0]
                dates.append(date)
    
            dates = set(dates)
            date_times = []
            for date_str in dates:
                date_times.append(datetime.strptime(date_str, '%Y%m%d'))
    
            recent = max(date_times)
            recent_date = recent.strftime("%Y%m%d")

            return f"{business_area}/{context}/{tablename}/{recent_date}/{tablename}_{recent_date}.{extension}"
        else:
            return f"{business_area}/{context}/{tablename}.parquet"
        