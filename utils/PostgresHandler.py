from pyspark.sql import SparkSession
import os
import sys
sys.path.append('/home/jovyan/work')

class PostgresHandler:
    """Classe para lidar com leitura de tabelas direto do DW"""

    def __init__(self):
    # Parâmetros de conexão com o PostgreSQL
        self.db_name = "dw2"
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

        # Configuração do Spark
        self.spark = SparkSession.builder \
            .appName(f"Database Consumption --") \
            .config("spark.driver.extraClassPath", "/home/jovyan/work/jars/postgresql-42.6.0.jar") \
            .config("spark.driver.memory", "20g") \
            .config("spark.executor.memory", "20g") \
            .getOrCreate()

    def read_table(self, tablename):
        """Retorna tabela do DW em forma de Spark DataFrame.

        Attributes:
            tablename (str): Nome da tabela.
        
        """
        df = self.spark.read \
                .format("jdbc") \
                .option("url", self.db_url) \
                .option("dbtable", tablename) \
                .option("user", self.db_user) \
                .option("password", self.db_pass) \
                .option("driver", "org.postgresql.Driver") \
                .load()
        return df
        
    def get_spark_session(self):
        """Retorna a sessão do Spark em uso"""
        return self.__spark

    def set_spark_session(self, spark_session):
        """Define uma nova sessão do Spark a ser utilizada"""
        self.__spark = spark_session