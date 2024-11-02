from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from minio import Minio
import tempfile
import os
from datetime import datetime
import shutil
from utils.SendMinio import SendMinio
from pyspark.sql.functions import current_timestamp, to_timestamp
from pyspark.sql.functions import col, lit
from delta import *


class SparkRefine:
    """
    Classe para transformar tabelas na camada de refino utilizando Spark. Contém funcionalidades frequentemente usadas
    como mapear valores de uma coluna, renomear colunas, substituir valores nulos por 'NÃO INFORMADO', etc. Além disso, 
    permite a criação de views no ambiente Spark de maneira simplificada e envia tabelas transformadas para a camada de
    refino no MinIO

    Attributes:
        ingestion_date (str): Data de ingestão no formato YYYYMMDD
        detailed_log (bool): Caso falso, não imprime informações do schema e primeiras 5 linhas do DataFrame

    Methods:
        create_spark_temp_view(): Cria a view do Spark.
        generate_parquet_dataframe_from_sql(): Gera o dataframe parquet da view e salva local.
        send_dataframe_to_minio(): Envia o dataframe para o Minio.
        transform_dataframe(): Realiza transformações no dataframe.
    """

    def __init__(self, 
                 ingestion_date = datetime.today().strftime("%Y%m%d"),
                 detailed_log = True
                ):
        """
        Inicializa as variáveis e componentes que serão usados
        """
        self.__temp_dir_list = []
        self.__detailed_log = detailed_log
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
    
    #configs adicionadas para o funcionamento do delta lake das linhas 68 até 70
    #as configs abaixo retornam erro
    '''
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", \
    "io.delta:delta-spark_2.12:3.0.0,io.delta:delta-core_2.12:1.2.1,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.571") \
    '''
    
    def __start_spark_session(self):
        """
        Inicializa a sessão do Spark
        """
        self.__spark = SparkSession.builder \
            .appName("Standardized to Refine") \
            .config("spark.driver.extraClassPath", "/home/jovyan/work/jars/postgresql-42.6.0.jar")\
            .config("spark.driver.memory", "22g") \
            .config("spark.executor.memory", "22g") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", True).getOrCreate()
        #self.__spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def __remove_temp_dirs(self):
        """
        Remove os diretórios temporários.
        """
        for temp_dir in self.__temp_dir_list:
            shutil.rmtree(temp_dir, ignore_errors=False, onerror=None)

    def create_spark_temp_view(self, bucket_name, file_path, view_name):
        """
        Cria uma view temporária no Spark com base em um arquivo Parquet do Minio.
        
        Args:
            bucket_name (str): Nome do bucket (raw, standardized, refined).
            file_path (str): Caminho do arquivo Parquet dentro do bucket.
            view_name (str): Nome da view que será gerada dentro do Spark.
    
        Exemplo:
            >>> create_spark_temp_view('refined', 'sei_usuarios/operacional/dim_sei_usuarios.parquet', 'dim_sei_usuarios')
            
        """
        print(f"Arquivo: {file_path}, bucket: {bucket_name}")
                
        temp_dir_parquet = tempfile.mkdtemp(prefix=f'tmp_{bucket_name}_')
        self.__temp_dir_list.append(temp_dir_parquet)
        
        print(f"Fazendo download do arquivo da camada {bucket_name}...")
        objects_remote = self.__minio_client.list_objects(bucket_name, prefix=file_path, recursive=True)
        for obj in objects_remote:
            local_file_path = os.path.join(temp_dir_parquet, obj.object_name)
            self.__minio_client.fget_object(bucket_name, obj.object_name, local_file_path)
    
        print("Carregando o arquivo para o Spark...")
        #df = self.__spark.read.option("inferSchema",True).parquet(f"{temp_dir_parquet}/{file_path}") 
        df = self.__spark.read.parquet(f"{temp_dir_parquet}/{file_path}")
        
        
        print(f"Criando view {view_name} no Spark...")
        self.__dataframe = df
        #self.__dataframe.createOrReplaceTempView(view_name)
        self.create_temp_view_from_dataframe(view_name)
        if self.__detailed_log:
            print(f"Numero de linhas no df: {self.__dataframe.count()}")
            self.__dataframe.printSchema()
            df.show(5)

    def create_temp_view_from_dataframe(self, view_name: str):
        """Dado o DataFrame que está sendo transformado pela classe, cria uma view do Spark. 
        
        Attributes:
            view_name (str): Nome da view no ambiente Spark
        """

        self.__dataframe.createOrReplaceTempView(view_name)
        print('View criada!')
    
    def create_spark_temp_view_with_transform(self, bucket_name, file_path, view_name,rename_columns = {}, cast_columns = {}, drop_columns = [],transform_string_columns = [], map_column_values = {}, create_sk = ''):
        """
        Cria uma view temporária no Spark com base em um arquivo Parquet do Minio, permitindo realizar operações antes de criar a view.
        As operações disponíveis são renamear coluna, alterar tipo de coluna, remover coluna, transformar valores de colunas string, 
        mapear valores de colunas, criar SK.
        
        Args:
            bucket_name (str): Nome do bucket (raw, standardized, refined).
            file_path (str): Caminho do arquivo Parquet dentro do bucket.
            view_name (str): Nome da view que será gerada dentro do Spark.
            rename_columns (dict): Dicionario mapeando as colunas com seus novos nomes.
            cast_columns (dict): Dicionario mapeando colunas com seus novos types.
            drop_columns (list): Lista de colunas a serem removidas do df.
            transform_string_columns (list): Lista de colunas para realizar o tratamento de strip, uppercase e substituição de nulos por 'NÃO INFORMADO'.
            map_column_values (dict(dict)): Lista de colunas e seus mapeamentos de dados.
            
        Exemplo:
            >>> create_spark_temp_view(
                    bucket_name='refined', 
                    file_path='sei_usuarios/operacional/dim_sei_usuarios.parquet',
                    view_name='dim_sei_usuarios',
                    rename_columns={ 'cd_usuarios':'nk_user'},
                    cast_columns={'dsc_setor': 'string'},
                    drop_columns=['dsc_cpf'], 
                    transform_string_columns: [dsc_nome],
                    map_column_values: {
                        'nk_user':{
                            3: 1,
                        }
                    }
                )
            
        """
        print(f"Arquivo: {file_path}")
                
        temp_dir_parquet = tempfile.mkdtemp(prefix=f'{bucket_name}_')
        self.__temp_dir_list.append(temp_dir_parquet)
        
        print(f"Fazendo download do arquivo da camada {bucket_name}...")
        objects_remote = self.__minio_client.list_objects(bucket_name, prefix=file_path, recursive=True)
        for obj in objects_remote:
            local_file_path = os.path.join(temp_dir_parquet, obj.object_name)
            self.__minio_client.fget_object(bucket_name, obj.object_name, local_file_path)
    
        print("Carregando o arquivo para o Spark...")
        #df = self.__spark.read.option("inferSchema",True).parquet(f"{temp_dir_parquet}/{file_path}")
        df = self.__spark.read.parquet(f"{temp_dir_parquet}/{file_path}")
        
        self.__dataframe = df
        
        print("Executando transformações...")

        self.transform_dataframe(rename_columns = rename_columns, cast_columns = cast_columns,
                            drop_columns = drop_columns, transform_string_columns = transform_string_columns,
                            map_column_values = map_column_values, create_sk = create_sk)
        
        print(f"Criando view {view_name} no Spark...")
        self.__dataframe.createOrReplaceTempView(view_name)
        print("View criada")
        if self.__detailed_log:
            print(f"Numero de linhas no df: {self.__dataframe.count()}")
            self.__dataframe.printSchema()
            self.__dataframe.show(5)

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

    # Criando a var format, para usar o formato 'parquet' ou 'delta'
    def generate_parquet_dataframe_from_sql(self, sql, format="parquet", mode="overwrite"):
        """
        Gera um DataFrame salvo na variável `self.__dataframe` a partir de uma consulta SQL nas views do ambiente Spark.

        Args:
            sql (str): SQL que irá criar o dataframe.
            format (str): Formato de arquivo que será utilizado. Por padrão é "parquet".
            mode (str): Modo de escrita do dataframe. Por padrão é "overwrite".            
        """
        self.__dataframe = self.__spark.sql(sql)

        if self.__detailed_log:
            print(f'Numero de linhas no df: {self.__dataframe.count()}')
            self.__dataframe.printSchema()
            self.__dataframe.show(5)

    #meio confuso em como fazer aqui, precisaria um send_delta_to_minio? 
    def send_dataframe_to_minio(self, remote_path, add_metadata = True):     
        """
        Envia o dataframe da variável `self.__dataframe` para o MinIO em formato `.parquet`.

        Args:
            remote_path (str): Caminho remoto do arquivo parquet.
        """
        if add_metadata:
            print("Adicionando metadados")
            self.__dataframe = self.__add_metadata_to_dataframe(self.__dataframe)

        print("Salvando localmente em pasta temporária")
        self.__dataframe_local_path = tempfile.mkdtemp(prefix='tmp_refined_')        
        self.__temp_dir_list.append(self.__dataframe_local_path)
        self.__dataframe.write.format('parquet').mode('overwrite').save(self.__dataframe_local_path)

        print("Enviando para o MinIO")
        sm = SendMinio(client=self.__minio_client, 
                       bucket_name='refined', 
                       object_name=remote_path, 
                       file_path=self.__dataframe_local_path)
        
        sm.removeFiles(remote_path)
        sm.sendParquet2bucket()
        
        self.__remove_temp_dirs()
        
        self.__spark.stop()
        

    def transform_dataframe(self,  rename_columns = {}, cast_columns = {}, drop_columns = [], transform_string_columns = [], map_column_values = {}, create_sk=''):
        """
        Transforma o dataframe gerado.
        
        Args:
            rename_columns (dict): Dicionario mapeando as colunas com seus novos nomes.
            cast_columns (dict): Dicionario mapeando colunas com seus novos types.
            drop_columns (list): Lista de colunas a serem removidas do df.
            transform_string_columns (list): Lista de colunas para realizar o tratamento de strip, uppercase e substituição de nulos por 'NÃO INFORMADO'.
            map_column_values (dict(dict)): Lista de colunas e seus mapeamentos de dados.
            
        Exemplo:
            >>> transform_dataframe(
                    rename_columns={ 'cd_setor':'nk_setor'},
                    cast_columns={'dsc_orgao': 'string'},
                    drop_columns=['dsc_cpf'], 
                    transform_string_columns: [dsc_nome],
                    map_column_values: {
                        'nk_setor':{
                            3: 1,
                        }
                    }
                )
        """
        
        df = self.__dataframe

        # rename columns
        for old_colunmn, new_column in rename_columns.items():
            df = df.withColumnRenamed(old_colunmn, new_column)


            
        # cast values
        for column, type in cast_columns.items():
            df = df.withColumn(column, df[column].cast(type))


        # drop columns
        for column in drop_columns:
            df = df.drop(column)


        # Map column values
        for column, mapped_values in map_column_values.items():
            for old_val, new_val in mapped_values.items():
                df = df.withColumn(column, when(df[column] == old_val, new_val).otherwise(df[column]))

                
        # transform_string_columns
        for column in transform_string_columns:
            df = df.withColumn(column, trim(col(column)))
            df = df.withColumn(column, upper(col(column)) )
            df = df.withColumn(column, when(col(column) == "", "NÃO INFORMADO").otherwise(col(column)))
            df = df.na.fill(value='NÃO INFORMADO',subset=[column])


        # create sk over column
        if create_sk != '':
            windowSpec = Window.orderBy(lit(1))
            df = df.withColumn(create_sk, row_number().over(windowSpec))        

        self.__dataframe = df

    def check_columns_with_null(self):
        """
        Exibe as colunas que possuem valores nulos ou vazios
        """
        columns_with_null = []
        for column in self.__dataframe.columns:
            count_null = self.__dataframe.filter(col(column).isNull() | col(column).contains('None') | col(column).contains('NULL') | (col(column) == '')).count()
            if count_null > 0:
                columns_with_null.append(column)
                print(f"A coluna '{column}' tem {count_null} valores nulos.")

        if not columns_with_null:
            print("Não foi encontrado valores nulos nas colunas.")

    def get_dataframe(self):
        return self.__dataframe

    def set_dataframe(self, dataframe):
        self.__dataframe = dataframe

    def get_spark_session(self):
        return self.__spark

    def set_spark_session(self, spark_session):
        self.__spark = spark_session
        
        
        


