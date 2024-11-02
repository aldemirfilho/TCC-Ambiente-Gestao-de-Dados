"""Esquemas do Spark (tipos de colunas) esperados para tabelas do DW"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schemas = { 
    'GovCaged': StructType([
                StructField("saldo", IntegerType(), True),  # Integer with nullable
                StructField("raca_cor", StringType(), True),  # String with nullable
                StructField("tipo_empregador", StringType(), True),  # String with nullable
                StructField("contribuinte", StringType(), True),  # String with nullable
                StructField("fonte_novo_caged", StringType(), True),  # String with nullable
                StructField("tipo_movimentacao", StringType(), True),  # String with nullable
                StructField("grau_instrucao", StringType(), True),  # String with nullable
                StructField("cnae", StringType(), True),  # String with nullable
                StructField("industria", StringType(), True),  # String with nullable
                StructField("dt_carga", StringType(), True),  # Timestamp with nullable
                StructField("codigo_municipio", StringType(), True),  # String with nullable
                StructField("tipo_deficiencia", StringType(), True),  # String with nullable
                StructField("secao", StringType(), True),  # String with nullable
                StructField("cbo", StringType(), True),  # String with nullable
                StructField("demanda_formacao", StringType(), True),  # String with nullable
                StructField("nome_municipio", StringType(), True),  # String with nullable
                StructField("tempo_emprego_meses", IntegerType(), True),  # Integer with nullable
                StructField("divisao", StringType(), True),  # String with nullable
                StructField("grande_grupo", StringType(), True),  # String with nullable
                StructField("industrial_uniepro", StringType(), True),  # String with nullable
                StructField("tipo_trabalhador", StringType(), True),  # String with nullable
                StructField("salario_mensal", DoubleType(), True),  # Double with nullable
                StructField("grupo", StringType(), True),  # String with nullable
                StructField("subgrupo_principal", StringType(), True),  # String with nullable
                StructField("uf", StringType(), True),  # String with nullable
                StructField("aprendiz", StringType(), True),  # String with nullable
                StructField("horas_contratuais", DoubleType(), True),  # DoubleType with nullable
                StructField("classe", StringType(), True),  # String with nullable
                StructField("subgrupo", StringType(), True),  # String with nullable
                StructField("movimentacao", StringType(), True),  # String with nullable
                StructField("idade", IntegerType(), True),  # Integer with nullable
                StructField("porte", StringType(), True),  # String with nullable
                StructField("subclasse", StringType(), True),  # String with nullable
                StructField("familia", StringType(), True),  # String with nullable
                StructField("periodo", StringType(), True),  # Timestamp with nullable
                StructField("sexo", StringType(), True),  # String with nullable
                StructField("tipo_estabelecimento", StringType(), True),  # String with nullable
                StructField("ocupacao", StringType(), True),  # String with nullable
                StructField("origem", StringType(), True),  # String with nullable
            ]),
    'GovCagedDW': StructType([
                StructField("tipo_movimentacao", StringType(), True),
                StructField("raca_cor", StringType(), True),
                StructField("tipo_empregador", StringType(), True),
                StructField("contribuinte", StringType(), True),
                StructField("fonte_novo_caged", StringType(), True),
                StructField("codigo_municipio", StringType(), True),
                StructField("grau_instrucao", StringType(), True),
                StructField("cnae", StringType(), True),
                StructField("industria", StringType(), True),
                StructField("dt_carga", StringType(), True),
                StructField("nome_municipio", StringType(), True),
                StructField("tipo_deficiencia", StringType(), True),
                StructField("secao", StringType(), True),
                StructField("cbo", StringType(), True),
                StructField("demanda_formacao", StringType(), True),
                StructField("tipo_trabalhador", StringType(), True),
                StructField("tempo_emprego_meses", IntegerType(), True),
                StructField("divisao", StringType(), True),
                StructField("grande_grupo", StringType(), True),
                StructField("industrial_uniepro", StringType(), True),
                StructField("aprendiz", StringType(), True),
                StructField("salario_mensal", DoubleType(), True),
                StructField("grupo", StringType(), True),
                StructField("subgrupo_principal", StringType(), True),
                StructField("id_uf", IntegerType(), True),
                StructField("movimentacao", StringType(), True),
                StructField("idade", IntegerType(), True),
                StructField("horas_contratuais", IntegerType(), True),
                StructField("classe", StringType(), True),
                StructField("subgrupo", StringType(), True),
                StructField("dsc_uf", StringType(), True),
                StructField("periodo", StringType(), True),
                StructField("sexo", StringType(), True),
                StructField("porte", StringType(), True),
                StructField("subclasse", StringType(), True),
                StructField("familia", StringType(), True),
                StructField("saldo", IntegerType(), True),
                StructField("tipo_estabelecimento", StringType(), True),
                StructField("ocupacao", StringType(), True),
                StructField("origem", StringType(), True)
            ]),
 
}