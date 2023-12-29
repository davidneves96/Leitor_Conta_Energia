import fitz

from pyspark.sql import DataFrame
from pyspark.sql.functions import row_number, lit
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.window import Window

class ExtratorServico:
    def __init__(self, spark_session):
        self.spark = spark_session

    def executar_extracao(
            self, caminho:str,
            pagina:int, coordenadas_x0_y0_x1_y1:list, 
            cabecalho:bool, ordenacao_horizontal:bool = True, 
            indice:int = 0
            ) -> DataFrame:

        # Inicialize a sessão do Spark
        # spark = SparkSession.builder.appName("PDFToSpark").getOrCreate()

        # Abra o PDF usando PyMuPDF
        pdf_document = fitz.open(caminho)

        # Lista para armazenar os dados extraídos de várias áreas
        all_extracted_data = []

        # Defina as coordenadas das áreas que você deseja extrair
        areas_to_extract = [coordenadas_x0_y0_x1_y1
            # Adicione mais áreas conforme necessário
        ]
        # Iterar por todas as páginas do PDF
        for page_number in range(len(pdf_document)):
            page = pdf_document[page_number]

            # Iterar por todas as áreas definidas para extração
            if page_number == pagina:
                for area in areas_to_extract:
                    x0, y0, x1, y1 = area

                    # Extrair o texto dentro das coordenadas marcadas
                    highlight_text = page.get_text("text", clip=fitz.Rect(x0, y0, x1, y1), sort= ordenacao_horizontal)

                    # Dividir o texto em linhas e, em seguida, cada linha em palavras
                    lines = highlight_text.strip().split('\n')

                    # Adicionar cada linha ao resultado
                    all_extracted_data.extend(lines)

        # Fechar o documento
        pdf_document.close()

        if cabecalho == True:
            # Criar um DataFrame Spark a partir de todos os dados extraídos
            if indice > 0:
                
                for i in range (indice):
                    lista = all_extracted_data[i] + " " + all_extracted_data[i+1]
                    header = lista.split(",")  # Suponha que as colunas sejam separadas por vírgula
                    
            else:
                header = all_extracted_data[0].split(",")  # Suponha que as colunas sejam separadas por vírgula
            
            schema = StructType([StructField(col_name.replace(".",""), StringType(), True) for col_name in header])
            # Remova a primeira linha (cabeçalho) dos seus dados, pois já a usamos para criar o schema
            if indice > 0:
                data_without_header = all_extracted_data[indice+1:]
            else:
                data_without_header = all_extracted_data[1:]

            # Crie o DataFrame com o schema
            df = self.spark.createDataFrame([(line,) for line in data_without_header], schema=schema)
        else:
            # Crie o DataFrame sem especificar um cabeçalho
            df = self.spark.createDataFrame([(line,) for line in all_extracted_data], ["dados"])
            
        partition = Window.partitionBy(lit(None)).orderBy(lit(None))
            
        df = df.withColumn("id", row_number().over(partition))
        
        return df

    def join_bases(spark, dataframe_um: DataFrame, 
                    dataframe_dois: DataFrame, 
                    chaves_join:list = "id", 
                    orientacao:str = "full") -> DataFrame:

        df_join = (
            dataframe_um
            .join(
                dataframe_dois, 
                chaves_join, 
                orientacao
            )
        )
    
        return df_join
    
    def salvar_no_hdfs(spark, df:DataFrame, modo:str, caminho_no_hdfs:str, nome_base:str) -> None:
        caminho_hdfs = caminho_no_hdfs + nome_base
        df.write.mode(modo).parquet(caminho_hdfs)

    # Adicione outras funções conforme necessário