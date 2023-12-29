import fitz

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.types import StringType, IntegerType, DecimalType, StructType, StructField
from pyspark.sql.window import Window

class ExtratorServico:
    def __init__(self, spark_session):
        self.spark = spark_session

    def marcar_pdf (caminho_entrada:str, caminho_saida:str, coordenadas_x0_y0_x1_y1:list, page_number:int = 1):
        # Abra o PDF usando PyMuPDF
        pdf_document = fitz.open(caminho_entrada)

        # Selecione a página que você deseja visualizar
        page = pdf_document[page_number - 1]

        # Defina as coordenadas do retângulo que você deseja visualizar
        x0, y0, x1, y1 = coordenadas_x0_y0_x1_y1

        # Desenhe um retângulo na página para visualizar as coordenadas
        rect = fitz.Rect(x0, y0, x1, y1)
        highlight = page.add_highlight_annot(rect)

        # Salve o PDF modificado com a marcação das coordenadas
        pdf_document.save(caminho_saida, garbage=4, deflate=True, clean=True)

        # Feche o documento
        pdf_document.close()

    def executar_extracao (
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

    # Adicione outras funções conforme necessário