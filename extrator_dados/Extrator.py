import argparse
from pyspark.sql import SparkSession

from ExtratorServico import ExtratorServico
from ExtratorSetup import ExtratorSetup

def main():

    parser = argparse.ArgumentParser(description='Script de Extração de Dados de PDF')
    parser.add_argument('--caminho_pdf', required=True, help='Caminho para o arquivo PDF a ser processado')
    # Adicione mais argumentos conforme necessário
    args = parser.parse_args()

    # Configuração da sessão Spark
    spark = SparkSession.builder.appName("ExtratorPDF").getOrCreate()

    # Configuração e inicialização do ExtratorServico
    extrator_servico = ExtratorServico(spark)

    # Configuração do ExtratorSetup
    extrator_setup = ExtratorSetup()

    

    # Execução da extração
    dataframe_extracao = extrator_servico.executar_extracao(caminho_pdf)

    # Salvar no HDFS usando as configurações do ExtratorSetup
    extrator_servico.salvar_no_hdfs(dataframe_extracao, extrator_setup.location_hdfs, extrator_setup.nome_tabela_hive)

    spark.stop()

if __name__ == "__main__":
    main()
