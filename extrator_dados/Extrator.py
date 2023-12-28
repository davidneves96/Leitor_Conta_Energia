from ExtratorServico import ExtratorServico
from ExtratorSetup import ExtratorSetup

def main():
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

if __name__ == "__main__":
    main()
