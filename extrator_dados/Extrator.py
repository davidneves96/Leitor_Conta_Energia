import argparse
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, lit

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

    df_itens_fatura = extrator_servico.executar_extracao(args.caminho_pdf, 0, [20, 340, 140, 545], True, True)
    partition = Window.partitionBy(lit(None)).orderBy(lit(None))
    df_itens_fatura = df_itens_fatura.filter(col("Itens da Fatura") != "LANÇAMENTOS E SERVIÇOS").withColumn("id", row_number().over(partition))
    
    df_unidade = extrator_servico.executar_extracao(args.caminho_pdf, 0, [140, 340, 180, 545], True)
    
    df_fatura = extrator_servico.join_bases(df_itens_fatura, df_unidade)
    
    df_qtd = extrator_servico.executar_extracao(args.caminho_pdf, 0, [180, 340, 210, 545], True)
    
    df_fatura = extrator_servico.join_bases(df_fatura, df_qtd)

    df_preco_unit = extrator_servico.executar_extracao(args.caminho_pdf, 0, [210, 340, 250, 545], True, True, 1)
    
    df_fatura = extrator_servico.join_bases(df_fatura, df_preco_unit)
    
    df_valor = extrator_servico.executar_extracao(args.caminho_pdf, 0, [260, 340, 282, 545], True)
    
    df_fatura = extrator_servico.join_bases(df_fatura, df_valor)

    df_pis_confins = extrator_servico.executar_extracao(args.caminho_pdf, 0, [283, 340, 320, 545], True, True, 1)

    df_fatura = extrator_servico.join_bases(df_fatura, df_pis_confins) 

    df_base_calc_icms = extrator_servico.executar_extracao(args.caminho_pdf, 0, [320, 340, 350, 545], True, True, 1)

    df_fatura = extrator_servico.join_bases(df_fatura, df_base_calc_icms) 

    df_aliq_icsm = extrator_servico.executar_extracao(args.caminho_pdf, 0, [350, 340, 370, 545], True, True, 1)

    df_fatura = extrator_servico.join_bases(df_fatura, df_aliq_icsm) 

    df_icsm = extrator_servico.executar_extracao(args.caminho_pdf, 0, [380, 340, 405, 545], True, True, 1)

    df_fatura = extrator_servico.join_bases(df_fatura, df_icsm) 

    df_tarifa_unit = extrator_servico.executar_extracao(args.caminho_pdf, 0, [405, 340, 435, 545], True, True, 1)

    df_fatura = extrator_servico.join_bases(df_fatura, df_tarifa_unit) 

    df_data_leitura = extrator_servico.executar_extracao(args.caminho_pdf, 0, [410, 140, 460, 190], False, True, 1)

    data = df_data_leitura.select("dados").first()[0]
    df_fatura = (
        df_fatura
        .withColumn("Data Leitura", lit(data))
    )
    
    df_datas_total_pgto = extrator_servico.executar_extracao(args.caminho_pdf, 0, [50, 240, 330, 280], False, True, 1)

    mes_ref = df_datas_total_pgto.select("dados").filter(col("id") == 3).first()[0]
    dt_venc = df_datas_total_pgto.select("dados").filter(col("id") == 1).first()[0]
    total_pgto = df_datas_total_pgto.select("dados").filter(col("id") == 2).first()[0]

    df_fatura = (
        df_fatura
        .withColumn("Mes de Referencia", lit(mes_ref))
        .withColumn("Data de Vencimento da Fatura", lit(dt_venc))
        .withColumn("Total a Pagar", lit(total_pgto))
    )

    df_cd_cli = extrator_servico.executar_extracao(args.caminho_pdf, 0, [230, 180, 330, 230], False, True, 1)

    cd_cli = df_cd_cli.select("dados").filter(col("id") == 1).first()[0]
    cd_inst = df_cd_cli.select("dados").filter(col("id") == 2).first()[0]

    df_fatura = (
        df_fatura
        .withColumn("Codigo do Cliente", lit(cd_cli))
        .withColumn("Codigo da Instalacao", lit(cd_inst))
    ) 

    # Salvar no HDFS usando as configurações do ExtratorSetup
    extrator_servico.salvar_no_hdfs(df_fatura, extrator_setup.location_hdfs, "fatura")

    spark.stop()

if __name__ == "__main__":
    main()
