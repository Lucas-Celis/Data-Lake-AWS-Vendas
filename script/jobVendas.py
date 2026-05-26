import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Clientes
Clientes_node1779321304867 = glueContext.create_dynamic_frame.from_catalog(database="vendas", table_name="clientes_csv", transformation_ctx="Clientes_node1779321304867")

# Script generated for node Produtos
Produtos_node1779321372596 = glueContext.create_dynamic_frame.from_catalog(database="vendas", table_name="produtos_csv", transformation_ctx="Produtos_node1779321372596")

# Script generated for node itensvenda
itensvenda_node1779321139449 = glueContext.create_dynamic_frame.from_catalog(database="vendas", table_name="itensvenda_csv", transformation_ctx="itensvenda_node1779321139449")

# Script generated for node Vendas
Vendas_node1779320959164 = glueContext.create_dynamic_frame.from_catalog(database="vendas", table_name="vendas_csv", transformation_ctx="Vendas_node1779320959164")

# Script generated for node Vendedores
Vendedores_node1779321443989 = glueContext.create_dynamic_frame.from_catalog(database="vendas", table_name="vendedores_csv", transformation_ctx="Vendedores_node1779321443989")

# Script generated for node itensvendaMapping
itensvendaMapping_node1779321163916 = ApplyMapping.apply(frame=itensvenda_node1779321139449, mappings=[("idproduto", "long", "idproduto_itensvenda", "long"), ("idvenda", "long", "idvenda_itensvenda", "long"), ("quantidade", "long", "quantidade", "long"), ("valorunitario", "double", "valorunitario", "double"), ("valortotal", "double", "valortotal", "double"), ("desconto", "double", "desconto", "double")], transformation_ctx="itensvendaMapping_node1779321163916")

# Script generated for node VendasMapping
VendasMapping_node1779321061503 = ApplyMapping.apply(frame=Vendas_node1779320959164, mappings=[("idvenda", "long", "idvenda", "long"), ("idvendedor", "long", "idvendedor_vendas", "long"), ("idcliente", "long", "idcliente_vendas", "long"), ("data", "string", "data", "string"), ("total", "double", "total", "double")], transformation_ctx="VendasMapping_node1779321061503")

# Script generated for node Joinvendas_itenvendas
Joinvendas_itenvendas_node1779321236704 = Join.apply(frame1=VendasMapping_node1779321061503, frame2=itensvendaMapping_node1779321163916, keys1=["idvenda"], keys2=["idvenda_itensvenda"], transformation_ctx="Joinvendas_itenvendas_node1779321236704")

# Script generated for node JoinClientes
JoinClientes_node1779321327735 = Join.apply(frame1=Clientes_node1779321304867, frame2=Joinvendas_itenvendas_node1779321236704, keys1=["idcliente"], keys2=["idcliente_vendas"], transformation_ctx="JoinClientes_node1779321327735")

# Script generated for node JoinProdutos
JoinProdutos_node1779321391974 = Join.apply(frame1=Produtos_node1779321372596, frame2=JoinClientes_node1779321327735, keys1=["idproduto"], keys2=["idproduto_itensvenda"], transformation_ctx="JoinProdutos_node1779321391974")

# Script generated for node JoinVendedores
JoinVendedores_node1779321463064 = Join.apply(frame1=Vendedores_node1779321443989, frame2=JoinProdutos_node1779321391974, keys1=["idvendedor"], keys2=["idvendedor_vendas"], transformation_ctx="JoinVendedores_node1779321463064")

# Script generated for node ColunasFinais
ColunasFinais_node1779321558837 = ApplyMapping.apply(frame=JoinVendedores_node1779321463064, mappings=[("desconto", "double", "desconto", "double"), ("valorunitario", "double", "valorunitario", "double"), ("valortotal", "double", "valortotal", "double"), ("sexo", "string", "sexo", "string"), ("cliente", "string", "cliente", "string"), ("total", "double", "total", "double"), ("estado", "string", "estado", "string"), ("data", "string", "data", "string"), ("quantidade", "long", "quantidade", "long"), ("nome", "string", "nome", "string"), ("status", "string", "status", "string"), ("produto", "string", "produto", "string"), ("preco", "double", "preco", "double")], transformation_ctx="ColunasFinais_node1779321558837")

# Script generated for node Datalake
EvaluateDataQuality().process_rows(frame=ColunasFinais_node1779321558837, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779320685854", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Datalake_node1779321693349 = glueContext.write_dynamic_frame.from_options(frame=ColunasFinais_node1779321558837, connection_type="s3", format="glueparquet", connection_options={"path": "s3://datalakelucascelis/datalake/", "partitionKeys": ["status"]}, format_options={"compression": "snappy"}, transformation_ctx="Datalake_node1779321693349")

job.commit()