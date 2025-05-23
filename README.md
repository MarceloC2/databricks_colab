# 🚀 Guia para Processamento de Dados com Apache Spark e *Azure* Blob Storage

Este guia prático ajudará você a configurar um **ambiente de processamento de dados** utilizando **Apache Spark** e **Azure Blob Storage**, mesmo que esteja começando agora!

---

📌 O que o código faz
Este código executa um fluxo completo de manipulação de dados, desde a leitura no *Azure* Blob Storage até a conversão e upload para o mesmo. Aqui está um resumo das etapas principais:

* ✅ Criação da SparkSession com configurações avançadas
* ✅ Configuração do *Azure* Blob Storage e acesso via account_key
* ✅ Leitura do arquivo JSON diretamente do *Azure* com Spark
* ✅ Tratamento de erros, baixando o arquivo localmente se necessário
* ✅ Exibição dos dados carregados e do schema do DataFrame
* ✅ Conversão dos dados para formato *Parquet*, garantindo armazenamento eficiente
* ✅ Upload dos arquivos *Parquet* para o *Azure* Blob Storage
* ✅ Listagem dos blobs para validar o envio dos arquivos


---
## 📌 1. Pré-requisitos

Antes de começar, certifique-se de que você tem:
- **Python instalado** (`3.x`)
- **Conta no *Azure*** e um **container no Blob Storage**
- **Jupyter Notebook ou Google Colab** (recomendado)
- **Bibliotecas necessárias** (`pyspark`, `azure-storage-blob`)

---

## 📥 2. Instalando as Dependências

Vamos instalar as bibliotecas necessárias para trabalhar com Spark e o armazenamento no *Azure*:

```python
!pip install pyspark azure-storage-blob
```
---
## 🔥 3. Criando a SparkSession
O SparkSession é necessário para usar Spark. Vamos configurá-lo

```Python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Processamento Azure") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-common:3.2.0,org.apache.hadoop:hadoop-azure:3.2.0,com.microsoft.azure:azure-storage:8.6.0") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
    .getOrCreate()
```
Explicação:
- local[*]: Usa todos os núcleos disponíveis para processamento local.
- config(...): Carrega pacotes necessários para acessar o *Azure* Blob Storage.

---

## 🔑 4. Configurando o Acesso ao *Azure* Blob Storage

Você precisa configurar as credenciais para acessar seu armazenamento

```Python
storage_account_name = "SEU_STORAGE_ACCOUNT"
container_name = "SEU_CONTAINER"
account_key = "SUA_CHAVE_DE_ACESSO"

# Configura o acesso ao Blob Storage
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", account_key)
```
⚠️ ATENÇÃO! Nunca compartilhe sua "account_key" publicamente

---

## 📂 5. Definindo o Caminho do Arquivo JSON no *Azure*

```Python
json_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/bronze/dados_brutos.json"
```

O prefixo "wasbs://" indica que estamos acessando o *Azure* Blob Storage

---

## 📖 6. Tentando Ler o Arquivo JSON no *Azure*

```Python
try:
    df = spark.read.json(json_path)
    print("Arquivo JSON lido diretamente do Azure Blob Storage!")
except Exception as e:
    print("Erro ao acessar o Azure. Tentando baixar localmente.")
```
- O código tenta ler JSON diretamente do *Azure* e, se falhar, faz o download localmente.

---

## 📥 7. Baixando o JSON Localmente (Se Necessário)

Caso o acesso ao *Azure* falhe, podemos baixar e ler o arquivo localmente:
```Python
from azure.storage.blob import BlobServiceClient

connect_str = "DefaultEndpointsProtocol=https;AccountName=SEU_STORAGE;AccountKey=SUA_CHAVE;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
blob_client = blob_service_client.get_blob_client(container=container_name, blob="bronze/dados_brutos.json")

local_json_path = "/content/dados_brutos.json"
with open(local_json_path, "wb") as f:
    f.write(blob_client.download_blob().readall())

df = spark.read.json(local_json_path)
print("Arquivo JSON lido a partir do arquivo local.")
```

---

## 📊 8. Visualizando os Dados

```Python
df.show(5, truncate=False)
df.printSchema()
```
Isso exibe as primeiras 5 linhas e o schema das colunas.

---

## 💾 9. Convertendo para *Parquet*

```Pyhton
local_parquet_path = "/content/imoveis_parquet"
df.write.mode("overwrite").parquet(local_parquet_path)
print(f"Dados gravados localmente em: {local_parquet_path}")
```
*Parquet* é um formato de armazenamento mais eficiente do que JSON.

---

## 🚀 10. Fazendo Upload dos Arquivos *Parquet* para o *Azure*

```Python
import os

destination_prefix = "bronze/imoveis_parquet/"

for root, dirs, files in os.walk(local_parquet_path):
    for file in files:
        local_file = os.path.join(root, file)
        relative_path = os.path.relpath(local_file, local_parquet_path)
        blob_path = os.path.join(destination_prefix, relative_path).replace("\\", "/")

        print(f"Fazendo upload de {local_file} para {blob_path} ...")
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        with open(local_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

print("Upload concluído!")
```
- O código primeiro salva os dados localmente como *Parquet*, depois os envia ao *Azure* Blob Storage.

---

## 📝 11. Listando os Arquivos no Blob Storage

```Python
for blob in blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=destination_prefix):
    print(blob.name)
```
Isso verifica se os arquivos foram enviados corretamente.❎

---

📝 12. Listando os Arquivos no *Azure* Blob Storage
Após o upload dos arquivos *Parquet*, é essencial validar se eles foram corretamente armazenados no *Azure* Blob Storage. Podemos fazer isso listando os blobs presentes na pasta de destino.

```Python
print("Lista dos blobs na pasta 'bronze/imoveis_parquet':")
for blob in container_client.list_blobs(name_starts_with=destination_prefix):
    print(blob.name)
```

🔎 Explicação do código:
- O código percorre os arquivos dentro do container no *Azure* Blob Storage e lista os blobs que começam com o prefixo `"bronze/imoveis_parquet/"`. Isso permite verificar se os arquivos foram corretamente enviados.
- Imprime os nomes dos arquivos armazenados no Blob Storage.
📌 Por que isso é importante?
- Garante que os arquivos foram corretamente enviados para o *Azure*.
- Permite verificar se todos os arquivos esperados estão disponíveis.
- Ajuda na depuração em caso de problemas no upload.
Se algum arquivo esperado não aparecer na listagem, revise os passos anteriores para confirmar que o upload foi bem-sucedido. 🚀

---

## 🎯 Conclusão

Neste guia, aprendemos a: 
✅ Criar um ambiente Spark 
✅ Configurar acesso ao *Azure* Blob Storage 
✅ Ler arquivos JSON 
✅ Salvar dados como *Parquet* 
✅ Enviar arquivos para o *Azure* Blob Storage
Agora você tem um fluxo de processamento funcional! 

---
---

# Comparando Métodos

O seu manual apresenta uma abordagem alternativa para o desafio de montagem do *Azure* Data Lake Gen2 no *Databricks*, substituindo o método de montagem (dbutils.fs.mount) por um fluxo de leitura/gravação via WASBS. Isso é útil para ambientes onde a montagem não é viável ou onde queremos mais controle sobre as operações de leitura/escrita.

## 📌 Principais diferenças entre os métodos
- Montagem (dbutils.fs.mount)
- Cria um atalho permanente para acessar os arquivos do ADLS Gen2 no *Databricks*.
- Permite reutilizar o caminho /mnt/... em vários notebooks.
- Necessita de credenciais para configurar o acesso.
- Acesso via WASBS (spark.read.json() e spark.write.parquet())
- Não exige montagem, pois lê/escreve diretamente no *Azure* Blob Storage.
- Usa o protocolo wasbs:// com Account Key, dispensando OAuth ou Managed Identity.
- Necessário especificar o caminho completo do arquivo em cada operação.

## 🔁 Alternativa para montar Storage no *Databricks*
Seu método alternativo usa Apache Spark e Blob Storage API para manipular os arquivos diretamente. Aqui está como ele se encaixa no desafio:

| Passo do desafio           | Método alternativo                      |
|----------------------------|-----------------------------------------|
| Montar o Storage           | Não monta, mas lê/escreve via wasbs://  |
| Verificar camadas          | Lista arquivos via container_client.list_blobs() |
| Validar existência do JSON | Usa spark.read.json(json_path)         |
| Ler o JSON com Spark       | Usa df_imoveis = spark.read.json(json_path) |
| Converter para *Parquet*     | Usa df_imoveis.write.parquet(...)      |
| Exibir dados               | Usa df_imoveis.show()                  |


## 📌 Vantagem do método alternativo: 
Ele pode funcionar sem a necessidade de montar o Storage no *Databricks*, permitindo maior flexibilidade.

---

**inbound**

_No Databricks, o caminho /mnt/dados/bronze/dados_brutos_imoveis.json faz referência a um armazenamento montado (mount point), permitindo acesso simplificado a um Azure Data Lake ou Blob Storage.
Como o Databricks tem integração nativa com o Azure, ele permite montar diretórios diretamente, tornando o acesso mais direto.
No Colab, não temos essa funcionalidade de montagem nativa, então usamos a leitura via WASBS ou o download local pelo azure-storage-blob.
Para que o código reflita a lógica do Databricks e faça referência ao conceito de inbound, podemos simplesmente definir inbound_path como o caminho no Blob Storage e mantê-lo ao longo do código._
