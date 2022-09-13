# Twitter Lake House

Projeto realizado para aplicação de arquitetura definida para retorno de dados do Twitter.

## Índice

1. Objetivo 
2. Arquitetura
3. Steps
  3.1 Coleta dos dados
  3.2 Ingestão de dados para a camada Bronze
  3.3 Tratamento de dados para a camada Silver
  3.4 Disponibilização de dados na camada Gold
  
  
## 1. Objetivo

Criação de um datalake com dados extraídos do Twitter.

Os dados serão armazenados para possibilitar a geração de insights:
- Análise de sentimentos
- Clusterização de curtidas
- Regionalização de acessos

## 2. Arquitetura

Os componentes utilizados na arquitetura para o projeto foram:

- Airflow para orquestração do pipeline

- S3 Bucket para armazenamento dos dados nas diversas camadas:
  - RAW: disponibilização do dado bruto
  - Bronze: dado com um tratamento inicial e pronto para ser consumido
  - Silver: dado com melhor tratamento que possibilita um consumo mais facilitado
  - Gold: dado disponibilizado em formatos pré-definidos para possibilitar o consumo por DataViz.
  
- Databricks para efetuar o processamento distribuído, através de Apache Spark e Delta Lake para processar e armazenar os dados. 

## 3. Steps

### 3.1 Coleta de dados

Inicialmente, é feita a coleta de dados pela API disponibilizada pelo Twitter. Utilizando Python e a biblioteca requests, realizamos requisições ao Twitter e salvamos os dados no formato JSON, no datalake com o Apache Spark em Delta.

Os dados brutos foram salvos inicialmente em formato .json, pois possuem uma estrutura complexa para serem salvos em .parquet. Os scripts criados para esta etapa estão disponíveis em .


### 3.2 Ingestão de dados para a camada Bronze

Com os dados coletados e armazenados na camada raw, podemos efetuar transformações para organizar melhor os dados no datalake e dar início às consultas. O primeiro passo é definir o conjunto de informações a serem ingeridos na camada Bronze, isto é, a partir dos dados brutos em raw, definimos um schema das informações que precisamos converter e disponibilizar na Bronze, para então realizar a ingestão e persistir os dados em formato .parquet. 

Durante essa fase, em um ambiente profissional, é feita também a classificação de dados sensíveis que deverão ser mascarados ou mesmo omitidos nessa camada.

Os scripts desse step estão em


### 3.3 Tratamento de dados para a camada Silver

Agora que temos na camada bronze uma forma mais fácil e otimizada para consumir os dados, podemos criar na camada Silver, novas visualizações de dados. Visões mais analíticas e sumarizadas que ajudarão na criação de análises ou mesmo na criação de modelos preditivos.

Utilizamos um script de template para realizar as ingestões em Delta a partir de queries SQL. 

Esse script encontra-se em 


### 3.4 Disponibilização de dados na camada Gold

Mesmo com dados em visões mais analíticas e sumarizadas, ainda existe necessidade de disponibilização de dados em formato multidimensional para facilitar o consumo por algumas ferramentas de DataViz.

Na camada Gold, são feitas disponibilizações sob demanda para determinadas áreas e requisições. Para tal, é feita uma modelagem de dados de acordo com a necessidade e os dados são ingeridos a partir da Silver.

O script desse step encontra-se em 

