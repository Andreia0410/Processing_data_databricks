# Processing_data_databricks
Processando dados com Databricks

Task

1. Converter arquivos csv em parquet e enviar para processing zone.

Dataset

2. Usaremos o dataset https://www.kaggle.com/nhs/general-practice-prescribing-data

Modos de leitura

1. permissive: Define todos os campos para NULL quando encontra registros corrompidos e coloca todos registros corrompidos em uma coluna chamada _corrupt_record. (default)

2. dropMalformed: Apaga uma linha corrompida ou que não consiga ler.

3. failFast: Falha imediatamente quando encontra uma linha que não consiga ler.

