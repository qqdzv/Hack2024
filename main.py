import polars as pl
import clickhouse_driver
import time
from logger import logger
import pandas as pd

client = clickhouse_driver.Client(host='clickhouse', port='9000')

def load_data_as_polars(table_name):
    chunk_size = 50000
    offset = 0
    chunks = []  # Список для хранения DataFrame чанков

    start_time = time.perf_counter()
    logger.info(f"Starting to upload data from {table_name}")
                
    while True:
        # Запрос с ограничением на размер чанка
        query = f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"
        data, columns = client.execute(query, with_column_types=True)

        # Если данные пустые, выходим из цикла
        if not data:
            break

        # Преобразование текущего чанка в DataFrame
        df_chunk = pl.DataFrame({col[0]: [row[i] for row in data] for i, col in enumerate(columns)})

        # Добавление чанка в список
        chunks.append(df_chunk)
        
        offset += chunk_size
        if offset%1000000==0:
            logger.info(f'Обработан чанк: {offset}')

    # Объединение всех чанков в один DataFrame
    full_df = pl.concat(chunks)
    logger.info(f"Data upload complete in {round(time.perf_counter()-start_time,1)} seconds")
    logger.info(f"Loaded DataFrame with {full_df.shape[0]} rows and {full_df.shape[1]} columns")
    return full_df


def start():

    df1 = load_data_as_polars('table_dataset1')
    df2 = load_data_as_polars('table_dataset2')
    df3 = load_data_as_polars('table_dataset3')

if __name__ == "__main__":
    start()
