import polars as pl
import clickhouse_driver
import time
import numpy as np
import re
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



# Функция для очистки столбцов
def clean_columns(df, columns_to_clean):
    for column in columns_to_clean:
        df[column] = (
            df[column]
            .str.strip()  # Удаление пробелов по краям
            .str.lower()  # Приведение к нижнему регистру
            .str.replace(r'[\d\-]', '', regex=True)  # Удаление цифр и дефисов
            .str.replace(r'\s+', ' ', regex=True)  # Замена нескольких пробелов на один
        )
    return df

# Функция для парсинга имени
def parse_name(name):
    parts = name.strip().split(maxsplit=2)
    last_name = parts[0] if len(parts) > 0 else ''
    first_name = parts[1] if len(parts) > 1 else ''
    middle_name = parts[2] if len(parts) > 2 else ''
    return [last_name, first_name, middle_name]

# Функция для очистки и парсинга столбцов
def clean_and_parse_columns(df, columns_to_clean, column_to_parse=None):
    df = clean_columns(df, columns_to_clean)
    if column_to_parse is not None:
        df[['last_name', 'first_name', 'middle_name']] = pd.DataFrame(df[column_to_parse].apply(parse_name).tolist(), index=df.index)
    return df

def start():
    # Загружаем данные в Polars DataFrame
    df1 = load_data_as_polars('table_dataset1')
    df2 = load_data_as_polars('table_dataset2')
    df3 = load_data_as_polars('table_dataset3')

    # конвертируем Polars DataFrame в Pandas DataFrame
    df1 = df1.to_pandas()
    df2 = df2.to_pandas()
    df3 = df3.to_pandas()

    # Пименяем очистку и парсинг к df1, df2 и df3
    df1 = clean_and_parse_columns(df1, columns_to_clean=['full_name', 'email', 'address'], column_to_parse='full_name')
    df2 = clean_and_parse_columns(df2, columns_to_clean=['first_name', 'middle_name', 'last_name', 'address'])
    df3 = clean_and_parse_columns(df3, columns_to_clean=['name', 'email'], column_to_parse='name')

    df1 = pl.from_pandas(df1)
    df2 = pl.from_pandas(df2)
    df3 = pl.from_pandas(df3)

    # Пример дальнейшего использования df1, df2, df3
    print(df1)
    print(df2)
    print(df3)

if __name__ == "__main__":
    start()
