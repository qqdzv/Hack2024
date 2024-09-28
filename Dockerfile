# Dockerfile
FROM python:3.10-slim

# Установим рабочую директорию
WORKDIR /app

# Скопируем файлы с зависимостями
COPY requirements.txt .

# Установим зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Скопируем остальной код
COPY . .

# Команда для запуска приложения
CMD ["python", "main.py"]
