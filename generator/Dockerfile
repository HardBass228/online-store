# 1. Берем официальный легкий образ Python
FROM python:3.10-slim

# 2. Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# 3. Копируем файл со списком библиотек в контейнер
COPY requirements.txt .

# 4. Устанавливаем эти библиотеки (Faker, SQLAlchemy и т.д.)
RUN pip install --no-cache-dir -r requirements.txt

# 5. Копируем сам код генератора (main.py) в контейнер
COPY main.py .

# 6. Команда, которая запустится при старте контейнера
CMD ["python", "main.py"]