import os
import random
import time
from sqlalchemy import create_engine, text
from faker import Faker
from datetime import datetime

# Настройки подключения (DEMO_DB — БД схемы магазина)
DB_USER = os.getenv('POSTGRES_USER', 'user')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'pass')
DB_NAME = os.getenv('POSTGRES_DB', 'analytics_db')  # docker-compose передаёт DEMO_DB в POSTGRES_DB
DB_HOST = 'db'

engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')
fake = Faker('ru_RU')

def setup_static_data(conn):
    """Заполняет справочники один раз"""
    print("Заполнение справочной информации...")
    
    cities = ['Москва', 'Санкт-Петербург', 'Казань', 'Екатеринбург', 'Новосибирск']
    for c in cities:
        conn.execute(text("INSERT INTO cities (id) VALUES (:id) ON CONFLICT DO NOTHING"), {"id": c})
    
    categories = ['Электроника', 'Одежда', 'Дом', 'Спорт']
    for cat in categories:
        conn.execute(text("INSERT INTO category (id) VALUES (:id) ON CONFLICT DO NOTHING"), {"id": cat})
    
    brands = ['Samsung', 'Apple', 'Nike', 'IKEA', 'Xiaomi']
    for b in brands:
        conn.execute(text("INSERT INTO brand (id) VALUES (:id) ON CONFLICT DO NOTHING"), {"id": b})

    countries = ['Китай', 'Россия', 'Вьетнам', 'США']
    for cnt in countries:
        conn.execute(text("INSERT INTO manufacturer_country (id) VALUES (:id) ON CONFLICT DO NOTHING"), {"id": cnt})

    # Создаем ПВЗ и Клиентов, если их еще нет
    check_clients = conn.execute(text("SELECT COUNT(*) FROM clients")).scalar()
    if check_clients == 0:
        for _ in range(10):
            conn.execute(text("INSERT INTO \"PVZ\" (city, street) VALUES (:c, :s)"), 
                         {"c": random.choice(cities), "s": fake.street_name()})
        for _ in range(50):
            conn.execute(text("INSERT INTO clients (name, city) VALUES (:n, :c)"), 
                         {"n": fake.name(), "c": random.choice(cities)})
        for _ in range(30):
            cost = round(random.uniform(500, 50000), 2)
            conn.execute(text("""INSERT INTO article (category, name, cost, brand, manufacturing_country) 
                                 VALUES (:cat, :n, :cost, :b, :cnt)"""),
                         {"cat": random.choice(categories), "n": fake.catch_phrase(), 
                          "cost": cost, "b": random.choice(brands), "cnt": random.choice(countries)})
    conn.commit()

def add_random_order_and_review(conn):
    """Создает один новый заказ и иногда отзыв"""
    client_ids = [r[0] for r in conn.execute(text("SELECT id FROM clients")).fetchall()]
    pvz_ids = [r[0] for r in conn.execute(text("SELECT id FROM \"PVZ\"")).fetchall()]
    article_data = [r for r in conn.execute(text("SELECT id, cost FROM article")).fetchall()]

    # 1. Генерируем новый заказ
    c_id = random.choice(client_ids)
    p_id = random.choice(pvz_ids)
    now = datetime.now()
    
    # Генерируем сумму заказа (от 500 до 50000 рублей)
    order_amount = round(random.uniform(500, 50000), 2)
    
    res = conn.execute(text("""
        INSERT INTO orders (\"clientID\", \"PVZID\", \"date\", amount) 
        VALUES (:c, :p, :d, :a) RETURNING id
    """), {"c": c_id, "p": p_id, "d": now, "a": order_amount})
    
    order_id = res.fetchone()[0] 

    # Добавляем товары в заказ
    for _ in range(random.randint(1, 3)):
        art_id, art_price = random.choice(article_data)
        conn.execute(text("""INSERT INTO order_items ("orderID", "articleID", quantity, unit_price) 
                             VALUES (:o, :a, :q, :up)"""),
                     {"o": order_id, "a": art_id, "q": random.randint(1, 2), "up": art_price})
        
        # 2. С шансом 30% оставляем отзыв на один из купленных товаров
        if random.random() < 0.3:
            conn.execute(text("""INSERT INTO review ("clientID", "articleID", rating, review_text) 
                                 VALUES (:c, :a, :r, :t)"""),
                         {"c": c_id, "a": art_id, "r": random.randint(3, 5), "t": fake.sentence()})
    
    conn.commit()
    print(f"[{now.strftime('%H:%M:%S')}] Новый заказ №{order_id} и отзыв добавлены. Сумма: {order_amount} руб.")

if __name__ == "__main__":
    time.sleep(10)  # Ждём БД
    with engine.connect() as connection:
        # Шаг 1: Один раз заполняем справочники
        setup_static_data(connection)
        # Шаг 2: Бесконечный цикл генерации
        print("Запуск потоковой генерации заказов...")
        while True:
            try:
                add_random_order_and_review(connection)
                time.sleep(10)  # Пауза перед следующим заказом
            except Exception as e:
                print(f"Ошибка: {e}")
                time.sleep(5)