from flask import Flask, request, redirect, jsonify
from flask_restx import Api, Resource, fields
import redis
import sqlite3
import hashlib
from datetime import datetime, timedelta
import logging
from contextlib import contextmanager

app = Flask(__name__)
api = Api(app, version='1.0', title='URL Shortener API',
          description='API для генерации коротких ссылок с поддержкой лимитов пользователей')

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
MAX_CONCURRENT_REQUESTS = 100
REQUEST_TTL = 60
MAX_CONCURRENT_REQUESTS_FOR_ONE_USER = 100

logging.basicConfig(filename='app_errors.log', level=logging.ERROR)

def log_error(e):
    logging.error(f"{datetime.now()}: {str(e)}")

# Подключение к базе данных (SQLite)
@contextmanager
def get_db_connection():
    conn = sqlite3.connect('urls.db')
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

# Создание таблицы для хранения URL'ов
def create_table():
    with get_db_connection() as conn:
        conn.execute('''
        CREATE TABLE IF NOT EXISTS url_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_url TEXT NOT NULL,
            short_url TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        ''')
        conn.commit()

url_model = api.model('URL', {
    'full_url': fields.String(required=True, description='Полный URL'),
    'user_id': fields.String(required=True, description='ID пользователя')
})

# Хэш-функция для генерации короткой ссылки
def generate_short_url(full_url):
    return hashlib.md5(full_url.encode()).hexdigest()[:6]

# Проверка TTL короткой ссылки
def is_link_expired(created_at):
    created_at_dt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S')
    return datetime.now() > created_at_dt + timedelta(minutes=10)

# Проверяем количество запросов от пользователя
def check_rate_limit(user_id):
    user_key = f"user_requests:{user_id}"
    total_key = "total_requests"

    pipeline = redis_client.pipeline()
    pipeline.incr(user_key)
    pipeline.incr(total_key)
    pipeline.expire(user_key, REQUEST_TTL)
    pipeline.expire(total_key, REQUEST_TTL)

    results = pipeline.execute()

    current_user_requests = results[0]
    current_total_requests = results[1]

    if current_total_requests > MAX_CONCURRENT_REQUESTS or current_user_requests > MAX_CONCURRENT_REQUESTS_FOR_ONE_USER:
        return False

    return True

# Endpoint для генерации короткой ссылки
@api.route('/generate')
class GenerateURL(Resource):
    @api.expect(url_model)
    @api.response(200, 'Короткая ссылка получена из бд')
    @api.response(201, 'Короткая ссылка успешно создана')
    @api.response(400, 'Ошибка в запросе')
    @api.response(429, 'Превышен лимит запросов')
    @api.response(500, 'Внутренняя ошибка сервера')
    def post(self):
        user_id = None
        counters_increased = False
        try:
            full_url = request.json.get('full_url')
            user_id = request.json.get('user_id')

            if not user_id:
                return {"error": "user_id is required"}, 400

            if not full_url:
                return {"error": "URL is required"}, 400

            is_allowed = check_rate_limit(user_id)
            counters_increased = True

            if not is_allowed:
                return {"error": "Too many concurrent requests. Please try again later."}, 429

            with get_db_connection() as conn:

                cur = conn.cursor()

                cur.execute('SELECT short_url, created_at FROM url_map WHERE full_url = ?', (full_url,))
                row = cur.fetchone()

                if row and not is_link_expired(row['created_at']):
                    return {"short_url": row['short_url']}, 200

                short_url = generate_short_url(full_url)
                created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                if row:
                    cur.execute('UPDATE url_map SET short_url = ?, created_at = ? WHERE full_url = ?',
                                (short_url, created_at, full_url))
                else:
                    cur.execute('INSERT INTO url_map (full_url, short_url, created_at) VALUES (?, ?, ?)',
                            (full_url, short_url, created_at))
                conn.commit()

            return {"short_url": short_url}, 201

        except Exception as e:
            log_error(e)
            return {"error": "An error occurred"}, 500

        finally:
            if counters_increased:
                if user_id is not None:
                    current_user_requests = redis_client.get(f"user_requests:{user_id}")
                    if current_user_requests and int(current_user_requests) > 0:
                        redis_client.decr(f"user_requests:{user_id}")

                current_total_requests = redis_client.get("total_requests")
                if current_total_requests and int(current_total_requests) > 0:
                    redis_client.decr("total_requests")

# Endpoint для получения полной ссылки по короткой
@api.route('/get/<string:short_url>')
class GetFullURL(Resource):
    @api.response(200, 'Полная ссылка успешно найдена')
    @api.response(404, 'Полная ссылка не найдена')
    @api.response(410, 'Короткая ссылка истекла')
    @api.response(500, 'Внутренняя ошибка сервера')
    def get(self, short_url):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()

                cur.execute('SELECT full_url, created_at FROM url_map WHERE short_url = ?', (short_url,))
                row = cur.fetchone()

                if not row:
                    return {"error": "Full URL not found"}, 404

                if is_link_expired(row['created_at']):
                    return {"error": "This link has expired"}, 410

                return {"full_url": row['full_url']}, 200

        except Exception as e:
            log_error(e)
            return {"error": "An error occurred"}, 500

# Endpoint для перенаправления по короткой ссылке
@api.route('/<string:short_url>')
class RedirectURL(Resource):
    @api.response(302, 'Перенаправление успешно')
    @api.response(404, 'Полная ссылка не найдена')
    @api.response(410, 'Короткая ссылка истекла')
    @api.response(500, 'Внутренняя ошибка сервера')
    def get(self, short_url):
        try:
            with get_db_connection() as conn:
                cur = conn.cursor()

                cur.execute('SELECT full_url, created_at FROM url_map WHERE short_url = ?', (short_url,))
                row = cur.fetchone()

                if not row:
                    return {"error": "Full URL not found"}, 404

                if is_link_expired(row['created_at']):
                    return {"error": "This link has expired"}, 410

                return redirect(row['full_url'], code=302)

        except Exception as e:
            log_error(e)
            return {"error": "An error occurred"}, 500

if __name__ == '__main__':
    create_table()
    app.run(debug=True)
