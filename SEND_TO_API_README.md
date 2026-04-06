# Скрипт для отправки постов на LLM API

## Использование

### 1. Локальный запуск

```bash
# Активировать виртуальное окружение (если нужно)
source venv/bin/activate

# Запустить скрипт
LLM_API_URL=http://localhost:8000/api/jobs python send_to_llm_api.py
```

### 2. С Docker

```bash
docker-compose exec bot python send_to_llm_api.py
```

Или если нужно передать API URL:

```bash
docker-compose exec bot bash -c "LLM_API_URL=http://your-api:8000/api/jobs python send_to_llm_api.py"
```

## Переменные окружения

| Переменная | Значение по умолчанию | Описание |
|-----------|-----------|---------|
| `LLM_API_URL` | - | URL API для отправки постов (обязательно) |
| `BATCH_SIZE` | `10` | Количество постов отправляемых за раз |
| `DB_PATH` | `jobs.db` | Путь к базе данных (из config.py) |

## Пример запуска

```bash
# Отправить посты на локальный API
LLM_API_URL=http://localhost:8000/api/jobs BATCH_SIZE=20 python send_to_llm_api.py

# Результат:
# 🚀 Запуск отправки постов на http://localhost:8000/api/jobs
# 📦 Размер батча: 20
# 📨 Найдено 150 постов
# 📤 Отправка батча 1/8
# ✓ Отправлен пост #150 (чат: ВТБ карьера)
# ✓ Отправлен пост #149 (чат: Альфа Будущее)
# ...
# ✅ Статистика:
#    Успешно: 150
#    Ошибок: 0
#    Всего: 150
```

## Структура отправляемого JSON

```json
{
  "id": 1,
  "chat_id": -1001234567890,
  "chat_title": "ВТБ карьера",
  "message_id": 12345,
  "sender_id": 987654321,
  "date_utc": "2026-04-07T10:30:00+00:00",
  "text": "Ищем разработчика...",
  "url": "https://t.me/vtb_career/12345",
  "inserted_at_utc": "2026-04-07T10:35:00+00:00"
}
```

## Troubleshooting

**Ошибка: "Укажите LLM_API_URL"**
```bash
export LLM_API_URL=http://your-api-url:port/endpoint
python send_to_llm_api.py
```

**Нет постов для отправки**
- Убедитесь, что бот собрал посты (проверьте `jobs.db`)
- Запустите: `python -c "from database import make_session_factory; from models import Job; sf = make_session_factory('jobs.db'); s = sf(); print(f'Posts: {s.query(Job).count()}')"`

**Таймауты при отправке**
- Увеличьте `BATCH_SIZE` или проверьте скорость вашего API
