"""
Скрипт для миграции БД - добавить колонку 'sent' в таблицу jobs.
"""
import sqlite3
from config import load_settings


def migrate_db(db_path: str) -> bool:
    """Add 'sent' column to jobs table if it doesn't exist."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if column exists
        cursor.execute("PRAGMA table_info(jobs)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'sent' in columns:
            print("✓ Колонка 'sent' уже существует")
            conn.close()
            return True
        
        # Add the column
        print("Добавляю колонку 'sent' в таблицу jobs...")
        cursor.execute("ALTER TABLE jobs ADD COLUMN sent BOOLEAN DEFAULT 0 NOT NULL")
        conn.commit()
        
        print("✓ Колонка успешно добавлена")
        conn.close()
        return True
    except Exception as e:
        print(f"✗ Ошибка при миграции: {e}")
        return False


def main():
    settings = load_settings()
    
    print(f"🔧 Миграция БД: {settings.db_path}")
    if migrate_db(settings.db_path):
        print("✅ Миграция завершена!")
    else:
        print("❌ Миграция не удалась")


if __name__ == "__main__":
    main()
