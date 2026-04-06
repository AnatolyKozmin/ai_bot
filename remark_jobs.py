"""
Скрипт для переметки постов в БД перед запуском автоматической отправки.
"""
import os
from config import load_settings
from database import make_session_factory
from models import Job
from sqlalchemy.orm import Session


def mark_all_unsent(session_factory) -> int:
    """Mark all posts as unsent (sent=False)."""
    with session_factory() as session:
        # Update all posts to sent=False
        updated = session.query(Job).update({Job.sent: False})
        session.commit()
        return updated


def get_stats(session_factory) -> dict:
    """Get database statistics."""
    with session_factory() as session:
        total = session.query(Job).count()
        sent = session.query(Job).filter(Job.sent == True).count()
        unsent = session.query(Job).filter(Job.sent == False).count()
        return {"total": total, "sent": sent, "unsent": unsent}


def main():
    settings = load_settings()
    session_factory = make_session_factory(settings.db_path)
    
    # Show current stats
    stats = get_stats(session_factory)
    print(f"📊 Статистика БД перед изменениями:")
    print(f"   Всего постов: {stats['total']}")
    print(f"   Отправлено: {stats['sent']}")
    print(f"   Не отправлено: {stats['unsent']}")
    
    if stats['total'] == 0:
        print("\n⚠️  В БД нет постов")
        return
    
    # Ask for confirmation
    print(f"\n⚠️  Это пометит все {stats['total']} постов как не отправленные")
    response = input("Продолжить? (y/n): ")
    
    if response.lower() == 'y':
        updated = mark_all_unsent(session_factory)
        print(f"\n✅ Переметили {updated} постов на отправку")
        
        # Show new stats
        stats = get_stats(session_factory)
        print(f"\n📊 Статистика БД после изменений:")
        print(f"   Всего постов: {stats['total']}")
        print(f"   Отправлено: {stats['sent']}")
        print(f"   Не отправлено: {stats['unsent']}")
        
        print(f"\n🚀 Теперь запусти бота и все эти посты начнут отправляться на LLM API!")
    else:
        print("Отменено")


if __name__ == "__main__":
    main()
