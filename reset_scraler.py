from pymongo import MongoClient
import sys

def reset_database(db_name="lab_corpus"):
    client = MongoClient("mongodb://localhost:27017")
    db = client[db_name]
    
    print("=" * 60)
    print("Чистка бд")
    print("=" * 60)
    
    confirmation = input("ARE U SHURE??????. (yeeeeah/no): ")
    if confirmation.lower() != 'yeeeeah':
        print("Отменено.")
        return
    
    collections = db.list_collection_names()
    
    for collection in collections:
        if collection in ['documents', 'queue']:
            result = db[collection].delete_many({})
            print(f"✓ Очищена коллекция '{collection}': удалено {result.deleted_count} записей")
    
    print("\nБаза данных полностью очищена.")
    print("=" * 60)
    
    client.close()

if __name__ == "__main__":
    reset_database()