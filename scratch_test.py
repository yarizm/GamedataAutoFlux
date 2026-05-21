import asyncio
from src.storage.local_store import LocalStorage

async def main():
    store = LocalStorage({'db_name': 'reports.db'})
    await store.initialize()
    result = await store.query('key:report:')
    print("total:", result.total)
    for r in result.records:
        print(r.key)
    await store.close()

if __name__ == '__main__':
    asyncio.run(main())
