# `@synvox/offline`

An offline sync database.

## Define a storage engine

Offline does not come bundled with a storage engine, but you can implement it on top of any key-value store. Here's an example using localStorage:

```js
import {StorageEngine, MemoryStorage} from '@synvox/offline'

class LocalStorageEngine implements StorageEngine {
  async getItem<T>(key: string){
    if (!(key in localStorage)) return undefined
    return JSON.parse(localStorage[key]) as T
  }
  async setItem<T>(key: string, value: T){
    const json = JSON.stringify(value)
    localStorage[key] = json
  }
  async removeItem(key: string) {
    delete localStorage[key]
  }
  async getAllKeys() {
    return localStorage.keys()
  };
  async clear() {
    localStorage.length=0
  };
  async transaction(fn: (trx: MemoryStorage) => Promise<void>){
    const trx = new MemoryStorage(this);
    try {
      await fn(trx);
      await trx.commit();
    } catch (e) {
      trx.clear();
      throw e;
    }
  };
}
```

## Define tables you want to sync

```js
const storage = new LocalStorageEngine();
const db = new Database(storage);

db.table({
  key: 'pages',
  async getSince(since: Date) {
    return getAllArticlesUpdatedAfter(date);
  },
});

db.sync();
```

## Query the Database

```js
await db.query('pages', {
  bookId: '123',
});
```

This will scan through each item in the pages table for pages that have `bookId = 123`.

## Define Indexes

You can optionally specify indexes for offline to use instead of scanning through each item.
Define `indexes` with `{[indexName:string]: columnName}`.

```js
db.table({
  key: 'pages',
  indexes: {
    pageIdIndex: 'pageId',
  },
  async getSince(since: Date) {
    return getAllArticlesUpdatedAfter(date);
  },
});

await db.query('pages', {
  bookId: '123',
});
```

## Removing Items

You can define a function which all changed items will go through to be checked if they were deleted. If the function returns true, the item will be removed from the storage engine and indexes updated accordingly.

```js
db.table({
  key: 'pages',
  indexes: {
    pageIdIndex: 'pageId',
  },
  async getSince(since: Date) {
    return getAllArticlesUpdatedAfter(date);
  },
  isItemDeleted(item) {
    return item._deleted;
  },
});

await db.query('pages', {
  bookId: '123',
});
```
