import { Database, MemoryStorage } from '../src';

it('supports basic functions', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 20 }).map((_, index) => {
    return { id: `${index}`, body: `Comment body ${index}`, active: true };
  });

  const Comments = db.table({
    key: 'comments',
    indexes: {
      primary: 'id',
      enabled: 'active',
    },
    async getSince() {
      const items = pendingItems;
      pendingItems = [];
      return items;
    },
  });

  await db.sync();

  expect((await Comments.query({ id: `1` })).length).toBe(1);
  expect((await Comments.query({ id: `1` })).indexes).toEqual([
    'comments/indexes/primary',
  ]);
  expect((await Comments.query({ active: true })).length).toEqual(20);
  expect((await Comments.query({ active: true })).indexes).toEqual([
    'comments/indexes/enabled',
  ]);

  pendingItems = [{ id: '1', body: 'new body', active: false }];
  await db.sync();

  expect((await Comments.query({ body: 'new body' })).length).toEqual(1);
  expect((await Comments.query({ body: 'new body' })).indexes).toEqual([]);

  // Re index
  expect((await Comments.query({ active: false })).length).toEqual(1);
  expect((await Comments.query({ active: false })).indexes).toEqual([
    'comments/indexes/enabled',
  ]);

  // multiple indexes
  expect((await Comments.query({ active: false, id: '1' })).length).toEqual(1);
  expect((await Comments.query({ active: false, id: '1' })).indexes).toEqual([
    'comments/indexes/enabled',
    'comments/indexes/primary',
  ]);

  pendingItems = [{ id: '2', body: 'new body', active: false }];
  await db.sync();

  expect((await Comments.query({ active: false })).length).toEqual(2);
  expect((await Comments.query({ active: false })).indexes).toEqual([
    'comments/indexes/enabled',
  ]);

  await db.delete('comments', { active: false });
  expect((await Comments.query({ active: false })).length).toEqual(0);

  // clear
  await db.clear();
});

it('deletes when items are deleted', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 20 }).map((_, index) => {
    return {
      id: `${index}`,
      body: `Comment body ${index}`,
      active: true,
      deleted: false,
    };
  });

  const Comments = db.table({
    key: 'comments',
    indexes: {
      primary: 'id',
      enabled: 'active',
    },
    async getSince() {
      const items = pendingItems;
      pendingItems = [];
      return items;
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  await db.sync();

  expect((await Comments.query({})).length).toEqual(20);

  pendingItems = [{ id: '1', body: 'bod', active: false, deleted: true }];
  await db.sync();

  expect((await Comments.query({})).length).toEqual(19);
});

it('throws when a table is not found', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  db.table({
    key: 'comments',
    indexes: {
      primary: 'id',
      enabled: 'active',
    },
    async getSince() {
      return [];
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  await db.sync();

  let error: null | Error = null;

  try {
    await db.query('thing', {});
  } catch (e) {
    error = e;
  }

  expect(error).not.toBe(null);
});

it('supports forced syncs', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 20 }).map((_, index) => {
    return {
      id: `${index}`,
      body: `Comment body ${index}`,
      active: true,
      deleted: false,
    };
  });

  let commentsSince = null;
  let postsSince = null;

  db.table({
    key: 'comments',
    indexes: {
      primary: 'id',
      enabled: 'active',
    },
    async getSince(s) {
      commentsSince = s;
      return pendingItems;
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  db.table({
    key: 'posts',
    forceSync: true,
    indexes: {
      primary: 'id',
      enabled: 'active',
    },
    async getSince(s) {
      postsSince = s;
      return pendingItems;
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  await db.sync();
  expect(commentsSince).toBe(null);
  expect(postsSince).toBe(null);
  await db.sync();
  expect(commentsSince).not.toBe(null);
  expect(postsSince).toBe(null);
});

it('supports transactions on MemoryStorage', async () => {
  const storage = new MemoryStorage();

  await storage.setItem('key', 123);
  expect(await storage.getItem('key')).toBe(123);

  try {
    await storage.transaction(async trx => {
      expect(await trx.getItem('key')).toBe(123);

      await trx.setItem('key', 456);
      expect(await trx.getItem('key')).toBe(456);

      throw new Error();
    });
  } catch (e) {}

  expect(await storage.getItem('key')).toBe(123);

  await storage.transaction(async trx => {
    expect(await trx.getItem('key')).toBe(123);

    await trx.setItem('key', 456);
    expect(await trx.getItem('key')).toBe(456);
  });

  expect(await storage.getItem('key')).toBe(456);
});

it('supports table queries', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 20 }).map((_, index) => {
    return {
      id: `${index}`,
      body: `Comment body ${index}`,
      active: true,
      deleted: false,
    };
  });

  const Comments = db.table({
    key: 'comments',
    indexes: {
      primary: 'id',
      enabled: 'active',
    },
    async getSince() {
      return pendingItems;
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  await db.sync();

  expect(await Comments.query({ id: '1' })).toMatchInlineSnapshot(`
    Array [
      Object {
        "active": true,
        "body": "Comment body 1",
        "deleted": false,
        "id": "1",
      },
    ]
  `);

  await Comments.patch({
    id: '1',
    deleted: true,
  });

  expect(await Comments.query({ id: '1' })).toMatchInlineSnapshot(`Array []`);

  await db.transaction(async trx => {
    await Comments.patch(
      {
        id: '1',
        deleted: false,
        active: true,
        body: 'Restored',
      },
      trx
    );
  });

  expect(await Comments.query({ id: '1' })).toMatchInlineSnapshot(`
    Array [
      Object {
        "active": true,
        "body": "Restored",
        "deleted": false,
        "id": "1",
      },
    ]
  `);
});

it('supports function queries', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 50 }).map((_, index) => {
    return {
      id: `${index}`,
      active: index < 5,
      number: index * 3,
      deleted: false,
    };
  });

  const Comments = db.table({
    key: 'comments',
    indexes: {
      active: 'active',
    },
    async getSince() {
      return pendingItems;
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  await db.sync();

  let iterations = 0;
  expect(
    await Comments.query({
      active: true,
      number: (v: number) => {
        iterations++;
        return v % 2 === 0;
      },
    })
  ).toMatchInlineSnapshot(`
    Array [
      Object {
        "active": true,
        "deleted": false,
        "id": "0",
        "number": 0,
      },
      Object {
        "active": true,
        "deleted": false,
        "id": "2",
        "number": 6,
      },
      Object {
        "active": true,
        "deleted": false,
        "id": "4",
        "number": 12,
      },
    ]
  `);

  expect(iterations).toMatchInlineSnapshot(`5`);
});

it('supports function queries (indexed)', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 50 }).map((_, index) => {
    return {
      id: `${index}`,
      active: index < 5,
      number: index * 3,
      deleted: false,
    };
  });

  const Comments = db.table({
    key: 'comments',
    indexes: {
      active: 'active',
      number: 'number',
    },
    async getSince() {
      return pendingItems;
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  await db.sync();

  let iterations = 0;
  expect(
    await Comments.query({
      active: true,
      number: (v: number) => {
        iterations++;
        return v % 2 === 0;
      },
    })
  ).toMatchInlineSnapshot(`
    Array [
      Object {
        "active": true,
        "deleted": false,
        "id": "0",
        "number": 0,
      },
    ]
  `);

  expect(iterations).toMatchInlineSnapshot(`1`);
});

it('supports function queries (indexed at table def)', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 5 }).map((_, index) => {
    return {
      id: `${index}`,
      active: index < 1,
      number: index * 3,
      deleted: false,
    };
  });

  const Comments = db.table({
    key: 'comments',
    indexes: {
      indexed: row => {
        if (row.number === undefined || row.active === undefined) return;

        return row.number % 2 === 0 && row.active;
      },
    },
    async getSince() {
      return pendingItems;
    },
    isItemDeleted(item) {
      return item.deleted;
    },
  });

  await db.sync();

  const result = await Comments.query({
    active: true,
    number: 0,
  });

  expect(result.indexes).toMatchInlineSnapshot(`
    Array [
      "comments/indexes/indexed",
    ]
  `);

  expect(result).toMatchInlineSnapshot(`
    Array [
      Object {
        "active": true,
        "deleted": false,
        "id": "0",
        "number": 0,
      },
    ]
  `);
});

it('supports bulk updates', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 5 }).map((_, index) => {
    return {
      id: `${index}`,
      active: index < 1,
      number: index * 3,
      deleted: false,
    };
  });

  const Comments = db.table({
    key: 'comments',
  });

  const commit = await db.mergeTable(Comments, pendingItems);
  await db.transaction(async trx => {
    await commit(trx);
  });

  const result = await Comments.query({
    active: true,
    number: 0,
  });

  expect(result).toMatchInlineSnapshot(`
    Array [
      Object {
        "active": true,
        "deleted": false,
        "id": "0",
        "number": 0,
      },
    ]
  `);
});

it('supports url keys', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 5 }).map((_, index) => {
    return {
      id: `http://thing.com/${index}`,
      hash: `http://hash.com/${index}`,
    };
  });

  const Comments = db.table({
    key: 'comments',
    indexes: {
      hash: 'hash',
    },
  });

  const commit = await db.mergeTable(Comments, pendingItems);
  await db.transaction(async trx => {
    await commit(trx);
  });

  expect(await Comments.query({})).toMatchInlineSnapshot(`
    Array [
      Object {
        "hash": "http://hash.com/0",
        "id": "http://thing.com/0",
      },
      Object {
        "hash": "http://hash.com/1",
        "id": "http://thing.com/1",
      },
      Object {
        "hash": "http://hash.com/2",
        "id": "http://thing.com/2",
      },
      Object {
        "hash": "http://hash.com/3",
        "id": "http://thing.com/3",
      },
      Object {
        "hash": "http://hash.com/4",
        "id": "http://thing.com/4",
      },
    ]
  `);

  expect(
    await Comments.query({
      id: `http://thing.com/1`,
    })
  ).toMatchInlineSnapshot(`
    Array [
      Object {
        "hash": "http://hash.com/1",
        "id": "http://thing.com/1",
      },
    ]
  `);

  expect(
    await Comments.query({
      hash: `http://hash.com/2`,
    })
  ).toMatchInlineSnapshot(`
    Array [
      Object {
        "hash": "http://hash.com/2",
        "id": "http://thing.com/2",
      },
    ]
  `);
});
