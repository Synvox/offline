import { Database, MemoryStorage } from '../src';

it('supports basic functions', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  let pendingItems = Array.from({ length: 20 }).map((_, index) => {
    return { id: `${index}`, body: `Comment body ${index}`, active: true };
  });

  db.table({
    tableName: 'comments',
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

  expect((await db.query('public.comments', { id: `1` })).length).toBe(1);
  expect((await db.query('public.comments', { id: `1` })).indexes).toEqual([
    'public.comments.indexes.primary',
  ]);
  expect((await db.query('public.comments', { active: true })).length).toEqual(
    20
  );
  expect(
    (await db.query('public.comments', { active: true })).indexes
  ).toEqual(['public.comments.indexes.enabled']);

  pendingItems = [{ id: '1', body: 'new body', active: false }];
  await db.sync();

  expect(
    (await db.query('public.comments', { body: 'new body' })).length
  ).toEqual(1);
  expect(
    (await db.query('public.comments', { body: 'new body' })).indexes
  ).toEqual([]);

  // Re index
  expect((await db.query('public.comments', { active: false })).length).toEqual(
    1
  );
  expect(
    (await db.query('public.comments', { active: false })).indexes
  ).toEqual(['public.comments.indexes.enabled']);

  // multiple indexes
  expect(
    (await db.query('public.comments', { active: false, id: '1' })).length
  ).toEqual(1);
  expect(
    (await db.query('public.comments', { active: false, id: '1' })).indexes
  ).toEqual([
    'public.comments.indexes.enabled',
    'public.comments.indexes.primary',
  ]);

  pendingItems = [{ id: '2', body: 'new body', active: false }];
  await db.sync();

  expect((await db.query('public.comments', { active: false })).length).toEqual(
    2
  );
  expect(
    (await db.query('public.comments', { active: false })).indexes
  ).toEqual(['public.comments.indexes.enabled']);

  await db.delete('public.comments', { active: false });
  expect((await db.query('public.comments', { active: false })).length).toEqual(
    0
  );

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

  db.table({
    tableName: 'comments',
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

  expect((await db.query('public.comments', {})).length).toEqual(20);

  pendingItems = [{ id: '1', body: 'bod', active: false, deleted: true }];
  await db.sync();

  expect((await db.query('public.comments', {})).length).toEqual(19);
});

it('throws when a table is not found', async () => {
  const storage = new MemoryStorage();
  const db = new Database(storage);

  db.table({
    tableName: 'comments',
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
    await db.query('public.thing', {});
  } catch (e) {
    error = e;
  }

  expect(error).not.toBe(null);
});
