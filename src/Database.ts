import { StorageEngine, MemoryStorage } from '.';

export type TableDef = {
  key: string;
  keyPath: string;
  indexes: { [key: string]: string | ((row: any) => any) };
  getSince(since: Date | null): Promise<any[]>;
  isItemDeleted?(item: any): boolean;
  forceSync: boolean;
};

type IndexValue = { [key: string]: any };
type TableIndex = { value: any; ids: string[] }[];
type Meta = { lastSync: null | string };
type PaginationProps = {
  limit?: number;
  offset?: number;
};

const storageKey = {
  table: (key: string) => encodeURIComponent(key),
  tableMeta: (key: string) => `${storageKey.table(key)}/meta`,
  rows: (key: string) => `${storageKey.table(key)}/rows`,
  row: (key: string, id: string) =>
    `${storageKey.rows(key)}/${encodeURIComponent(id)}`,
  rowIndex: (key: string, id: string) =>
    `${storageKey.table(key)}/rows/${encodeURIComponent(id)}/indexes`,
  index: (key: string, indexName: string) =>
    `${storageKey.table(key)}/indexes/${encodeURIComponent(indexName)}`,
};

export class Table {
  key: string;
  keyPath: string;
  indexes: { [key: string]: string | ((row: any) => any) };
  getSince: (since: Date | null) => Promise<any[]>;
  isItemDeleted?(item: any): boolean;
  forceSync: boolean;
  database: Database;
  constructor(tableDef: TableDef, database: Database) {
    this.database = database;
    this.key = tableDef.key;
    this.keyPath = tableDef.keyPath;
    this.indexes = tableDef.indexes ?? {};
    this.getSince = tableDef.getSince ?? (() => []);
    this.isItemDeleted = tableDef.isItemDeleted ?? (() => false);
    this.forceSync = tableDef.forceSync ?? false;
  }
  async query<T>(filter: any, paginationProps: PaginationProps = {}) {
    return await this.database.queryTable<T>(this, filter, paginationProps);
  }
  async patch(item: any, trx?: MemoryStorage) {
    if (trx) await this.database.setItem(trx, this, item);
    else {
      await this.database.transaction(async trx => {
        await this.database.setItem(trx, this, item);
      });
    }
  }
}

export default class Database {
  private storage: StorageEngine;
  private tables: Table[];
  private syncing: boolean;

  constructor(storage: StorageEngine) {
    this.storage = storage;
    this.tables = [];
    this.syncing = false;
  }

  table(tableDef: Partial<TableDef> & { key: string }) {
    tableDef.forceSync = Boolean(tableDef.forceSync);
    tableDef.keyPath = tableDef.keyPath ?? 'id';
    const tableDefFull = tableDef as TableDef;
    const table = new Table(tableDefFull, this);
    this.tables.push(table);
    return table;
  }

  async transaction(action: (trx: MemoryStorage) => Promise<void>) {
    await this.storage.transaction(action);
  }

  async sync() {
    const commitFunctions = await Promise.all(
      Object.values(this.tables).map(table => this.syncTable(table))
    );

    this.syncing = true;
    await this.transaction(async (trx: MemoryStorage) => {
      for (let fn of commitFunctions) {
        await fn(trx);
      }
    });
    this.syncing = false;
  }

  getSyncing() {
    return this.syncing;
  }

  private async getTableMeta(table: TableDef): Promise<Meta> {
    const metaKey = storageKey.tableMeta(table.key);
    let meta = (await this.storage.getItem(metaKey)) as Meta;
    if (meta) return meta as Meta;

    meta = { lastSync: null };
    await this.setTableMeta(table, meta);

    return meta as Meta;
  }

  private async setTableMeta<Meta extends { lastSync: null | string }>(
    table: TableDef,
    meta: Meta
  ): Promise<void> {
    const metaKey = storageKey.tableMeta(table.key);

    await this.storage.setItem(metaKey, meta);
  }

  private recalculateIndexes(table: TableDef, item: any) {
    return Object.fromEntries(
      Object.entries(table.indexes).map(([indexName, columnName]) => {
        const value =
          typeof columnName === 'function'
            ? columnName(item)
            : item[columnName];

        return [indexName, value];
      })
    );
  }

  async setItem(trx: MemoryStorage, table: Table, item: any) {
    if (table.isItemDeleted && table.isItemDeleted(item)) {
      await this.deleteItem(trx, table, item[table.keyPath]);
      return;
    }

    const itemKey = storageKey.row(table.key, item[table.keyPath]);
    const indexesKey = storageKey.rowIndex(table.key, item[table.keyPath]);
    let oldIndexes: IndexValue | undefined = await trx.getItem(indexesKey);
    if (!oldIndexes) oldIndexes = {};

    await trx.setItem(itemKey, item);
    const newIndexes = this.recalculateIndexes(table, item);

    let removeFrom: IndexValue = {};
    for (let key in oldIndexes) {
      if (!(key in newIndexes) || oldIndexes[key] !== newIndexes[key]) {
        removeFrom[key] = oldIndexes[key];
      }
    }

    let updateTo: IndexValue = {};

    for (let key in newIndexes) {
      if (
        newIndexes[key] !== undefined &&
        (!(key in oldIndexes) || oldIndexes[key] !== newIndexes[key])
      ) {
        updateTo[key] = newIndexes[key];
      }
    }

    await trx.setItem(indexesKey, newIndexes);

    for (let indexName of Object.keys(table.indexes)) {
      if (!(indexName in removeFrom) && !(indexName in updateTo)) continue;

      const indexKey = storageKey.index(table.key, indexName);

      let index = (await trx.getItem(indexKey)) as TableIndex | undefined;
      if (!index) index = [];

      const remove = removeFrom[indexName];
      if (remove !== undefined) {
        index = index.map(group => {
          if (group.value !== remove) return group;
          return {
            ...group,
            ids: group.ids.filter(id => id !== item[table.keyPath]),
          };
        });
      }

      const add = updateTo[indexName];
      if (add !== undefined) {
        let found = false;
        index = index.map(group => {
          if (group.value !== add) return group;
          found = true;
          return {
            ...group,
            ids: [...group.ids, item[table.keyPath]],
          };
        });

        if (!found) {
          index = [...index, { value: add, ids: [item[table.keyPath]] }];
        }
      }

      await trx.setItem(indexKey, index);
    }
  }

  private async syncTable(table: Table) {
    const meta = await this.getTableMeta(table);

    let since =
      meta.lastSync && !table.forceSync ? new Date(meta.lastSync) : null;

    let updatedItems: any[] = [];

    const data = await table.getSince(since);
    updatedItems.push(...data);

    return this.mergeTable(table, updatedItems);
  }

  async mergeTable(table: Table, updatedItems: any[]) {
    const meta = await this.getTableMeta(table);
    return async (trx: MemoryStorage) => {
      if (table.forceSync) {
        await this.clearTable(trx, table);
      }

      for (let item of updatedItems) {
        await this.setItem(trx, table, item);
      }

      await this.setTableMeta(table, {
        ...meta,
        lastSync: new Date().toISOString(),
      });
    };
  }

  hasTable(key: string) {
    return this.tables.some(table => table.key === key);
  }

  selectTable(key: string) {
    const table = this.tables.find(table => table.key === key);
    if (!table) throw new Error(`The table with path ${key} was not found.`);
    return table;
  }

  async query(key: string, filter: any, paginationProps: PaginationProps = {}) {
    const table = this.selectTable(key);
    return await this.queryTable(table, filter, paginationProps);
  }

  async queryTable<T>(
    table: Table,
    filter: any,
    { limit, offset }: PaginationProps
  ): Promise<T[] & { indexes: string[] }> {
    const rowsKeys = storageKey.rows(table.key);
    const scanFilters: { [key: string]: any } = {};

    let ids = filter[table.keyPath] ? [filter[table.keyPath]] : null;
    let indexes = [];

    for (let [indexName, indexFactory] of Object.entries(table.indexes)) {
      if (typeof indexFactory !== 'function') continue;

      const indexKey = storageKey.index(table.key, indexName);

      let index = (await this.storage.getItem(indexKey)) as
        | TableIndex
        | undefined;

      if (!index) index = [];

      const indexValue = indexFactory(filter);
      if (indexValue === undefined) continue;

      const group = index.find(group => group.value === indexValue);

      if (!group) continue;

      indexes.push(indexKey);
      const groupIds = group.ids.map(decodeURIComponent);

      if (ids === null) {
        // use index ids as id set
        ids = groupIds;
      } else {
        // find intersection of ids and group ids
        ids = ids.filter(id => groupIds.includes(id));
      }
    }

    for (let [name, value] of Object.entries(filter)) {
      let [indexName] =
        Object.entries(table.indexes)
          .filter(([_, v]) => typeof v === 'string')
          .find(([_, columnName]) => columnName === name) || [];

      if (!indexName) {
        scanFilters[name] = value;
        continue;
      }

      const indexKey = storageKey.index(table.key, indexName);

      let index = (await this.storage.getItem(indexKey)) as
        | TableIndex
        | undefined;
      if (!index) index = [];

      const group =
        typeof value === 'function'
          ? index.find(group => (value as any)(group.value))
          : index.find(group => group.value === value);

      if (!group) {
        scanFilters[name] = value;
        continue;
      }

      indexes.push(indexKey);

      const groupIds = group.ids.map(decodeURIComponent);

      if (ids === null) {
        // use index ids as id set
        ids = groupIds;
      } else {
        // find intersection of ids and group ids
        ids = ids.filter(id => groupIds.includes(id));
      }
    }

    if (ids === null) {
      // if we still don't have any ids, get them all :(
      ids = (await this.storage.getAllKeys())
        .filter(key => key.startsWith(rowsKeys))
        .map(key => {
          const [id, index] = key
            .replace(rowsKeys, '')
            .split('/')
            .slice(1);

          if (index) return;
          return decodeURIComponent(id);
        })
        .filter(Boolean);
    }

    const result = [];

    let offsetRemaining = Number(offset);
    for (let id of ids) {
      const row = (await this.storage.getItem(
        storageKey.row(table.key, id)
      )) as any;
      if (!row) continue;

      const matches = Object.entries(scanFilters).every(([column, value]) => {
        if (typeof value === 'function') return Boolean(value(row[column]));
        return row[column] === value;
      });

      if (matches) {
        if (offsetRemaining) {
          offsetRemaining--;
          continue;
        }

        result.push(row);
        if (limit && result.length >= limit) break;
      }
    }

    return Object.assign([...result], { indexes }) as T[] & {
      indexes: string[];
    };
  }

  async delete(key: string, filter: any) {
    const table = this.selectTable(key);
    const rows = await this.queryTable(table, filter, {});

    await this.transaction(async (trx: MemoryStorage) => {
      for (let row of rows) {
        await this.deleteItem(trx, table, (row as any)[table.keyPath]);
      }
    });
  }

  private async deleteItem(trx: MemoryStorage, table: Table, id: string) {
    const indexesKey = storageKey.rowIndex(table.key, id);
    const rowKey = storageKey.row(table.key, id);
    let removeFrom: IndexValue | undefined = await trx.getItem(indexesKey);
    if (!removeFrom) removeFrom = {};

    for (let indexName of Object.keys(table.indexes)) {
      const remove = removeFrom[indexName];
      const indexKey = storageKey.index(table.key, indexName);

      let index = (await trx.getItem(indexKey)) as TableIndex | undefined;
      if (!index) index = [];

      if (remove !== undefined) {
        index = index.map(group => {
          if (group.value !== remove) return group;
          return {
            ...group,
            ids: group.ids.filter(i => i !== id),
          };
        });
      }

      await trx.setItem(indexKey, index);
    }

    await Promise.all([trx.removeItem(rowKey), trx.removeItem(indexesKey)]);
  }

  async clear() {
    return this.storage.clear();
  }

  private async clearTable(trx: MemoryStorage, table: Table) {
    const ids = (await this.storage.getAllKeys()).filter(key =>
      key.startsWith(table.key)
    );

    for (let id of ids) {
      await trx.removeItem(id);
    }
  }
}
