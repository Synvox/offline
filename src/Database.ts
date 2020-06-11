import { StorageEngine, MemoryStorage } from '.';

export type TableDef = {
  tableName: string;
  schemaName: string;
  tablePath: string;
  indexes: { [key: string]: string };
  getSince(since: Date | null): Promise<any[]>;
  isItemDeleted?(item: any): boolean;
  forceSync: boolean;
};

type IndexValue = { [key: string]: any };
type TableIndex = { value: any; ids: string[] }[];
type Meta = { lastSync: null | string };

export default class Database {
  storage: StorageEngine;
  tables: TableDef[];
  syncing: boolean;

  constructor(storage: StorageEngine) {
    this.storage = storage;
    this.tables = [];
    this.syncing = false;
  }

  table(table: Partial<TableDef> & { tableName?: string }) {
    table.schemaName = table.schemaName || 'public';
    table.tablePath =
      table.tablePath || `${table.schemaName}.${table.tableName}`;
    table.forceSync = Boolean(table.forceSync);
    this.tables.push(table as TableDef);
  }

  async sync() {
    const commitFunctions = await Promise.all(
      Object.values(this.tables).map(table => this.syncTable(table))
    );

    this.syncing = true;
    await this.storage.transaction(async (trx: MemoryStorage) => {
      await Promise.all(commitFunctions.map(fn => fn(trx)));
    });
    this.syncing = false;
  }

  async getTableMeta(table: TableDef): Promise<Meta> {
    const metaKey = `${table.tablePath}.meta`;
    let meta = (await this.storage.getItem(metaKey)) as Meta;
    if (meta) return meta as Meta;

    meta = { lastSync: null };
    await this.setTableMeta(table, meta);

    return meta as Meta;
  }

  async setTableMeta<Meta extends { lastSync: null | string }>(
    table: TableDef,
    meta: Meta
  ): Promise<void> {
    const metaKey = `${table.tablePath}.meta`;

    await this.storage.setItem(metaKey, meta);
  }

  async syncTable(table: TableDef) {
    const meta = await this.getTableMeta(table);

    let since =
      meta.lastSync && !table.forceSync ? new Date(meta.lastSync) : null;

    let updatedItems: any[] = [];

    const data = await table.getSince(since);
    updatedItems.push(...data);

    return async (trx: MemoryStorage) => {
      if (table.forceSync) {
        await this.clearTable(trx, table);
      }

      function recalculateIndexes(table: TableDef, item: any) {
        return Object.fromEntries(
          Object.entries(table.indexes).map(([indexName, columnName]) => {
            const value = item[columnName];
            return [indexName, value];
          })
        );
      }

      for (let item of updatedItems) {
        if (table.isItemDeleted && table.isItemDeleted(item)) {
          await this.deleteItem(trx, table, item.id);
          continue;
        }

        const itemKey = `${table.tablePath}.rows.${item.id}`;
        const indexesKey = `${table.tablePath}.rows.${item.id}.indexes`;
        const oldIndexes: IndexValue = (await trx.getItem(indexesKey)) || {};

        await trx.setItem(itemKey, item);
        const newIndexes = recalculateIndexes(table, item);

        let removeFrom: IndexValue = {};
        for (let key in oldIndexes) {
          if (!(key in newIndexes) || oldIndexes[key] !== newIndexes[key]) {
            removeFrom[key] = oldIndexes[key];
          }
        }

        let updateTo: IndexValue = {};

        for (let key in newIndexes) {
          if (!(key in oldIndexes) || oldIndexes[key] !== newIndexes[key]) {
            updateTo[key] = newIndexes[key];
          }
        }

        await trx.setItem(indexesKey, newIndexes);

        for (let indexName of Object.keys(table.indexes)) {
          if (!(indexName in removeFrom) && !(indexName in updateTo)) continue;

          const indexKey = `${table.tablePath}.indexes.${indexName}`;

          let index = ((await trx.getItem(indexKey)) || []) as TableIndex;

          const remove = removeFrom[indexName];
          if (remove !== undefined) {
            index = index.map(group => {
              if (group.value !== remove) return group;
              return {
                ...group,
                ids: group.ids.filter(id => id !== item.id),
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
                ids: [...group.ids, item.id],
              };
            });

            if (!found) {
              index = [...index, { value: add, ids: [item.id] }];
            }
          }

          await trx.setItem(indexKey, index);
        }
      }

      await this.setTableMeta(table, {
        ...meta,
        lastSync: new Date().toISOString(),
      });
    };
  }

  selectTable(...selector: string[]) {
    const [schemaName, tableName] = selector;
    const tablePath = `${schemaName}.${tableName}`;
    const table = this.tables.find(table => table.tablePath === tablePath);
    if (!table)
      throw new Error(`The table with path ${tablePath} was not found.`);
    return table;
  }

  async query(tablePath: string[], filter: any) {
    const table = this.selectTable(...tablePath);
    return await this.queryTable(table, filter);
  }

  async queryTable<T>(
    table: TableDef,
    filter: any,
    limit: null | number = null
  ): Promise<T[] & { indexes: string[] }> {
    const rowsKeys = `${table.tablePath}.rows`;
    const scanFilters: { [key: string]: any } = {};

    let ids = filter.id ? [filter.id] : null;
    let indexes = [];

    for (let [name, value] of Object.entries(filter)) {
      let [indexName] =
        Object.entries(table.indexes).find(
          ([_, columnName]) => columnName === name
        ) || [];

      if (!indexName) {
        scanFilters[name] = value;
        continue;
      }

      const indexKey = `${table.tablePath}.indexes.${indexName}`;
      const index = ((await this.storage.getItem(indexKey)) ||
        []) as TableIndex;
      const group = index.find(group => group.value === value);

      if (!group) {
        scanFilters[name] = value;
        continue;
      }

      indexes.push(indexKey);

      if (ids === null) {
        // use index ids as id set
        ids = group.ids;
      } else {
        // find intersection of ids and group ids
        ids = ids.filter(id => group.ids.includes(id));
      }
    }

    if (ids === null) {
      // if we still don't have any ids, get them all :(
      ids = (await this.storage.getAllKeys())
        .filter(key => key.startsWith(rowsKeys))
        .map(key => {
          const [id, index] = key
            .replace(rowsKeys, '')
            .split('.')
            .slice(1);

          if (index) return;
          return id;
        })
        .filter(Boolean);
    }

    const result = [];

    for (let id of ids) {
      const row = (await this.storage.getItem(`${rowsKeys}.${id}`)) as any;
      if (!row) continue;

      const matches = Object.entries(scanFilters).every(([column, value]) => {
        if (row[column] === undefined) return true; // @TODO for view functions?
        return row[column] === value;
      });

      if (matches) {
        result.push(row);
        if (limit !== null && result.length >= limit) break;
      }
    }

    return Object.assign([...result], { indexes }) as T[] & {
      indexes: string[];
    };
  }

  async delete(tablePath: string[], filter: any) {
    const table = this.selectTable(...tablePath);
    const rows = await this.queryTable(table, filter);

    await this.storage.transaction(async (trx: MemoryStorage) => {
      for (let row of rows) {
        await this.deleteItem(trx, table, (row as any).id);
      }
    });
  }

  async deleteItem(trx: MemoryStorage, table: TableDef, id: string) {
    const indexesKey = `${table.tablePath}.rows.${id}.indexes`;
    const rowKey = `${table.tablePath}.rows.${id}`;
    const removeFrom: IndexValue = (await trx.getItem(indexesKey)) || {};

    for (let indexName of Object.keys(table.indexes)) {
      const remove = removeFrom[indexName];
      const indexKey = `${table.tablePath}.indexes.${indexName}`;

      let index = ((await trx.getItem(indexKey)) || []) as TableIndex;

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

  async clearTable(trx: MemoryStorage, table: TableDef) {
    const ids = (await this.storage.getAllKeys()).filter(key =>
      key.startsWith(table.tablePath)
    );

    for (let id of ids) {
      await trx.removeItem(id);
    }
  }
}
