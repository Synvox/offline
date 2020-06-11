import { StorageEngine } from '.';

export type TableDef = {
  tableName: string;
  schemaName: string;
  tablePath: string;
  indexes: { [key: string]: string };
  getSince(since: Date | null): Promise<any[]>;
  isItemDeleted?(item: any): boolean;
};

type IndexValue = { [key: string]: any };
type TableIndex = { value: any; ids: string[] }[];
type Meta = { lastSync: null | string };

export default class Database {
  storage: StorageEngine;
  tables: TableDef[];

  constructor(storage: StorageEngine) {
    this.storage = storage;
    this.tables = [];
  }

  table(table: Partial<TableDef> & { tableName?: string }) {
    table.schemaName = table.schemaName || 'public';
    table.tablePath =
      table.tablePath || `${table.schemaName}.${table.tableName}`;
    this.tables.push(table as TableDef);
  }

  async sync() {
    const commitFunctions = await Promise.all(
      Object.values(this.tables).map(table => this.syncTable(table))
    );
    await Promise.all(commitFunctions.map(fn => fn()));
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

    let since = meta.lastSync ? new Date(meta.lastSync) : null;

    let updatedItems: any[] = [];

    const data = await table.getSince(since);
    updatedItems.push(...data);

    return async () => {
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
          await this.deleteItem(table, item.id);
          continue;
        }

        const itemKey = `${table.tablePath}.rows.${item.id}`;
        const indexesKey = `${table.tablePath}.rows.${item.id}.indexes`;
        const oldIndexes: IndexValue =
          (await this.storage.getItem(indexesKey)) || {};

        await this.storage.setItem(itemKey, item);
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

        await this.storage.setItem(indexesKey, newIndexes);

        for (let indexName of Object.keys(table.indexes)) {
          if (!(indexName in removeFrom) && !(indexName in updateTo)) continue;

          const indexKey = `${table.tablePath}.indexes.${indexName}`;

          let index = ((await this.storage.getItem(indexKey)) ||
            []) as TableIndex;

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

          await this.storage.setItem(indexKey, index);
        }
      }

      await this.setTableMeta(table, {
        ...meta,
        lastSync: new Date().toISOString(),
      });
    };
  }

  getTableByTablePath(tablePath: string) {
    const table = this.tables.find(table => table.tablePath === tablePath);
    if (!table)
      throw new Error(`The table with path ${tablePath} was not found.`);
    return table;
  }

  async query(tablePath: string, filter: any) {
    const table = this.getTableByTablePath(tablePath);
    return await this.queryTable(table, filter);
  }

  async queryTable<T>(
    table: TableDef,
    filter: any
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

    // get all rows for our remaining ids
    let rows = await Promise.all(
      ids.map(async id => await this.storage.getItem(`${rowsKeys}.${id}`))
    );

    // filter out those
    rows = rows.filter((row: any) => {
      if (!row) return false;
      return Object.entries(scanFilters).every(([column, value]) => {
        if (row[column] === undefined) return true; // @TODO for view functions?
        return row[column] === value;
      });
    });

    return Object.assign([...rows], { indexes }) as T[] & { indexes: string[] };
  }

  async delete(tablePath: string, filter: any) {
    const table = this.getTableByTablePath(tablePath);
    const rows = await this.queryTable(table, filter);

    for (let row of rows) {
      await this.deleteItem(table, (row as any).id);
    }
  }

  async deleteItem(table: TableDef, id: string) {
    const indexesKey = `${table.tablePath}.rows.${id}.indexes`;
    const rowKey = `${table.tablePath}.rows.${id}`;
    const removeFrom: IndexValue =
      (await this.storage.getItem(indexesKey)) || {};

    for (let indexName of Object.keys(table.indexes)) {
      const remove = removeFrom[indexName];
      const indexKey = `${table.tablePath}.indexes.${indexName}`;

      let index = ((await this.storage.getItem(indexKey)) || []) as TableIndex;

      if (remove !== undefined) {
        index = index.map(group => {
          if (group.value !== remove) return group;
          return {
            ...group,
            ids: group.ids.filter(i => i !== id),
          };
        });
      }

      await this.storage.setItem(indexKey, index);
    }

    await Promise.all([
      this.storage.removeItem(rowKey),
      this.storage.removeItem(indexesKey),
    ]);
  }

  async clear() {
    return this.storage.clear();
  }
}
