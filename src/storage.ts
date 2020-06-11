export interface StorageEngine {
  getItem: <T>(key: string) => Promise<T | undefined>;
  setItem: <T>(key: string, value: T) => Promise<void>;
  removeItem: (key: string) => Promise<void>;
  getAllKeys: () => Promise<string[]>;
  clear: () => Promise<void>;
  transaction: (fn: (trx: MemoryStorage) => Promise<void>) => Promise<void>;
}

export class MemoryStorage implements StorageEngine {
  private state: { [key: string]: unknown } = {};
  private pendingKeys: string[];
  private fallback: StorageEngine | null;
  constructor(fallback: StorageEngine | null = null) {
    this.fallback = fallback || null;
    this.pendingKeys = [];
  }
  async getItem<T>(key: string) {
    if (!this.fallback || this.state.hasOwnProperty(key)) {
      return this.state[key] as T | undefined;
    }

    const value = await this.fallback.getItem<T>(key);
    this.state[key] = value;

    return value as T | undefined;
  }
  async setItem<T>(key: string, value: T) {
    this.state[key] = value;
    if (this.fallback) {
      this.pendingKeys.push(key);
    }
  }
  async removeItem(key: string) {
    delete this.state[key];
    if (this.fallback) {
      this.pendingKeys.push(key);
      await this.fallback.removeItem(key);
    }
  }
  async getAllKeys() {
    let keys = [
      ...(this.fallback ? await this.fallback.getAllKeys() : []),
      ...Object.keys(this.state),
    ];

    return Array.from(new Set(keys));
  }
  async clear() {
    this.state = {};
    this.pendingKeys = [];
  }
  async transaction(fn: (trx: MemoryStorage) => Promise<void>) {
    const trx = new MemoryStorage(this);

    try {
      await fn(trx);
      await trx.commit();
    } catch (e) {
      this.clear();
      throw e;
    }
  }
  async commit() {
    if (!this.fallback) return;

    const pendingKeys = this.pendingKeys;
    this.pendingKeys = [];

    for (let key of pendingKeys) {
      if (this.state.hasOwnProperty(key)) {
        await this.fallback.setItem(key, this.state[key]);
      } else {
        await this.fallback.removeItem(key);
      }
    }
  }
}
