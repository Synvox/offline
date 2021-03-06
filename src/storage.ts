export interface StorageEngine {
  getItem: <T>(key: string) => Promise<T | undefined>;
  setItem: <T>(key: string, value: T) => Promise<void>;
  removeItem: (key: string) => Promise<void>;
  getAllKeys: () => Promise<string[]>;
  clear: () => Promise<void>;
  transaction: (fn: (trx: MemoryStorage) => Promise<void>) => Promise<void>;
}

export class MemoryStorage implements StorageEngine {
  public state: { [key: string]: unknown };
  private pendingKeys: string[];
  private fallback: StorageEngine | null;

  constructor(fallback: StorageEngine | null = null) {
    this.state = {};
    this.pendingKeys = [];
    this.fallback = fallback || null;
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
      trx.clear();
      throw e;
    }
  }

  async commit() {
    if (!this.fallback) return;

    const pendingKeys = this.pendingKeys;
    this.pendingKeys = [];

    await Promise.all(
      pendingKeys.map(async key => {
        if (this.state.hasOwnProperty(key)) {
          await this.fallback!.setItem(key, this.state[key]);
        } else {
          await this.fallback!.removeItem(key);
        }
      })
    );
  }
}
