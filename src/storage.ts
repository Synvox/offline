export interface StorageEngine {
  getItem: <T>(key: string) => Promise<T | undefined>;
  setItem: <T>(key: string, value: T) => Promise<void>;
  removeItem: (key: string) => Promise<void>;
  getAllKeys: () => Promise<string[]>;
  clear: () => Promise<void>;
}

export class MemoryStorage implements StorageEngine {
  private state: { [key: string]: unknown } = {};
  async getItem<T>(key: string) {
    return this.state[key] as T | undefined;
  }
  async setItem<T>(key: string, value: T) {
    this.state[key] = value;
  }
  async removeItem(key: string) {
    delete this.state[key];
  }
  async getAllKeys() {
    return Object.keys(this.state);
  }
  async clear() {
    this.state = {};
  }
}
