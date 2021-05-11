export type MaybePromise<T> = T | Promise<T>;

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));
