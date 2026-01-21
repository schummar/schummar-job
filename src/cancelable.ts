export type Cancelable<T = unknown> = T &
  Disposable & {
    (): void;
  };

export function createCancelable(cancel: () => void): Cancelable;
export function createCancelable<T>(object: T, cancel: () => void): T & Cancelable;
export function createCancelable(arg0: object | (() => void), arg1?: () => void): Cancelable {
  const object = typeof arg0 === 'function' ? {} : arg0;
  const cancel = typeof arg0 === 'function' ? arg0 : arg1!;

  return Object.assign(() => cancel(), object, (Symbol.dispose ? { [Symbol.dispose]: cancel } : {}) as Disposable);
}
