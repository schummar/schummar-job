export const countdown = (n: number, fn: (count: () => void) => Promise<void>, timeout = 1000): Promise<void> =>
  new Promise((resolve, reject) => {
    if (n <= 0) resolve();

    let c = 0;
    const t = setTimeout(() => reject(`Timeout (${c}/${n})`), timeout);

    fn(() => {
      c++;
      if (c === n) {
        resolve();
        clearTimeout(t);
      }
    });
  });

export const noopLogger = {
  error: (): void => {
    // ignore
  },
  warn: (): void => {
    // ignore
  },
  info: (): void => {
    // ignore
  },
};
