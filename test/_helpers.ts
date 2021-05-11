export const countdown = (n: number, fn: (count: () => void) => Promise<void>, timeout = 1000): Promise<void> =>
  new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(`Timeout`), timeout);

    fn(() => {
      n--;
      if (n === 0) {
        resolve();
        clearTimeout(t);
      }
    });
  });
