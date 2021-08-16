export const countdown = (n: number, fn: (count: () => void) => Promise<void>, timeout = 1000, waitUntilTimeout?: boolean): Promise<void> =>
  new Promise((resolve, reject) => {
    if (n <= 0) resolve();

    let c = 0;
    const t = setTimeout(() => {
      if (waitUntilTimeout) {
        if (c === n) resolve();
        else reject(`Wrong count (${c} != ${n})`);
      } else reject(`Timeout (${c}/${n})`);
    }, timeout);

    fn(() => {
      c++;
      if (!waitUntilTimeout && c === n) {
        resolve();
        clearTimeout(t);
      }
    });
  });

export const noopLogger = () => {
  // ignore
};
