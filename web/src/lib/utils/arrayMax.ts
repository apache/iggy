export const arrayMax = (array: number[]) => {
  if (array.length === 0) return 0;

  return Math.max(...array);
};
