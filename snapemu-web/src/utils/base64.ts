export function hexToBase64(hex: string): string {
  const len = hex.length;
  let index = 0;
  const arr = [];
  while (len > index) {
    const cur = parseInt(hex.slice(index, index + 2), 16);
    index = index + 2;
    arr.push(cur)
  }
  const s = String.fromCodePoint(...arr);
  return btoa(s);
}

export function base64ToHex(base64: string): string {
  const bytes = atob(base64).split('');
  let s = '';
  for (const bytesKey in bytes) {
    s = s + bytes[bytesKey].charCodeAt(0).toString(16).padStart(2, '0');
  }
  return s.toUpperCase();
}