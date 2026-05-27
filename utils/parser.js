function parseDriverMessage(text) {
  if (!text) return null;

  const lines = text
    .split("\n")
    .map(v => v.trim())
    .filter(Boolean);

  if (lines.length < 3) return null;

  const firstLine = lines[0];
  const plate = lines[1].toUpperCase();
  const thirdLine = lines[2];

  const firstMatch = firstLine.match(/^(\S+)\s+(.+)$/);
  if (!firstMatch) return null;

  const orderCode = firstMatch[1];
  const address = firstMatch[2];

  if (/^(到|抵達)$/.test(thirdLine)) {
    return { type: "arrived", orderCode, address, plate };
  }

  if (/^(上|客上)$/.test(thirdLine)) {
    return { type: "customer_on", orderCode, address, plate };
  }

  const minuteMatch = thirdLine.match(/(\d+)/);
  if (!minuteMatch) return null;

  return {
    type: "report",
    orderCode,
    address,
    plate,
    minutes: Number(minuteMatch[1])
  };
}

module.exports = {
  parseDriverMessage
};