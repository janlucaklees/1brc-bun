type LocationData = {
  sum: number;
  count: number;
  min: number;
  max: number;
};

const fileName = Bun.argv[2];

const file = Bun.file(fileName);
const stream = file.stream();

const decoder = new TextDecoder();

const map = new Map<string, LocationData>();

let str = "";

for await (const rawChunk of stream) {
  const chunk = decoder.decode(rawChunk);

  str += chunk;

  const lines = str.split(/\r?\n/);

  while (lines.length > 1) {
    const line = lines.shift()!;

    if (line === "") {
      continue;
    }

    const [city, tempRaw] = line.split(";");
    const temp = parseFloat(tempRaw);

    if (!map.has(city)) {
      map.set(city, {
        sum: 0,
        count: 0,
        min: Number.POSITIVE_INFINITY,
        max: Number.NEGATIVE_INFINITY,
      } as LocationData);
    }

    const data = map.get(city)!;
    data.count++;
    data.sum += temp;

    if (temp > data.max) {
      data.max = temp;
    }

    if (temp < data.min) {
      data.min = temp;
    }
  }

  str = lines[0];
}

const resList = [];
const order = Array.from(map.keys()).sort();

for (const city of order) {
  const data = map.get(city)!;
  resList.push(
    `${city}=${data.min.toFixed(1)}/${(data.sum / data.count).toFixed(1)}/${data.max.toFixed(1)}`
  );
}

console.log("{" + resList.join(", ") + "}");
