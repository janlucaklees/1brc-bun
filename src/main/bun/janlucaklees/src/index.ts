import os from "os";

import type LocationData from "./types/LocationData";

// Settings
// This is roughly double the size of one chunk from the stream reader, at least on my system.
const initialBufferSize = 51200;
const numWorkers: number = os.cpus().length - 1;

// Constants
const LINE_BREAK = 10;

// Setup workers.
const workers = setupWorkers(numWorkers);

// Set initial values.
let currentBufferSize = initialBufferSize;
let buffer: Uint8Array = new Uint8Array(currentBufferSize);
let currentLength = 0;
let usableLength = 0;
let currentWorker = 0;

// Read the file as a stream.
const stream = Bun.file(Bun.argv[2]).stream();

// Process the file
for await (const chunk of stream) {
  handleChunk(chunk);
}

// Wait for workers to terminate and return their computed data then merge it.
const locationsMap = mergeWorkerData(await Promise.all(flushWorkers()));

// Sort Locations
const locations = Array.from(locationsMap.keys());
locations.sort();

// Print locations
let result =
  "{" +
  locations
    .map((location) => {
      const data = locationsMap.get(location)!;
      return `${location}=${(data.minTemperature / 10).toFixed(1)}/${(data.measurementSum / data.measurementCount / 10).toFixed(1)}/${(data.maxTemperature / 10).toFixed(1)}`;
    })
    .join(", ") +
  "}";

console.log(result);

/******************************************************************************
 * Functions
 *****************************************************************************/
function setupWorkers(numWorkers: number) {
  const workerURL = new URL("worker.ts", import.meta.url).href;

  const workers = new Array<Worker>(numWorkers);
  for (let i = 0; i < numWorkers; i++) {
    workers[i] = new Worker(workerURL);
  }

  return workers;
}

function handleChunk(chunk: Uint8Array) {
  // Check whether we need to flush the buffer
  if (currentLength + chunk.length > currentBufferSize) {
    // Find the last line-break (10 in a Uint8Array), so that we know up until where we can use the contents of the buffer.
    for (let i = currentLength - 1; i >= 0; i--) {
      if (buffer[i] === LINE_BREAK) {
        usableLength = i + 1;
        break;
      }
    }

    // Offload processing to a worker
    workers[currentWorker].postMessage(buffer.subarray(0, usableLength));
    currentWorker = (currentWorker + 1) % numWorkers;

    // Move the new leftover to the start of the buffer and update it's length.
    buffer.copyWithin(0, usableLength, currentLength);
    currentLength = currentLength - usableLength;

    // Increase the buffer size if necessary. We only do that if the next chunk won't fit in it.
    if (currentLength + chunk.length > currentBufferSize) {
      currentBufferSize =
        Math.pow(
          2,
          Math.ceil(
            Math.log2((currentLength + chunk.length) / currentBufferSize)
          )
        ) * currentBufferSize;
      const newBuffer = new Uint8Array(currentBufferSize);
      newBuffer.set(buffer);
      buffer = newBuffer;
    }
  }

  // Put the new chunk into the buffer, so that it continues right after the last leftover.
  // This way we effectively prepend the leftover from the last iteration to the chunk.
  buffer.set(chunk, currentLength);
  currentLength += chunk.length;
}

function flushWorkers() {
  const aggregatePromises = new Array(numWorkers);
  for (let i = 0; i < numWorkers; i++) {
    const worker = workers[i];

    aggregatePromises[i] = new Promise((resolve) => {
      worker.onmessage = (event: MessageEvent) => {
        resolve(event.data);
      };
    });

    // Pass an empty array to signal termination.
    worker.postMessage(new Uint8Array());
  }
  return aggregatePromises;
}

function mergeWorkerData(aggregate: Array<Map<string, LocationData>>) {
  const locationsMap = new Map<string, LocationData>();
  for (let i = 0; i < aggregate.length; i++) {
    for (const [location, data] of aggregate[i]) {
      // Location does not exist? Just initialize it with the data we currently have.
      if (!locationsMap.has(location)) {
        locationsMap.set(location, data);
        continue;
      }

      // Location already exists in the map, merge it.
      const otherData = locationsMap.get(location)!;
      otherData.measurementCount += data.measurementCount;
      otherData.measurementSum += data.measurementSum;
      otherData.maxTemperature = Math.max(
        otherData.maxTemperature,
        data.maxTemperature
      );
      otherData.minTemperature = Math.min(
        otherData.minTemperature,
        data.minTemperature
      );
    }
  }
  return locationsMap;
}
