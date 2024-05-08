import type LocationData from "./types/LocationData";

declare var self: Worker;

// Constants
const LINE_BREAK = 10;
const MINUS = 45;
const DOT = 46;
const ZERO = 48;
const SEMICOLON = 59;

const decoder = new TextDecoder();
const locationMap = new Map<string, LocationData>();

self.onmessage = (event: MessageEvent) => {
  const lines: Uint8Array = event.data;

  // Check for termination event. By definition we terminate on an empty array.
  if (lines.length === 0) {
    postMessage(locationMap);
    process.exit();
    return;
  }

  let i = 0;
  while (i < lines.length) {
    const locationStart = i;

    while (lines[i] !== SEMICOLON) {
      i++;
    }

    // Get the location name.
    const location = decoder.decode(lines.subarray(locationStart, i));

    // Process the temperature
    let temp: number;
    let negator = 1;

    // We don't need to do anything with the semicolon, so we just skip it.
    i++;

    // But we want to process what comes after.
    if (lines[i] === MINUS) {
      negator = -1;
      i++;
    }

    // After the semicolon and the optional minus, there must be a number that is not zero.
    // The number is actually a char so we can convert it to it's integer value by subtraction.
    temp = lines[i] - ZERO;
    i++;

    // If the next char is not a dot, we have another number we need to process.
    if (lines[i] !== DOT) {
      temp = temp * 10 + (lines[i] - ZERO);
      i++;
    }

    // Here is the dot, we don't need to process it.
    i++;

    // Add the digit after the dot.
    temp = temp * 10 + (lines[i] - ZERO);
    i++;

    // Make sure the number has the correct sign.
    temp = temp * negator;

    if (locationMap.has(location)) {
      const locationData = locationMap.get(location)!;
      locationData.measurementCount++;
      locationData.measurementSum += temp;
      locationData.maxTemperature = Math.max(locationData.maxTemperature, temp);
      locationData.minTemperature = Math.min(locationData.minTemperature, temp);
    } else {
      locationMap.set(location, {
        measurementCount: 1,
        measurementSum: temp,
        minTemperature: temp,
        maxTemperature: temp,
      } as LocationData);
    }

    // The next char is a line break. We skip that.
    i++;
  }
};
