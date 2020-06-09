import {col, merge} from "./index";

export function isIterable(obj) {
  // checks for null and undefined
  if (obj === null || obj === undefined) return false;
  if (typeof obj === "string") return false;
  return typeof obj[Symbol.iterator] === 'function';
}

export function makeIterable(obj) {
  if (isIterable(obj)) return obj;
  return [obj];
}

export function* generate(row, scope, steps) {
  if (steps.length === 0) {
    return yield row;
  }

  const [step, ...remainingSteps] = steps;

  if (isIterable(step)) { // array of objects
    yield* generate(row, scope, [merge(step), ...remainingSteps]);

  } else if (typeof step === "object") {
    const colSteps = Object.entries(step).map(
      ([key, data]) => col(key, data)
    );

    yield* generate(row, scope, [...colSteps, ...remainingSteps]);

  } else if (typeof step === "function") {
    yield* step(row, scope, remainingSteps);

  } else {
    throw "Unknown step type."
  }
}

