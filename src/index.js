function* generate(row, scope, steps) {
  if (steps.length === 0) {
    return yield row;
  }

  const [step, ...remainingSteps] = steps;

  if (isColumnDefinition(step)) {
    const key = Object.keys(step)[0];
    const data = step[key];
    yield* generate(row, scope, [col(key, data), ...remainingSteps]);

  } else if (isIterable(step)) {
    yield* generate(row, scope, [forEach(step), ...remainingSteps]);

  } else if (typeof step === "function") {
    yield* step(row, scope, remainingSteps);

  } else {
    throw "Unknown step type: " + JSON.stringify(step);
  }
}

function isIterable(obj) {
  // checks for null and undefined
  if (obj === null || obj === undefined) return false;
  return typeof obj[Symbol.iterator] === 'function';
}

function makeIterable(obj) {
  if (isIterable(obj)) return obj;
  return [obj];
}

function isColumnDefinition(step) {
  return (typeof step === "object")
    && Object.keys(step).length === 1;
}

function cartesian(...steps) {
  if (steps.length === 0) return [];
  return generate({}, {}, steps);
}

function col(key, data) {
  return function* (row, scope, remainingSteps) {
    // apply function with scope instead of row, since after "group" the row is empty
    const resolvedData = typeof data === "function" ? data(scope) : data;
    const iterableData = makeIterable(resolvedData);

    for (const value of iterableData) {
      const newRow = { ...row, [key]: value };
      const newScope = { ...scope, [key]: value };

      yield* generate(newRow, newScope, remainingSteps);
    }
  }
}

function forEach(data) {
  return function* (row, scope, remainingSteps) {
    for (const valueObj of data) {
      if (typeof valueObj === "object") {
        const newRow = { ...row, ...valueObj };
        const newScope = { ...scope, ...valueObj };

        yield* generate(newRow, newScope, remainingSteps);
      } else {
        throw "If a step is an array, it must be an array of objects." + JSON.stringify(value);
      }
    }
  }
}

function filter(filterFn) {
  return function* (row, scope, remainingSteps) {
    if (filterFn(row)) {
      return yield* generate(row, scope, remainingSteps);
    }
  }
}

function group(columnName) {
  return function* (row, scope, remainingSteps) {
    return yield {
      ...row,
      [columnName]: Array.from(generate({}, scope, remainingSteps))
    };
  }
}

module.exports = {
  cartesian,
  filter,
  group,
  col,
  forEach
};
