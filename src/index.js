function* generate(row, scope, steps) {
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

function isIterable(obj) {
  // checks for null and undefined
  if (obj === null || obj === undefined) return false;
  if (typeof obj === "string") return false;
  return typeof obj[Symbol.iterator] === 'function';
}

function makeIterable(obj) {
  if (isIterable(obj)) return obj;
  return [obj];
}

function cartesianGenerator(...steps) {
  if (steps.length === 0) return [];
  return generate({}, {}, steps);
}

function cartesian(...steps) {
  return Array.from(cartesianGenerator(...steps));
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

/**
 * Given an array of objects as data, merge the current row with each object in the data
 * to form a new row.
 *
 * @param data
 * @param mergeCondition
 * @returns {function(...[*]=)}
 */
function merge(data, mergeCondition = () => true) {
  return function* (row, scope, remainingSteps) {
    for (const objToMerge of data) {
      if (typeof objToMerge === "object") {
        if (mergeCondition(scope, objToMerge)) {
          const newRow = { ...row, ...objToMerge };
          const newScope = { ...scope, ...objToMerge };

          yield* generate(newRow, newScope, remainingSteps);
        }
      } else {
        throw "Only object is allowed in arrays." + JSON.stringify(value);
      }
    }
  }
}

/**
 * Join is merge, based on a equal column between the current row and data.
 *
 * @param data
 * @param column
 * @returns {function(...[*]=)}
 */
function join(data, column) {
  const joinFn = (scope, objToMerge) =>
    scope.hasOwnProperty(column) && objToMerge.hasOwnProperty(column)
    && scope[column] === objToMerge[column];

  return merge(data, joinFn);
}

function filter(filterFn) {
  return function* (row, scope, remainingSteps) {
    if (filterFn(row)) {
      return yield* generate(row, scope, remainingSteps);
    }
  }
}

function group(columnName, ...steps) {
  return function* (row, scope, remainingSteps) {
    const nestedRow = {
      ...row,
      [columnName]: Array.from(generate({}, scope, steps))
    };

    yield* generate(nestedRow, scope, remainingSteps);
  }
}

function select(...cols) {
  return function* (row, scope, remainingSteps) {
    const newRow = {};
    const newScope = {};

    for (const col of cols) {
      if (row.hasOwnProperty(col)) {
        newRow[col] = row[col];
        newScope[col] = scope[col];
      }
    }

    return yield* generate(newRow, newScope, remainingSteps);
  }
}

function discard(...cols) {
  return function* (row, scope, remainingSteps) {
    const newRow = { ...row };
    const newScope = { ...scope };

    for (const col of cols) {
      if (row.hasOwnProperty(col)) {
        delete newRow[col];
      }

      if (scope.hasOwnProperty(col)) {
        delete newScope[col];
      }
    }

    return yield* generate(newRow, newScope, remainingSteps);
  }
}

module.exports = {
  cartesian, cartesianGenerator,
  filter,
  group,
  join,
  select,
  discard
};
