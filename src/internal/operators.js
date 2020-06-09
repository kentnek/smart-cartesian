import {generate, makeIterable} from "./index";

export function col(key, data) {
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
export function merge(data, mergeCondition = () => true) {
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
export function join(data, column) {
  const joinFn = (scope, objToMerge) =>
    scope.hasOwnProperty(column) && objToMerge.hasOwnProperty(column)
    && scope[column] === objToMerge[column];

  return merge(data, joinFn);
}

export function filter(filterFn) {
  return function* (row, scope, remainingSteps) {
    if (filterFn(row)) {
      return yield* generate(row, scope, remainingSteps);
    }
  }
}

export function group(columnName, ...steps) {
  return function* (row, scope, remainingSteps) {
    const nestedRow = {
      ...row,
      [columnName]: Array.from(generate({}, scope, steps))
    };

    yield* generate(nestedRow, scope, remainingSteps);
  }
}

export function select(...cols) {
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

export function discard(...cols) {
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
