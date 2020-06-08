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
    yield* generate(row, scope, [join(step), ...remainingSteps]);

  } else if (typeof step === "function") {
    yield* step(row, scope, remainingSteps);

  } else {
    throw "Unknown step type: " + JSON.stringify(step);
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

function isColumnDefinition(step) {
  return (typeof step === "object")
    && Object.keys(step).length === 1;
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

function join(data, joinCondition) {
  return function* (row, scope, remainingSteps) {

    let joinFn = (_) => true;

    if (typeof joinCondition === "string") {
      const colToJoin = joinCondition

      joinFn = (valueObj) => scope.hasOwnProperty(colToJoin)
        && valueObj.hasOwnProperty(colToJoin)
        && scope[colToJoin] === valueObj[colToJoin];

    } else if (typeof joinCondition === "function") {
      joinFn = joinCondition;
    }


    for (const valueObj of data) {
      if (typeof valueObj === "object") {
        if (joinFn(valueObj)) {
          const newRow = { ...row, ...valueObj };
          const newScope = { ...scope, ...valueObj };

          yield* generate(newRow, newScope, remainingSteps);
        }


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
