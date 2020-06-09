import {generate} from "./internal";

export function cartesianGenerator(...steps) {
  if (steps.length === 0) return [];
  return generate({}, {}, steps);
}

export function cartesian(...steps) {
  return Array.from(cartesianGenerator(...steps));
}
