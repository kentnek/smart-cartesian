const chai = require("chai");
const expect = chai.expect;
const { cartesian, filter, group, select, discard, join } = require("../src/index");

function test(title, input, expected) {
  const actual = cartesian(...input);

  return it(
    title,
    () => expect(actual).to.deep.equal(expected)
  );
}

function testError(title, input, exception) {
  const actual = cartesian(...input);

  return it(
    title,
    () => expect(actual).to.throw(exception)
  )
}

describe('Add column from array', () => {
  test("empty input", [], []);

  test(
    "one array",
    [
      { key: [1, 2, 3] }
    ],
    [
      { key: 1 },
      { key: 2 },
      { key: 3 }
    ]
  );

  test(
    "two arrays",
    [
      { a: [1, 2, 3] },
      { b: ['x', 'y'] },
    ],
    [
      { a: 1, b: 'x' },
      { a: 1, b: 'y' },
      { a: 2, b: 'x' },
      { a: 2, b: 'y' },
      { a: 3, b: 'x' },
      { a: 3, b: 'y' },
    ]
  );

  test(
    "three arrays, in one object",
    [{
      a: [1, 2],
      b: ['x', 'y'],
      c: [30, 40]
    }],
    [
      { a: 1, b: 'x', c: 30 },
      { a: 1, b: 'x', c: 40 },
      { a: 1, b: 'y', c: 30 },
      { a: 1, b: 'y', c: 40 },
      { a: 2, b: 'x', c: 30 },
      { a: 2, b: 'x', c: 40 },
      { a: 2, b: 'y', c: 30 },
      { a: 2, b: 'y', c: 40 },
    ]
  );
});

describe('Add column from function', () => {
  test(
    "function returns single value",
    [
      { a: [1, 2, 3] },
      { b: row => row.a * 10 }
    ],
    [
      { a: 1, b: 10 },
      { a: 2, b: 20 },
      { a: 3, b: 30 }
    ]
  );

  test(
    "function returns array",
    [
      { a: [1, 2, 3] },
      { b: row => ["x" + row.a, "y" + row.a] }
    ],
    [
      { a: 1, b: "x1" },
      { a: 1, b: "y1" },
      { a: 2, b: "x2" },
      { a: 2, b: "y2" },
      { a: 3, b: "x3" },
      { a: 3, b: "y3" }
    ]
  );
});

describe('Iterate from existing array of objects', () => {
  const data = cartesian({
    a: [1, 2, 3],
    b: [4, 5, 6]
  });

  test(
    "filter on one array",
    [
      data,
      { product: row => row.a * row.b }
    ],
    [
      { a: 1, b: 4, product: 4 },
      { a: 1, b: 5, product: 5 },
      { a: 1, b: 6, product: 6 },

      { a: 2, b: 4, product: 8 },
      { a: 2, b: 5, product: 10 },
      { a: 2, b: 6, product: 12 },

      { a: 3, b: 4, product: 12 },
      { a: 3, b: 5, product: 15 },
      { a: 3, b: 6, product: 18 },
    ]
  );

  const nestedData = [
    { a: 1, b: ['a', 'b', 'c'] },
    { a: 2, b: ['d', 'e', 'f'] },
  ]

  test(
    "flatten nested data",
    [
      nestedData,
      { b: row => row.b }
    ],
    [
      { a: 1, b: 'a' },
      { a: 1, b: 'b' },
      { a: 1, b: 'c' },
      { a: 2, b: 'd' },
      { a: 2, b: 'e' },
      { a: 2, b: 'f' },
    ]
  );
});

describe('Joins', () => {
  test(
    "join on single column",

    [
      [
        { a: 1, b: 'x' },
        { a: 2, b: 'y' },
        { a: 3, b: 'z' },
      ],
      join(
        [
          { b: 'y', c: 10 },
          { b: 'y', c: 20 },
          { b: 'z', c: 30 }
        ],
        "b"
      )
    ],

    [
      { a: 2, b: 'y', c: 10 },
      { a: 2, b: 'y', c: 20 },
      { a: 3, b: 'z', c: 30 },
    ]
  )
});

describe('Filters', () => {
  test(
    "filter on one array",
    [
      { a: [1, 2, 3, 4, 5, 6] },
      filter(row => row.a % 2 === 0)
    ],
    [
      { a: 2 },
      { a: 4 },
      { a: 6 }
    ]
  );

  test(
    "filter on two arrays",
    [
      { a: [1, 2, 3] },
      { b: [4, 5, 6] },
      filter(row => (row.a + row.b) % 3 === 0)
    ],
    [
      { a: 1, b: 5 },
      { a: 2, b: 4 },
      { a: 3, b: 6 },
    ]
  );

  test(
    "filter out all",
    [
      { a: [1, 2, 3] },
      { b: [4, 5, 6] },
      filter(row => (row.a + row.b) > 10)
    ],
    []
  )
});

describe('Groups', () => {
  test(
    "one group",
    [
      { a: [1, 2] },
      { b: ['x', 'y'] },

      group("c",
        { d: [4, 5] }
      ),
    ],
    [
      {
        a: 1,
        b: 'x',
        c: [{ d: 4 }, { d: 5 }]
      },
      {
        a: 1,
        b: 'y',
        c: [{ d: 4 }, { d: 5 }]
      },
      {
        a: 2,
        b: 'x',
        c: [{ d: 4 }, { d: 5 }]
      },
      {
        a: 2,
        b: 'y',
        c: [{ d: 4 }, { d: 5 }]
      },
    ]
  );

  test(
    "two groups",
    [
      { a: [1, 2] },
      { b: ['x', 'y'] },

      group("c",
        { d: [4, 5] }
      ),

      group("e",
        { f: [6, 7] }
      ),
    ],
    [
      {
        a: 1,
        b: 'x',
        c: [{ d: 4 }, { d: 5 }],
        e: [{ f: 6 }, { f: 7 }]
      },
      {
        a: 1,
        b: 'y',
        c: [{ d: 4 }, { d: 5 }],
        e: [{ f: 6 }, { f: 7 }]
      },
      {
        a: 2,
        b: 'x',
        c: [{ d: 4 }, { d: 5 }],
        e: [{ f: 6 }, { f: 7 }]
      },
      {
        a: 2,
        b: 'y',
        c: [{ d: 4 }, { d: 5 }],
        e: [{ f: 6 }, { f: 7 }]
      },
    ]);
});

describe('Select and Discard', () => {

  test("select columns",
    [
      { a: [1, 2] },
      { b: [3, 4] },
      { c: row => row.b * 2 },
      select("a", "c"),
    ],
    [
      { "a": 1, "c": 6 },
      { "a": 1, "c": 8 },
      { "a": 2, "c": 6 },
      { "a": 2, "c": 8 },
    ]
  );

  test("discard columns",
    [
      { a: [1, 2] },
      { b: [3, 4] },
      { c: row => row.b * 2 },
      discard("b"),
    ],
    [
      { "a": 1, "c": 6 },
      { "a": 1, "c": 8 },
      { "a": 2, "c": 6 },
      { "a": 2, "c": 8 },
    ]
  );
});

describe('Mixed use cases', () => {
  test(
    "filter then add column from function",
    [
      { a: [1, 2, 3] },
      { b: [4, 5, 6] },
      filter(row => (row.a + row.b) % 3 === 0),
      { product: row => row.a * row.b }
    ],
    [
      { a: 1, b: 5, product: 5 },
      { a: 2, b: 4, product: 8 },
      { a: 3, b: 6, product: 18 },
    ]
  );

  test(
    "filter then group",
    [
      { a: [1, 2, 3], b: [4, 5, 6] },
      filter(row => (row.a + row.b) % 3 === 0),
      { c: ["x", "y"] },
      group("d",
        { e: row => [row.c + row.a, row.c + row.b] }
      ),
    ],
    [
      { a: 1, b: 5, c: "x", d: [{ e: "x1" }, { e: "x5" }] },
      { a: 1, b: 5, c: "y", d: [{ e: "y1" }, { e: "y5" }] },
      { a: 2, b: 4, c: "x", d: [{ e: "x2" }, { e: "x4" }] },
      { a: 2, b: 4, c: "y", d: [{ e: "y2" }, { e: "y4" }] },
      { a: 3, b: 6, c: "x", d: [{ e: "x3" }, { e: "x6" }] },
      { a: 3, b: 6, c: "y", d: [{ e: "y3" }, { e: "y6" }] },
    ]
  );
});

