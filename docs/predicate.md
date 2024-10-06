# Predicate pushdown support

## Predicate structure

Currently, VAST Database allows pushing down filter predicate `p` of the following logical structure:

- An `AND` between multiple column predicates:
  `p = p1 & p2 & p3`
- Within each column predicate `pi`, an `OR` between range predicates:
  `ci = r1 | r2 | r3 | r4`
- Each range predicate `rj` is one of the following operators (where `"x"` is a column name, and `c` is a constant):
  - comparison operator:
    - `t["x"] < c`
    - `t["x"] <= c`
    - `t["x"] == c`
    - `t["x"] > c`
    - `t["x"] >= c`
    - `t["x"] != c`
  - `is_in` operator:
    - `t["x"].isin([c1, c2, c3, c4, c5])`
  - NULL checking:
    - `t["x"].isnull()`
    - `~t["x"].isnull()`
  - Prefix match:
    - `t["x"].startswith("prefix")`
  - Substring match:
    - `t["x"].contains("substr")`


## Column types

The following Arrow types support predicate pushdown:
  - `int8`
  - `int16`
  - `int32`
  - `int64`
  - `float32`
  - `float64`
  - `utf8`
  - `bool`
  - `decimal128`
  - `binary`
  - `date32`
  - `time32`
  - `time64`
  - `timestamp`
