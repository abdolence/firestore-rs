# Reads, joins and subqueries

This page lists what a SQL `SELECT` can read and how, from the `FROM` clause to joins and their
cost. Setup, parameters and the read-only options are in [SQL](../sql.md), and the results that
differ from a SQL database in [Query and write semantics](../editions/semantics.md).

## Sources

The `FROM` clause names where the documents come from:

| Source | Reads |
|---|---|
| ``FROM books`` or ``FROM `books` `` | a root collection; backticks hold an ID with hyphens or dots |
| `` FROM `restaurants/rest-01/reviews` `` | the collection under a document |
| `FROM collection_group('reviews')` | every collection with that ID, anywhere in the database |
| `FROM documents('books/b1', 'books/b2')` | the documents at those paths, relative to the documents root; a path that names no document is left out |
| `FROM database()` | every document of the database |
| `FROM subcollection('reviews')` | the collection under the current document, inside a [subquery or join](#joins-and-subqueries) |
| `FROM (SELECT ..) AS t` | a derived table: the rows of another `SELECT` |
| `FROM cte` | a [common table expression](#joins-and-subqueries) |

A table alias is written `FROM books AS b`. A name that is defined by `WITH` matches without regard
to case, as a table alias does, and hides a collection of the same name.

## Reads by ID

A `WHERE` on `__name__`, `__name__ = 'book-01'`, `__name__ IN ('book-01', 'book-02')` or
`__name__ IN UNNEST(@ids)`, reads each document by its name: the plan is a lookup of the named
documents. A bare ID is a document of the `FROM` collection, and a path such as
`books/book-01` is accepted too.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let rows = db
    .fluent()
    .sql("SELECT title FROM books WHERE __name__ IN UNNEST(@ids)")
    .param("ids", ["book-01", "book-02"])
    .query()
    .await?;
# let _ = rows;
# Ok(())
# }
```

A read by ID costs 1 read unit for each document of up to 4 KiB, as a Core `get` does, see
[Query plans and cost](../editions/query-plans.md#enterprise-edition).

A name compared with a column, such as the `__name__` of a joined table, is described in
[Join cost](#join-cost).

## Supported constructs

| Construct | Notes |
|---|---|
| `SELECT` list | columns, `AS` aliases, computed columns with an alias, `*` and `* EXCEPT (a, b)`; nested fields as `awards.hugo`, `awards['nebula']` and `tags[OFFSET(0)]` |
| `WHERE` | comparisons, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `IS [NOT] DISTINCT FROM`, `[NOT] IN (list)`, `IN UNNEST(array)`, `BETWEEN`, `LIKE`, `ILIKE`, `CASE`, `COALESCE`, `IFNULL` |
| `ORDER BY`, `LIMIT`, `OFFSET` | `ASC`, `DESC`, `NULLS FIRST`, `NULLS LAST`; `LIMIT` and `OFFSET` take an integer or a parameter |
| `GROUP BY`, `HAVING` | columns and expressions; `HAVING` may use an aggregate that the select list does not hold |
| Aggregates | `COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNTIF`, `ARRAY_AGG`, `ARRAY_AGG(DISTINCT x)`, `FIRST`, `LAST` |
| `SELECT DISTINCT` | over the selected columns |
| `UNION [ALL]`, `UNION DISTINCT` | each branch a plain `SELECT` with the same column names in the same order, or all `*` with `UNION ALL`; the query's `ORDER BY` and `LIMIT` apply to the union |
| Text and geo search | `document_matches('query')` and `geo_distance(field, geo_point(lat, lng)) <= meters` as top-level `AND` conditions of the `WHERE`, see [Search](#search) |
| Subqueries | scalar `(SELECT ..)`, `[NOT] EXISTS`, `[NOT] IN (SELECT ..)` and `ARRAY(SELECT ..)`, see [Joins and subqueries](#joins-and-subqueries) |
| Derived tables, `WITH` | non-recursive CTEs |
| `TABLESAMPLE` | `BERNOULLI (p PERCENT)` on a table, see [Sampling](#sampling) |
| Joins | `[INNER] JOIN`, `LEFT JOIN`, `CROSS JOIN`, a comma in `FROM`, and `UNNEST(array) AS x [WITH OFFSET AS pos]` |
| Writes | `INSERT .. VALUES`, `INSERT .. SELECT`, `INSERT OR REPLACE`, `INSERT .. ON CONFLICT`, `UPDATE`, `DELETE`, see [Writes](../sql.md#writes) |
| Operators | `+ - * / %`, `||` |
| Functions | any other call passes to the pipeline function of that name, such as `starts_with(title, @text)` or `char_length(title)`; Firestore rejects a name it does not have with `INVALID_ARGUMENT` |

SQL names that differ from the pipeline function are renamed: `LOWER` to `to_lower`, `UPPER` to
`to_upper`, `POWER` to `pow`, `IFNULL` to `if_null`, `REGEXP_CONTAINS` to `regex_contains`,
`AVG` to `average`, `MIN` to `minimum`, `MAX` to `maximum`, `COUNTIF` to `count_if`.
`SUBSTR`, `SUBSTRING` and `STRPOS` count from 1 as in SQL, and `ILIKE` compares both sides in lower
case. A `SUBSTR` position of 0 or below passes to Firestore as written: 0 is the whole string, and a
negative position counts from the end with -1 the last character, as in SQL. `ORDINAL(0)` reads
the first element, and a negative `ORDINAL` or `OFFSET` counts from the end, where SQL fails or
gives `NULL`. `NULLS FIRST` and `NULLS LAST` add a leading sort key that tests for an absent or null
value, since Firestore sorts both first in an ascending order.

### Search

The search is part of the source, so the translator lifts it out of the `WHERE`: one
`document_matches('query')` or one `geo_distance(..) <= meters` that is a top-level `AND`
condition, on a collection or collection group source and before any join. `score() AS relevance`
selects the relevance, and `ORDER BY score() DESC` or `ORDER BY geo_distance(..)` as the only sort
key becomes the search's own sort. A search needs a search index, see
[Search indexes](../pipeline-search.md#search-indexes).

### Sampling

`TABLESAMPLE BERNOULLI (p PERCENT)` keeps each document with probability `p / 100`, independently
of the others, so the count varies from run to run, and the input order is kept. *Bernoulli
sampling* decides for each item separately, as a coin toss does. It becomes the `sample` stage, see
[Sampling](../pipeline-aggregations-and-joins.md#sampling) in the pipeline chapter.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let about_a_tenth = db
    .fluent()
    .sql("SELECT title FROM books AS b TABLESAMPLE BERNOULLI (10 PERCENT) WHERE b.rating > 4")
    .query()
    .await?;
# let _ = about_a_tenth;
# Ok(())
# }
```

As in SQL, the sample is drawn from the whole table before `WHERE`, so the `WHERE` filters the
documents that were drawn: in the example, each document that matches is in the result with
probability 0.1. The clause follows the table and its alias. `p` is a literal or a parameter,
`@name` or `?`. Firestore refuses a value outside 0 to 100.

`TABLESAMPLE` is refused with `InvalidParametersError` in these places:

- a derived table, a joined table and a subquery, and a derived table or a CTE that holds a
  `TABLESAMPLE` and is read by a join or a subquery. Firestore runs a join and a subquery once for
  each row of the outer query, which would draw the sample again every time;
- `UPDATE` and `DELETE`;
- a statement with a search, `document_matches(..)` or `geo_distance(..)`, which reads the
  collection directly and cannot follow a sample.

A CTE that holds a `TABLESAMPLE` draws a new sample at each reference, since a CTE referenced twice
runs twice.

These forms are refused too, since no sampling of the stage matches them:

- `SYSTEM` and `BLOCK`, since Firestore has no block sampling;
- `REPEATABLE` and `SEED`, since the stage takes no seed and every run draws a new sample;
- `BUCKET` and the `SAMPLE` keyword;
- a sample with `ROWS`, or with no unit or method.

`RESERVOIR (n ROWS)` does not parse. A fixed number of documents is
`.sample(FirestoreSampleSize::Documents(n))` in the [pipeline API](../pipeline-aggregations-and-joins.md#sampling).

### Refused constructs

A construct with no pipeline form fails with `InvalidParametersError` that names it, before any
request. Anything in a parsed statement that the translator does not map is refused and never
dropped, so a statement does not run with a clause missing.

| Construct | Reason |
|---|---|
| Pipe syntax, `FROM t \|> WHERE ..` | not supported in this version |
| `MERGE`, `REPLACE INTO` | `MERGE` has no pipeline form, and `REPLACE INTO` does not parse in the BigQuery dialect; use `INSERT OR REPLACE` or `INSERT .. ON CONFLICT` |
| `INSERT OR IGNORE`, `OR ABORT`, `OR FAIL`, `OR ROLLBACK`, `RETURNING`, `UPDATE .. FROM`, `ORDER BY` or `LIMIT` on a write, `INSERT .. SET`, `INSERT .. DEFAULT VALUES` | a write has no pipeline form for them |
| DDL, `BEGIN`, `EXPLAIN` | a statement is a `SELECT`, `INSERT`, `UPDATE` or `DELETE`; transactions come from the API, and `.explain()` returns the plan |
| `WITH RECURSIVE`, a CTE column list, `MATERIALIZED` | pipelines have no iteration |
| Window functions, `QUALIFY`, `WINDOW` | pipelines have no window stage |
| `INTERSECT`, `EXCEPT`, `UNION BY NAME` | pipelines have no set stage for them |
| `RIGHT JOIN`, `FULL JOIN` | no pipeline form keeps the unmatched rows of the joined side |
| `JOIN .. USING`, `NATURAL JOIN`, a join without `ON`, `LATERAL` | write the condition in `ON` |
| `CAST`, `SAFE_CAST`, typed literals such as `TIMESTAMP '..'`, `INTERVAL`, `STRUCT` | no pipeline function converts a type; bind a typed parameter instead |
| `DISTINCT ON`, `GROUP BY ALL`, `ROLLUP`, `CUBE`, `GROUPING SETS` | no pipeline stage keeps one row per key or groups in these ways |
| `ANY_VALUE`, `STRING_AGG`, `LOGICAL_AND`, `LOGICAL_OR` | no accumulator with that meaning |
| `LIKE .. ESCAPE`, `LIKE ANY` | a backslash escapes in `LIKE` without it |
| `FOR UPDATE`, `FETCH`, `SELECT AS STRUCT` and the other clauses of other dialects | no pipeline form |
| Searches inside `OR` or after a join | Firestore accepts a search only right after the source, and fails the request otherwise |
| An unaliased computed column, or an unaliased nested field such as `awards.hugo` in a query that groups, `ORDER BY` or `GROUP BY` a constant or a position | the column needs a name, since a nested path cannot name an output field; a constant orders and groups nothing |

## Joins and subqueries

Joins and subqueries are in Preview in Firestore, see
[Pipeline aggregations and joins](../pipeline-aggregations-and-joins.md#joins). Firestore plans a
join as a nested loop: for each row of the left side it runs the right side as a subquery. A
subquery in an expression, `[NOT] EXISTS`, `[NOT] IN (SELECT ..)`, `(SELECT ..)` or
`ARRAY(SELECT ..)`, runs once per row of the query it is in too, whether or not it reads a column of
that row, so the rows scanned multiply. Rows scanned and read units on an Enterprise edition
database, for an outer collection of 11 documents and an inner one of 300, as Query Explain with
`analyze` reports them:

| Statement | Rows scanned | Read units |
|---|---|---|
| `SELECT title FROM outer` | 11 | 1 |
| `SELECT n FROM inner` | 300 | 7 |
| `SELECT title FROM outer WHERE n IN (SELECT n FROM inner)` | 3,311 (11 + 11 × 300) | 77 |
| `SELECT title FROM outer WHERE (SELECT COUNT(*) AS c FROM inner) > 0` | 3,311 | 27 |
| `SELECT title FROM outer WHERE EXISTS (SELECT 1 FROM inner WHERE n > 100000)` | 3,311 | 77 |

The plan of each subquery statement is an `Apply` of the outer scan to the subquery, so even an
uncorrelated `COUNT(*)` runs once for each outer row.

The cost depends on the index. Without one on the join key, each run reads the whole joined
collection. A join or subquery is therefore refused unless the options say `.allow_subqueries()`,
and the error says it runs one subquery per row, see
[Read-only options](../sql.md#read-only-options). These need no opt-in:

- a join or subquery into `subcollection(..)`, which reads only the children of each parent;
- a derived table or a CTE in the `FROM` of a query, which is the query's own source and does not
  multiply with the rows; a `JOIN` into one is still a join and needs the opt-in;
- `UNNEST` of an array of the row.

```rust,no_run
# use firestore::*;
# async fn example(db: FirestoreDb) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let rows = db
    .fluent()
    .sql(
        "SELECT r.name, v.rating, v.reviewer \
         FROM restaurants AS r LEFT JOIN collection_group('reviews') AS v ON v.restaurant = r.__name__ \
         WHERE r.cuisine = @cuisine",
    )
    .param("cuisine", "breakfast")
    .options(FirestoreSqlOptions::new().allow_subqueries())
    .query()
    .await?;
# let _ = rows;
# Ok(())
# }
```

Rules for the joined names:

- a qualified name, `v.rating`, is a column of the table or alias `v`, searched from the nearest
  query outwards, and a table or alias matches without regard to case; an alias used by two tables
  is an error;
- a joined table lives in a field named after its alias, and a document can hold a field of any
  name, so the translation cannot tell whether the alias is the name of a field of the left side.
  It refuses every way to read such a field instead. In `FROM orders AS o JOIN customers AS
  customer`, `o.customer` is refused, and so is a bare `customer` in any place but `customer IS
  NULL`, since the field `customer` of the orders is replaced by the joined table. Give the table
  another alias. A bare alias of an `UNNEST` names its element, and is allowed;
- in a query with a join or in a subquery, an unqualified name in an `ON` clause is an error, since
  either side could hold it, and so is a dotted name that starts with no table;
- `SELECT *` and `b.*` are refused in a query with a join, for the base table as well as for a
  joined one. The joined tables would come with the base document's fields, and replace any of them
  that has the name of an alias. Name the columns;
- the anti-join, the left rows that have no match, is `WHERE v IS NULL` on the alias.
  `WHERE v.rating IS NULL` matches nothing, since the field of an unmatched row is absent and not
  null;
- a *correlated* subquery, one that reads a column of the row it runs for, cannot sit in a query
  that groups, or in a clause with no stage before it to carry the column, such as the `SET` of an
  `UPDATE`;
- `EXISTS (SELECT 1 ..)` ignores the select list, so an aggregate, `GROUP BY` or `HAVING` inside
  `EXISTS` is refused: it would change which rows exist, and the select list that holds it is not
  evaluated.

A CTE, a named subquery defined with `WITH`, runs again for each place that uses it: a CTE
referenced twice runs twice. Results are not kept or reused between references. A statement that
repeats CTE bodies more than 1,024 times in all is refused.

### Join cost

Rows scanned and read units for some joins on an Enterprise edition database, as Query Explain
with `analyze` reports them, over small collections: 9 restaurants with 6 reviews, 11 books and
1,500 items.

| Join | Rows scanned | Read units | Results |
|---|---|---|---|
| 9 restaurants to the `reviews` collection group by a reference field, no index | 63 (9 + 9 × 6) | 3 | 6 |
| the same, for the 3 restaurants with `cuisine = 'breakfast'` | 27 (9 + 3 × 6) | 2 | 3 |
| 9 restaurants to `subcollection('reviews')` | 15 (9 + 6) | 1 | 6 |
| 11 books to books on `genre`, with the index `(genre ASC, rating DESC)` | 26 (11 + 15) | 17 | 15 |
| 20 of 1,500 items to the same 1,500 items on `n`, no index | 31,500 (1,500 + 20 × 1,500) | 864 | 20 |

The unindexed join scans the joined collection once for each left row: parents × child collection.
The indexed join reads an index range for each left row, and a join into `subcollection(..)` reads
each parent's children only.

Rules that follow from it:

- index the join key, and run `.explain()` before the join reaches production data. A plan with a
  `TableScan` under the join is the unindexed case, see
  [Query plans and cost](../editions/query-plans.md#recognising-a-full-scan);
- a join that compares the joined table's `__name__` with a column of the left row becomes a lookup
  by name for each left row. A plain `=` of `__name__` with a variable in a per-row subquery scans
  the collection instead: over a collection of 60 documents, run for 3 rows, it scans up to 180
  rows, where the lookup by name reads 2;
- a join over a large collection that is run for analysis belongs in BigQuery. Firestore joins fit
  a left side that a filter has narrowed to a few documents.
