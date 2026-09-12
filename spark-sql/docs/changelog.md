# :material-history: Changelog

History of notable changes to this project, generated from `git log`.

______________________________________________________________________

## 2026-09-11

- `83ccd68e` build(spark-sql): add cross-platform justfile mirroring the Makefile
- `1517a9ad` docs(copilot): mandate pytest-mock (mocker) over unittest.mock
- `034450be` docs(copilot): align .github instructions with the real project
- `c98e7474` test(spark-sql): scope coverage to the tested SQL-helper library
- `ee25888e` fix(spark-sql): make 'make secure' pass (bandit + safety)
- `1b6793cd` perf(spark-sql): speed up test Spark session
- `0e83a65d` perf(spark-sql): parallelize sqlfluff and add changed-only SQL targets
- `8959599e` chore(spark-sql): clean up dependency metadata
- `7cf72d00` fix(spark-sql): remove duplicate pytest.fixture decorator on spark fixture
- `dd95a89d` fix(spark-sql): resolve REPO_ROOT to repo root so tests find sql/ files
- `3a852a6d` build(spark-sql): fix Makefile target bugs, deps, and add missing target
- `aa98ba10` style(sql): resolve LT09 sqlfluff crash on SELECT * FROM VALUES blocks
- `ff912c1d` build(spark-sql): resolve uv binary robustly in Makefile
- `e36c2645` docs(data-sources): expand thin Hive integration pages
- `46e24aa3` docs(internals): expand thin Query Planner page
- `0b1000d1` docs(optimization): expand thin Spark SQL AST page
- `404c95b1` docs: group top-bar tabs into logical sections
- `b483cce6` docs(patterns): fold duplicated primitive examples into canonical pages
- `8cc47bc0` docs(patterns): de-duplicate taxonomy — one source of truth per concept
- `f0ab7c6c` docs(joins): add join troubleshooting suite and enhance join type pages
- `bdb2ab34` mcp

## 2026-09-04

- `470e08f4` spark sql

## 2026-09-03

- `62005ec0` sopark sql

## 2026-09-01

- `75969cf8` spark sql

## 2026-08-28

- `70540cdf` spark sql

## 2026-07-29

- `c54fc2f4` spark sql window functions

## 2026-07-28

- `ba67d113` timie binning

## 2026-07-17

- `705150b6` spark sql

## 2026-07-15

- `7f54b107` spark sql
- `620a2cb1` docs: add grouped navigation to mkdocs.yml
- `6262611e` docs: enhance documentation with Spark 4.0 features
- `65564bce` feat: merge src folder from v3 branch into master
- `263e711a` docs: replace Sphinx with MkDocs for spark-sql
- `00e8430c` docs: bring all documentation from v3 branch

## 2025-08-25

- `ca4106c0` spark sql join
- `becb94f6` spark sql hof

## 2025-08-13

- `98e05c64` sql

## 2025-08-12

- `b2441619` control structure

## 2025-08-11

- `50117f25` dbx-sql

## 2025-08-06

- `f9123515` spark sql

## 2025-08-02

- `812ed985` spark join

## 2025-08-01

- `17199323` dbx-sql

## 2025-07-30

- `cc298f04` spark sql docs

## 2025-07-22

- `0dee7c2b` session
- `dbb1eb7b` scd 2

## 2025-07-21

- `4ade9224` spark sql

## 2025-03-18

- `e7fa3a55` spark batch

## 2025-03-14

- `2941e565` spark batch setup
