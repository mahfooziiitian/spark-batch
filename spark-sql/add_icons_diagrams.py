#!/usr/bin/env python3
"""Add Material icons and Mermaid diagrams to Spark SQL docs."""

import re
import os

BASE = "/home/malam/development/processing/batch/spark-batch/spark-sql"

# ---------------------------------------------------------------------------
# Mermaid diagram definitions
# ---------------------------------------------------------------------------
D_JOIN_INDEX = """\
```mermaid
graph TD
    A[":material-link: Join"] --> B["Inner :material-set-center:"]
    A --> C["Left Outer :material-set-left:"]
    A --> D["Right Outer :material-set-right:"]
    A --> E["Full Outer :material-set-all:"]
    A --> F["Left Semi"]
    A --> G["Left Anti"]
    A --> H["Cross :material-grid:"]
```"""

D_JOIN_CONDITION = """\
```mermaid
graph LR
    A[Left Table] -- matching key --> C{Join Condition}
    B[Right Table] -- matching key --> C
    C -->|matched| D[Result Set]
```"""

D_BHJ = """\
```mermaid
graph LR
    A[Driver] -->|broadcast small table| B[Executor 1]
    A -->|broadcast small table| C[Executor 2]
    B --> D[Hash Join in Memory]
    C --> D
```"""

D_SMJ = """\
```mermaid
graph LR
    A[Left Table] --> B[Sort on Key]
    C[Right Table] --> D[Sort on Key]
    B --> E[Merge Join]
    D --> E
    E --> F[Result]
```"""

D_SKEW = """\
```mermaid
graph LR
    A[Skewed Partition] -->|AQE detects| B[Split into Sub-partitions]
    B --> C[Balanced Execution]
```"""

D_UDF = """\
```mermaid
graph LR
    A[Python / Scala Function] --> B[Register as UDF]
    B --> C["spark.sql('SELECT my_udf(col)')"]
    C --> D[Result]
```"""

D_MACRO = """\
```mermaid
graph LR
    A[SQL Expression] --> B[CREATE TEMPORARY MACRO]
    B --> C[Inline Expansion at Compile Time]
    C --> D[Catalyst Optimized Result]
```"""

D_STRUCTURE = """\
```mermaid
graph LR
    A[Raw String Column] --> B["from_json / from_csv / from_xml"]
    B --> C[Struct / Array Column]
```"""

# ---------------------------------------------------------------------------
# Scalar function diagrams
# ---------------------------------------------------------------------------
D_SCALAR_INDEX = """\
```mermaid
graph TD
    A[":material-function: Scalar Functions"] --> B["String :material-text:"]
    A --> C["DateTime :material-calendar-clock:"]
    A --> D["Math :material-calculator:"]
    A --> E["NULL :material-null:"]
    A --> F["Regex :material-regex:"]
    A --> G["Encryption :material-shield-lock:"]
    A --> H["Type Conversion :material-swap-horizontal:"]
```"""

D_DATETIME = """\
```mermaid
graph LR
    A[Input Date / Timestamp] --> B[DateTime Function]
    B --> C[Result Value]
```"""

D_ENCRYPTION = """\
```mermaid
graph LR
    A[Plain Text] --> B[Hash Function]
    B --> C[Hash Output]
```"""

D_REGEX = """\
```mermaid
graph LR
    A[Input String] --> B[Regex Pattern]
    B --> C{Match?}
    C -->|Yes| D[Extract / Replace]
    C -->|No| E[NULL / Original]
```"""

D_STRING = """\
```mermaid
graph LR
    A[Input String] --> B[String Function]
    B --> C[Transformed String]
```"""

D_MATH = """\
```mermaid
graph LR
    A[Numeric Input] --> B[Math Function]
    B --> C[Numeric Result]
```"""

D_NULL_HANDLING = """\
```mermaid
graph LR
    A[Value] --> B{IS NULL?}
    B -->|Yes| C[Return Default / NULL]
    B -->|No| D[Return Value]
```"""

D_PREDICATE = """\
```mermaid
graph LR
    A[Input Value] --> B{Predicate Check}
    B -->|TRUE| C[Include Row]
    B -->|FALSE| D[Exclude Row]
```"""

D_CONVERSION = """\
```mermaid
graph LR
    A[Source Type] --> B[CAST / TRY_CAST]
    B --> C[Target Type]
```"""

D_BITWISE = """\
```mermaid
graph LR
    A[Integer A] --> B[Bitwise Operation]
    C[Integer B] --> B
    B --> D[Result Integer]
```"""

D_CONTROL = """\
```mermaid
graph LR
    A[Condition] --> B{CASE / IF}
    B -->|TRUE| C[THEN Result]
    B -->|FALSE| D[ELSE Result]
```"""

D_WEB = """\
```mermaid
graph LR
    A[URL String] --> B[parse_url]
    B --> C[URL Component]
```"""

D_COUNT = """\
```mermaid
graph LR
    A[Table Rows] --> B[COUNT Function]
    B --> C[Integer Count]
```"""

D_ACL = """\
```mermaid
graph LR
    A[User / Role] --> B{Permission Check}
    B -->|GRANTED| C[Access Allowed]
    B -->|DENIED| D[Access Denied]
```"""

# ---------------------------------------------------------------------------
# Function index / lambda diagrams
# ---------------------------------------------------------------------------
D_FUNC_INDEX = """\
```mermaid
graph TD
    A[":material-function: Spark SQL Functions"] --> B["Scalar :material-function:"]
    A --> C["Aggregate :material-sigma:"]
    A --> D["Window :material-window-shutter:"]
    A --> E["Generator :material-expand-all:"]
    A --> F["HOF :material-lambda:"]
    A --> G["UDF :material-code-braces-box:"]
```"""

D_LAMBDA = """\
```mermaid
graph LR
    A[Array / Map] --> B["Lambda: x -> expression"]
    B --> C[HOF Result]
```"""

# ---------------------------------------------------------------------------
# Aggregate function diagrams
# ---------------------------------------------------------------------------
D_AGG_INDEX = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[Aggregate Function]
    C --> D[One Row per Group]
```"""

D_AGG_SIMPLE = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[MIN / MAX / SUM / AVG]
    C --> D[One Row per Group]
```"""

D_AGG_STATS = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[Statistical Function]
    C --> D[One Row per Group]
```"""

D_AGG_ARRAY = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[ARRAY_AGG]
    C --> D[One Row per Group]
```"""

D_AGG_COUNT = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[COUNT / COUNT_IF]
    C --> D[One Row per Group]
```"""

D_AGG_EVERY = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[EVERY / BOOL_AND]
    C --> D[One Row per Group]
```"""

D_AGG_FIRST = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[FIRST / FIRST_VALUE]
    C --> D[One Row per Group]
```"""

D_AGG_GROUP = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[GROUPING / GROUPING_ID]
    C --> D[One Row per Group]
```"""

D_AGG_LAST = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[LAST / LAST_VALUE]
    C --> D[One Row per Group]
```"""

D_AGG_LIST = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[COLLECT_LIST]
    C --> D[One Row per Group]
```"""

D_AGG_MAP = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[Map Aggregate Functions]
    C --> D[One Row per Group]
```"""

D_AGG_SET = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[COLLECT_SET]
    C --> D[One Row per Group]
```"""

D_AGG_STRINGS = """\
```mermaid
graph LR
    A[Input Rows] --> B[GROUP BY]
    B --> C[String Aggregate Functions]
    C --> D[One Row per Group]
```"""

# ---------------------------------------------------------------------------
# Collection function diagrams
# ---------------------------------------------------------------------------
D_COLL_INDEX = """\
```mermaid
graph LR
    A[Input Data] --> B[Collection Function]
    B --> C[Array / Map / Struct]
```"""

D_COLL_ARRAY = """\
```mermaid
graph LR
    A[Input] --> B[Array Functions]
    B --> C[Transformed Array]
```"""

D_COLL_LIST = """\
```mermaid
graph LR
    A[Input] --> B[COLLECT_LIST / ARRAY_AGG]
    B --> C[Ordered Array]
```"""

D_COLL_MAP = """\
```mermaid
graph LR
    A[Keys and Values] --> B[Map Functions]
    B --> C[Map Type]
```"""

D_COLL_SET = """\
```mermaid
graph LR
    A[Input] --> B[COLLECT_SET]
    B --> C[Distinct Array]
```"""

D_COLL_STRUCT = """\
```mermaid
graph LR
    A[Named Fields] --> B[Struct Functions]
    B --> C[Struct Type]
```"""

# ---------------------------------------------------------------------------
# Generator function diagrams
# ---------------------------------------------------------------------------
D_GEN_INDEX = """\
```mermaid
graph LR
    A[Single Row] --> B[Generator Function]
    B --> C[Row 1]
    B --> D[Row 2]
    B --> E[Row N]
```"""

D_EXPLODE = """\
```mermaid
graph LR
    A["Row with ARRAY[a,b,c]"] --> B[EXPLODE]
    B --> C[Row: a]
    B --> D[Row: b]
    B --> E[Row: c]
```"""

D_EXPLODE_OUTER = """\
```mermaid
graph LR
    A["Row with ARRAY[a,b,c]"] --> B[EXPLODE_OUTER]
    B --> C[Row: a]
    B --> D[Row: b]
    B --> E[Row: c]
```"""

D_INLINE = """\
```mermaid
graph LR
    A[Single Row] --> B[INLINE]
    B --> C[Row 1]
    B --> D[Row 2]
    B --> E[Row N]
```"""

D_POSEXPLODE = """\
```mermaid
graph LR
    A[Single Row] --> B[POSEXPLODE]
    B --> C[Row 1]
    B --> D[Row 2]
    B --> E[Row N]
```"""

D_POSEXPLODE_OUTER = """\
```mermaid
graph LR
    A[Single Row] --> B[POSEXPLODE_OUTER]
    B --> C[Row 1]
    B --> D[Row 2]
    B --> E[Row N]
```"""

D_STACK = """\
```mermaid
graph LR
    A[Single Row] --> B[STACK]
    B --> C[Row 1]
    B --> D[Row 2]
    B --> E[Row N]
```"""

# ---------------------------------------------------------------------------
# Higher-order function diagram
# ---------------------------------------------------------------------------
D_HOF = """\
```mermaid
graph LR
    A[Input Array] --> B["Lambda: element -> expression"]
    B --> C[Output Array / Scalar]
```"""

# ---------------------------------------------------------------------------
# File list: (relative_path, icon, diagram, special_stop_or_None)
# special_stop: if set, only stop when this exact line is encountered
# ---------------------------------------------------------------------------
FILES = [
    # function/udf
    ("docs/function/udf/index.md",  ":material-code-braces-box:", D_UDF,  None),
    ("docs/function/udf/udf.md",    ":material-code-braces-box:", D_UDF,  None),
    # function/macro
    ("docs/function/macro/intro.md", ":material-code-json:", D_MACRO, None),
    ("docs/function/macro/macro.md", ":material-code-json:", D_MACRO, None),
    # function/structure
    ("docs/function/structure/index.md", ":material-file-code:", D_STRUCTURE, None),
    ("docs/function/structure/csv.md",   ":material-file-code:", D_STRUCTURE, None),
    ("docs/function/structure/json.md",  ":material-file-code:", D_STRUCTURE, None),
    ("docs/function/structure/xml.md",   ":material-file-code:", D_STRUCTURE, None),
    # join index
    ("docs/join/index.md",      ":material-link:", D_JOIN_INDEX, None),
    ("docs/join/expression.md", ":material-link:", D_JOIN_CONDITION, None),
    # join types
    ("docs/join/types/index.md",      ":material-link:",       D_JOIN_CONDITION, None),
    ("docs/join/types/inner_join.md", ":material-set-center:", D_JOIN_CONDITION, None),
    ("docs/join/types/left_anti.md",  ":material-set-left:",   D_JOIN_CONDITION, None),
    ("docs/join/types/left_semi.md",  ":material-set-left:",   D_JOIN_CONDITION, None),
    ("docs/join/types/cartesian.md",  ":material-grid:",       D_JOIN_CONDITION, None),
    # outer joins
    ("docs/join/types/outer/index.md", ":material-set-all:", D_JOIN_CONDITION, None),
    ("docs/join/types/outer/left.md",  ":material-set-all:", D_JOIN_CONDITION, None),
    ("docs/join/types/outer/right.md", ":material-set-all:", D_JOIN_CONDITION, None),
    ("docs/join/types/outer/full.md",  ":material-set-all:", D_JOIN_CONDITION, None),
    # non-equi joins
    ("docs/join/types/non_equi_join/index.md",                            ":material-not-equal:", D_JOIN_CONDITION, None),
    ("docs/join/types/non_equi_join/range_join/index.md",                 ":material-not-equal:", D_JOIN_CONDITION, None),
    ("docs/join/types/non_equi_join/range_join/interval_overlap.md",      ":material-not-equal:", D_JOIN_CONDITION, None),
    ("docs/join/types/non_equi_join/range_join/point_in_interval.md",     ":material-not-equal:", D_JOIN_CONDITION, None),
    # strategies
    ("docs/join/strategy/index.md", ":material-cog-transfer:", D_BHJ, "### Flow"),
    ("docs/join/strategy/bhj.md",   ":material-cog-transfer:", D_BHJ, None),
    ("docs/join/strategy/smj.md",   ":material-cog-transfer:", D_SMJ, None),
    ("docs/join/strategy/bnlj.md",  ":material-cog-transfer:", D_BHJ, None),
    ("docs/join/strategy/hj.md",    ":material-cog-transfer:", D_BHJ, None),
    ("docs/join/strategy/shj.md",   ":material-cog-transfer:", D_BHJ, None),
    ("docs/join/strategy/srnlp.md", ":material-cog-transfer:", D_BHJ, None),
    ("docs/join/strategy/ssmj.md",  ":material-cog-transfer:", D_BHJ, None),
    # hints
    ("docs/join/hints/index.md",          ":material-lightbulb-on:", D_BHJ, None),
    ("docs/join/hints/operator.md",        ":material-lightbulb-on:", D_BHJ, None),
    ("docs/join/hints/range_join_hint.md", ":material-lightbulb-on:", D_BHJ, None),
    ("docs/join/hints/resolver.md",        ":material-lightbulb-on:", D_BHJ, None),
    # issues
    ("docs/join/issues/index.md",          ":material-alert-circle:", D_JOIN_CONDITION, None),
    ("docs/join/issues/column_duplicate.md", ":material-alert-circle:", D_JOIN_CONDITION, None),
    # optimization
    ("docs/join/optimization/index.md", ":material-speedometer:", D_JOIN_CONDITION, None),
    # skewjoin
    ("docs/join/optimization/skewjoin/index.md",        ":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/aqe.md",          ":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/bmpj.md",         ":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/bucketting.md",   ":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/caching.md",      ":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/ib.md",           ":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/separation.md",   ":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/salting/index.md",":material-scale-unbalanced:", D_SKEW, None),
    ("docs/join/optimization/skewjoin/salting/smj.md",  ":material-scale-unbalanced:", D_SKEW, None),
    # scalar index
    ("docs/function/scalar/index.md", ":material-function:", D_SCALAR_INDEX, None),
    # scalar/datetime/
    ("docs/function/scalar/datetime/index.md",       ":material-calendar-clock:", D_DATETIME, None),
    ("docs/function/scalar/datetime/interval.md",    ":material-calendar-clock:", D_DATETIME, None),
    ("docs/function/scalar/datetime/timestamp.md",   ":material-calendar-clock:", D_DATETIME, None),
    ("docs/function/scalar/datetime/timezone.md",    ":material-calendar-clock:", D_DATETIME, None),
    # scalar/datetime/date/
    ("docs/function/scalar/datetime/date/index.md",        ":material-calendar-clock:", D_DATETIME, None),
    ("docs/function/scalar/datetime/date/add_subtract.md", ":material-calendar-clock:", D_DATETIME, None),
    ("docs/function/scalar/datetime/date/difference.md",   ":material-calendar-clock:", D_DATETIME, None),
    ("docs/function/scalar/datetime/date/parts.md",        ":material-calendar-clock:", D_DATETIME, None),
    ("docs/function/scalar/datetime/date/truncate.md",     ":material-calendar-clock:", D_DATETIME, None),
    # scalar/encryption/
    ("docs/function/scalar/encryption/index.md", ":material-shield-lock:", D_ENCRYPTION, None),
    ("docs/function/scalar/encryption/intro.md", ":material-shield-lock:", D_ENCRYPTION, None),
    ("docs/function/scalar/encryption/crc32.md", ":material-shield-lock:", D_ENCRYPTION, None),
    ("docs/function/scalar/encryption/hex.md",   ":material-shield-lock:", D_ENCRYPTION, None),
    ("docs/function/scalar/encryption/mask.md",  ":material-shield-lock:", D_ENCRYPTION, None),
    ("docs/function/scalar/encryption/md5.md",   ":material-shield-lock:", D_ENCRYPTION, None),
    ("docs/function/scalar/encryption/sha.md",   ":material-shield-lock:", D_ENCRYPTION, None),
    # scalar/regex/
    ("docs/function/scalar/regex/index.md",                ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/regex_count.md",          ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/regex_extract.md",        ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/regex_instr.md",          ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/regex_like.md",           ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/regexp_function.md",      ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/regexp_replace_function.md", ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/regexp_substr_function.md",  ":material-regex:", D_REGEX, None),
    ("docs/function/scalar/regex/rlike_ilike_function.md", ":material-regex:", D_REGEX, None),
    # scalar/*.md
    ("docs/function/scalar/string.md",     ":material-text:",            D_STRING,       None),
    ("docs/function/scalar/math.md",       ":material-calculator:",      D_MATH,         None),
    ("docs/function/scalar/null.md",       ":material-null:",            D_NULL_HANDLING, None),
    ("docs/function/scalar/predicate.md",  ":material-check-circle:",    D_PREDICATE,    None),
    ("docs/function/scalar/conversion.md", ":material-swap-horizontal:", D_CONVERSION,   None),
    ("docs/function/scalar/bitwise.md",    ":material-chip:",            D_BITWISE,      None),
    ("docs/function/scalar/control.md",    ":material-source-branch:",   D_CONTROL,      None),
    ("docs/function/scalar/web.md",        ":material-web:",             D_WEB,          None),
    ("docs/function/scalar/count.md",      ":material-counter:",         D_COUNT,        None),
    ("docs/function/scalar/acl.md",        ":material-lock:",            D_ACL,          None),
    # function index + lambda
    ("docs/function/index.md",  ":material-function:", D_FUNC_INDEX, None),
    ("docs/function/lambda.md", ":material-lambda:",   D_LAMBDA,     None),
    # aggregate
    ("docs/function/aggregate/index.md",   ":material-sigma:", D_AGG_INDEX,   None),
    ("docs/function/aggregate/simple.md",  ":material-sigma:", D_AGG_SIMPLE,  None),
    ("docs/function/aggregate/stats.md",   ":material-sigma:", D_AGG_STATS,   None),
    ("docs/function/aggregate/array.md",   ":material-sigma:", D_AGG_ARRAY,   None),
    ("docs/function/aggregate/count.md",   ":material-sigma:", D_AGG_COUNT,   None),
    ("docs/function/aggregate/every.md",   ":material-sigma:", D_AGG_EVERY,   None),
    ("docs/function/aggregate/first.md",   ":material-sigma:", D_AGG_FIRST,   None),
    ("docs/function/aggregate/group.md",   ":material-sigma:", D_AGG_GROUP,   None),
    ("docs/function/aggregate/last.md",    ":material-sigma:", D_AGG_LAST,    None),
    ("docs/function/aggregate/list.md",    ":material-sigma:", D_AGG_LIST,    None),
    ("docs/function/aggregate/map.md",     ":material-sigma:", D_AGG_MAP,     None),
    ("docs/function/aggregate/set.md",     ":material-sigma:", D_AGG_SET,     None),
    ("docs/function/aggregate/strings.md", ":material-sigma:", D_AGG_STRINGS, None),
    # collection
    ("docs/function/collection/index.md",  ":material-format-list-bulleted:", D_COLL_INDEX,  None),
    ("docs/function/collection/array.md",  ":material-code-array:",           D_COLL_ARRAY,  None),
    ("docs/function/collection/list.md",   ":material-format-list-bulleted:", D_COLL_LIST,   None),
    ("docs/function/collection/map.md",    ":material-map:",                  D_COLL_MAP,    None),
    ("docs/function/collection/set.md",    ":material-format-list-bulleted:", D_COLL_SET,    None),
    ("docs/function/collection/struct.md", ":material-code-braces:",          D_COLL_STRUCT, None),
    # generator
    ("docs/function/generator/index.md",          ":material-expand-all:", D_GEN_INDEX,      None),
    ("docs/function/generator/explode.md",        ":material-expand-all:", D_EXPLODE,         None),
    ("docs/function/generator/explode_outer.md",  ":material-expand-all:", D_EXPLODE_OUTER,   None),
    ("docs/function/generator/inline.md",         ":material-table-row:",  D_INLINE,          None),
    ("docs/function/generator/posexplode.md",     ":material-table-row:",  D_POSEXPLODE,      None),
    ("docs/function/generator/posexplode_outer.md", ":material-table-row:", D_POSEXPLODE_OUTER, None),
    ("docs/function/generator/stack.md",          ":material-table-row:",  D_STACK,           None),
    # hof
    ("docs/function/hof/index.md",     ":material-lambda:", D_HOF, None),
    ("docs/function/hof/aggregate.md", ":material-lambda:", D_HOF, None),
    ("docs/function/hof/array.md",     ":material-lambda:", D_HOF, None),
    ("docs/function/hof/exists.md",    ":material-lambda:", D_HOF, None),
    ("docs/function/hof/filter.md",    ":material-lambda:", D_HOF, None),
    ("docs/function/hof/map.md",       ":material-lambda:", D_HOF, None),
    ("docs/function/hof/transform.md", ":material-lambda:", D_HOF, None),
]


def find_insertion_point(lines, h1_index, special_stop=None):
    """Return the line index *before* which the Overview section should be inserted."""
    i = h1_index + 1
    in_code_block = False

    while i < len(lines):
        line = lines[i]

        # Track fenced code blocks so we don't misidentify --- or ## inside them
        if line.startswith("```"):
            in_code_block = not in_code_block

        if in_code_block:
            i += 1
            continue

        if special_stop:
            # Only stop on the exact special pattern (ignore ## and --- )
            if line.rstrip("\n") == special_stop:
                return i
        else:
            if line.startswith("---"):
                return i
            if re.match(r"^## ", line):
                return i

        i += 1

    # -----------------------------------------------------------------------
    # Fallback: no primary stop found.
    # Insert after the first complete paragraph (before the blank line that
    # follows it).  This handles files with no --- or ## near the top.
    # -----------------------------------------------------------------------
    i = h1_index + 1
    # skip leading blank lines
    while i < len(lines) and lines[i].strip() == "":
        i += 1
    # consume the first paragraph (non-blank lines)
    while i < len(lines) and lines[i].strip() != "":
        i += 1
    # we are now pointing at the blank line *after* the first paragraph
    return i


def process_file(filepath, icon, diagram, special_stop=None):
    full_path = os.path.join(BASE, filepath)

    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Skip files that already have the Overview section
    if "### :material-sitemap: Overview" in content:
        print(f"SKIP  {filepath}")
        return

    lines = content.split("\n")

    # Locate H1
    h1_index = None
    for i, line in enumerate(lines):
        if re.match(r"^# ", line):
            h1_index = i
            break

    if h1_index is None:
        print(f"WARN  No H1 found in {filepath}")
        return

    # --- Add icon to H1 if not already present ---
    h1_line = lines[h1_index]
    if icon not in h1_line:
        # Insert icon right after "# "
        h1_line = re.sub(r"^# ", f"# {icon} ", h1_line)
        lines[h1_index] = h1_line

    # --- Find where to insert ---
    insert_at = find_insertion_point(lines, h1_index, special_stop)

    # Build overview block to splice in
    # Ensure there is exactly one blank line before the heading
    overview_str = f"### :material-sitemap: Overview\n\n{diagram}\n"
    overview_lines = overview_str.split("\n")

    # If the line just before insert_at is not blank, prepend a blank line
    if insert_at > 0 and lines[insert_at - 1].strip() != "":
        overview_lines = [""] + overview_lines

    lines = lines[:insert_at] + overview_lines + lines[insert_at:]

    new_content = "\n".join(lines)

    with open(full_path, "w", encoding="utf-8") as f:
        f.write(new_content)

    print(f"OK    {filepath}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
for filepath, icon, diagram, special_stop in FILES:
    process_file(filepath, icon, diagram, special_stop)

print("\nDone.")
