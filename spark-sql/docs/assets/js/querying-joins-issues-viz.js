/**
 * querying-joins-issues-viz.js
 * D3 v7 interactive visualizations for docs/querying/joins/issues/*.md.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 */
(function () {
  "use strict";

  const PURPLE = "#7c4dff";
  const TEAL = "#26a69a";
  const AMBER = "#ffa726";
  const RED = "#ef5350";
  const GRAY = "#90a4ae";
  const FG = "#546e7a";
  const HIT_COLOR = "#26a69a";

  function chip(parent, text, opts) {
    opts = opts || {};
    return parent.append("div")
      .style("display", "inline-flex").style("align-items", "center")
      .style("padding", "5px 12px").style("border-radius", "5px")
      .style("font-size", "0.74rem").style("font-family", "monospace")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .text(text);
  }

  function btnGroup(container, options, onSelect) {
    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem").style("flex-wrap", "wrap");
    let current = options[0][0];
    function refresh() {
      ctrl.selectAll("button").each(function () {
        const active = d3.select(this).attr("data-v") === current;
        d3.select(this).style("background", active ? PURPLE : "#fff").style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
    }
    options.forEach(([v, label]) => {
      ctrl.append("button").attr("data-v", v)
        .style("padding", "5px 14px").style("border-radius", "5px")
        .style("cursor", "pointer").style("font-size", "0.76rem")
        .text(label)
        .on("click", () => { current = v; refresh(); onSelect(v); });
    });
    refresh();
    return { get: () => current };
  }

  function box(parent, text, opts) {
    opts = opts || {};
    return parent.append("pre")
      .style("margin", "0").style("padding", "8px 10px")
      .style("background", opts.bg || "#f5f5f7")
      .style("border", `1px solid ${opts.border || "#ddd"}`)
      .style("border-radius", "5px")
      .style("font-size", "0.72rem").style("font-family", "monospace")
      .style("white-space", "pre-wrap").style("color", opts.fg || FG)
      .text(text);
  }

  function table(parent, cols, rows) {
    const wrap = parent.append("div").style("overflow-x", "auto");
    const tbl = wrap.append("table")
      .style("width", "100%")
      .style("border-collapse", "collapse")
      .style("font-size", "0.75rem");
    const border = "#dde3e6";

    tbl.append("thead").append("tr")
      .selectAll("th").data(cols).join("th")
      .style("text-align", "left")
      .style("padding", "5px 8px")
      .style("border-bottom", `2px solid ${border}`)
      .style("color", FG)
      .text(d => d);

    tbl.append("tbody").selectAll("tr").data(rows).join("tr")
      .selectAll("td").data(d => d).join("td")
      .style("padding", "5px 8px")
      .style("border-bottom", `1px solid ${border}`)
      .style("font-family", d => typeof d === "number" ? "monospace" : "inherit")
      .style("color", d => d === "NULL" ? RED : FG)
      .style("font-style", d => d === "NULL" ? "italic" : "normal")
      .text(d => d);
  }

  function renderPanel(stage, mode) {
    if (mode.metrics && mode.metrics.length) {
      const row = stage.append("div")
        .style("display", "flex")
        .style("gap", "6px")
        .style("flex-wrap", "wrap")
        .style("margin-bottom", "8px");
      mode.metrics.forEach((m) => chip(row, m.text, m.opts));
    }

    if (mode.query) {
      stage.append("div")
        .style("font-size", "0.7rem")
        .style("font-weight", "700")
        .style("color", FG)
        .style("margin-bottom", "4px")
        .text(mode.queryLabel || "Query / plan fragment");
      box(stage, mode.query, { bg: "#f8f9fb", border: "#d7dde2" }).style("margin-bottom", "8px");
    }

    if (mode.tableCols && mode.tableRows) {
      table(stage, mode.tableCols, mode.tableRows);
    }

    if (mode.note) {
      stage.append("div")
        .style("margin-top", "8px")
        .style("font-size", "0.72rem")
        .style("color", GRAY)
        .html(mode.note);
    }
  }

  function renderIssueViz(el, spec) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const head = container.append("div").style("margin-bottom", "8px");
    head.append("div")
      .style("font-weight", "700")
      .style("color", spec.color || PURPLE)
      .style("margin-bottom", spec.summary && spec.summary.length ? "6px" : "0")
      .text(spec.title);

    if (spec.summary && spec.summary.length) {
      const row = head.append("div")
        .style("display", "flex")
        .style("gap", "6px")
        .style("flex-wrap", "wrap");
      spec.summary.forEach((m) => chip(row, m.text, m.opts));
    }

    const stage = container.append("div");
    if (spec.modes && spec.modes.length > 1) {
      const group = btnGroup(container, spec.modes.map((m) => [m.id, m.label]), render);
      function render(id) {
        stage.selectAll("*").remove();
        const mode = spec.modes.find((m) => m.id === id) || spec.modes[0];
        renderPanel(stage, mode);
      }
      render(group.get());
    } else {
      renderPanel(stage, spec.modes[0]);
    }
  }

  const green = { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR };
  const amber = { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER };
  const purple = { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE };
  const red = { bg: "rgba(239,83,80,0.10)", fg: RED, border: RED };
  const gray = { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY };

  const SPECS = {
    "viz-joins-issues-index": {
      title: "Issue map: correctness first, performance second",
      color: PURPLE,
      summary: [
        { text: "silent correctness", opts: red },
        { text: "planner / execution", opts: purple },
        { text: "Spark 4.2 verified", opts: green },
      ],
      modes: [{
        id: "map",
        label: "Issue map",
        tableCols: ["Cluster", "Examples", "Typical signal"],
        tableRows: [
          ["Missing / wrong matches", "NULLs, type mismatch, case/space, floating point", "row drops with no error"],
          ["Too many rows", "fan-out, duplicate-key, incomplete predicate", "COUNT(*) grows"],
          ["Wrong outer semantics", "ON vs WHERE, post-join filter", "LEFT behaves like INNER"],
          ["Slow physical plan", "broadcast, range, skew, partitions, join order", "BNLJ / shuffle / stragglers"],
        ],
        note: "The section index is a routing map: prove <strong>correctness</strong> first, then inspect the <strong>physical plan</strong>."
      }]
    },
    "viz-joins-issues-broadcast-pitfalls": {
      title: "Broadcast join strategy switch",
      color: PURPLE,
      summary: [
        { text: "threshold = 10485760b", opts: purple },
        { text: "default → broadcast", opts: green },
        { text: "threshold=-1 → shuffle", opts: amber },
      ],
      modes: [
        {
          id: "auto",
          label: "Default threshold",
          query: "EXPLAIN SELECT f.id, d.region_name\nFROM big_fact f JOIN region_dim d ON f.region = d.region\n=> BroadcastHashJoin + BroadcastExchange",
          metrics: [{ text: "fast path", opts: green }],
          note: "Spark 4.2 auto-broadcasted the small side under the default 10 MB threshold."
        },
        {
          id: "off",
          label: "Broadcast disabled",
          query: "SET spark.sql.autoBroadcastJoinThreshold = -1\nEXPLAIN SELECT ...\n=> SortMergeJoin + Exchange hashpartitioning(...) on both sides",
          metrics: [{ text: "safer at scale", opts: amber }],
          note: "Turning broadcast off is a quick way to confirm whether a stale broadcast choice is the real problem."
        }
      ]
    },
    "viz-joins-issues-cartesian-join": {
      title: "Cartesian blow-up",
      color: RED,
      summary: [
        { text: "3 rows × 3 rows = 9", opts: red },
        { text: "crossJoin.enabled=true", opts: purple },
      ],
      modes: [
        {
          id: "bad",
          label: "No ON clause",
          query: "SELECT o.order_id, c.name\nFROM orders o JOIN customers c\n=> every order pairs with every customer",
          tableCols: ["order_id", "name"],
          tableRows: [[1, "Alice"], [1, "Bob"], [1, "Charlie"], [2, "Alice"]],
          note: "Spark 4.2 accepted the implicit cross join because <code>spark.sql.crossJoin.enabled</code> defaults to <code>true</code>; small inputs planned as <code>BroadcastNestedLoopJoin</code>."
        },
        {
          id: "good",
          label: "Correct ON clause",
          query: "SELECT o.order_id, c.name\nFROM orders o JOIN customers c ON o.customer_id = c.customer_id",
          tableCols: ["order_id", "name"],
          tableRows: [[1, "Alice"], [2, "Bob"], [3, "Charlie"]],
          note: "Adding the equi-key collapses the result back to one customer per order."
        }
      ]
    },
    "viz-joins-issues-case-whitespace-mismatch": {
      title: "String keys: raw vs normalized",
      color: PURPLE,
      summary: [
        { text: "raw_len 18 → trimmed_len 17", opts: amber },
        { text: "case mismatch", opts: red },
      ],
      modes: [
        {
          id: "raw",
          label: "Naive =",
          query: "LEFT JOIN marketing_opt_in m ON c.email = m.email",
          tableCols: ["customer_id", "email", "opted_in"],
          tableRows: [[1, "alice@example.com", true], [2, "BOB@example.com", "NULL"], [3, "carol@example.com ", "NULL"]],
          note: "Spark 4.2 matched only Alice; Bob differed by case, Carol by a trailing space."
        },
        {
          id: "norm",
          label: "LOWER(TRIM())",
          query: "LEFT JOIN marketing_opt_in m\n  ON LOWER(TRIM(c.email)) = LOWER(TRIM(m.email))",
          tableCols: ["customer_id", "email", "opted_in"],
          tableRows: [[1, "alice@example.com", true], [2, "BOB@example.com", true], [3, "carol@example.com ", false]],
          note: "Normalization fixes correctness, but repeated function-wrapped joins still pay a planning and scan cost."
        }
      ]
    },
    "viz-joins-issues-column-duplicate": {
      title: "Duplicate names stay duplicated",
      color: AMBER,
      summary: [
        { text: "schema: id, customer_id, amount, id, name", opts: amber },
        { text: "AMBIGUOUS_REFERENCE", opts: red },
      ],
      modes: [
        {
          id: "star",
          label: "SELECT *",
          query: "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id",
          tableCols: ["columns returned"],
          tableRows: [["id | customer_id | amount | id | name"]],
          note: "Spark SQL keeps both columns; it does <strong>not</strong> silently overwrite one of them."
        },
        {
          id: "alias",
          label: "Aliased select",
          query: "SELECT o.id AS order_id, c.id AS customer_id, c.name\nFROM orders o JOIN customers c ON o.customer_id = c.id",
          tableCols: ["order_id", "customer_id", "name"],
          tableRows: [[1, 101, "Alice"], [2, 102, "Bob"]],
          note: "Unqualified <code>SELECT id</code> raised Spark 4.2's <code>[AMBIGUOUS_REFERENCE]</code> error until the columns were renamed."
        }
      ]
    },
    "viz-joins-issues-data-explosion": {
      title: "One-sided fan-out",
      color: RED,
      summary: [
        { text: "order 1 → 3 payment rows", opts: red },
        { text: "SUM(amount)=1235.5", opts: amber },
      ],
      modes: [
        {
          id: "fanout",
          label: "Raw join",
          query: "JOIN payments p ON o.order_id = p.order_id",
          tableCols: ["order_id", "amount", "payment_status"],
          tableRows: [[1, 250.0, "AUTHORIZED"], [1, 250.0, "CAPTURED"], [1, 250.0, "REFUNDED"], [2, 175.5, "CAPTURED"]],
          note: "Only the right side duplicates here, so the blow-up is additive per left row rather than multiplicative across both sides."
        },
        {
          id: "dedup",
          label: "Pre-aggregate / dedup",
          query: "Reduce payments to one row per order before joining",
          metrics: [{ text: "expected total = 735.5", opts: green }],
          note: "Choosing one payment row per order, or summarizing first, restores one output row per order."
        }
      ]
    },
    "viz-joins-issues-debugging-workflow": {
      title: "15-step workflow, compressed",
      color: PURPLE,
      summary: [
        { text: "steps 1–7 = correctness", opts: red },
        { text: "steps 8–15 = performance", opts: purple },
      ],
      modes: [
        {
          id: "correctness",
          label: "Correctness first",
          tableCols: ["Step cluster", "Check"],
          tableRows: [["Key validity", "business key, NULLs, types"], ["Cardinality", "duplicates, fan-out, cross joins"], ["Outer semantics", "ON vs WHERE, preserved side"]],
          note: "These checks prevent silent wrong answers."
        },
        {
          id: "perf",
          label: "Then performance",
          tableCols: ["Step cluster", "Check"],
          tableRows: [["Plan", "EXPLAIN FORMATTED, join strategy"], ["Distribution", "shuffle partitions, skew"], ["Planner choices", "broadcast, AQE, join order"]],
          note: "Only optimize once the output rows are already proven correct."
        }
      ]
    },
    "viz-joins-issues-duplicate-key-explosion": {
      title: "N × M explosion on both sides",
      color: RED,
      summary: [
        { text: "s1: 3 clicks × 2 purchases = 6", opts: red },
        { text: "theoretical total = 7", opts: purple },
      ],
      modes: [
        {
          id: "math",
          label: "Multiplication",
          query: "SUM(left.cnt * right.cnt) predicts the exact join row count before materializing it.",
          tableCols: ["session_id", "left cnt", "right cnt", "rows after join"],
          tableRows: [["s1", 3, 2, 6], ["s2", 1, 1, 1]],
          note: "Spark 4.2 returned 7 joined rows, exactly matching the per-key multiplication."
        },
        {
          id: "fix",
          label: "One row per key",
          query: "Pick one purchase row with ROW_NUMBER() before joining",
          tableCols: ["session_id", "last_click_ts", "purchase_ts", "item"],
          tableRows: [["s1", "10:02", "10:05", "shoes"], ["s2", "11:00", "11:05", "hat"]],
          note: "The repaired example uses a deterministic first purchase row; it no longer mixes <code>MIN(purchase_ts)</code> with an unrelated <code>MIN(item)</code>."
        }
      ]
    },
    "viz-joins-issues-floating-point-join": {
      title: "Binary floating-point mismatch",
      color: PURPLE,
      summary: [
        { text: "0.1 + 0.2 = 0.30000000000000004", opts: red },
        { text: "epsilon join recovers match", opts: green },
      ],
      modes: [
        {
          id: "eq",
          label: "Equality join",
          query: "LEFT JOIN readings_b b ON a.val = b.val",
          tableCols: ["id", "val_a", "val_b"],
          tableRows: [[1, "0.30000000000000004", "NULL"], [2, "1.0", "1.0"], [3, "3.3000001", "NULL"]],
          note: "Spark 4.2 printed the full 17-digit mismatch exactly as expected."
        },
        {
          id: "eps",
          label: "Tolerance join",
          query: "LEFT JOIN readings_b b ON ABS(a.val - b.val) < 0.0001",
          tableCols: ["id", "val_a", "val_b"],
          tableRows: [[1, "0.30000000000000004", "0.3"], [2, "1.0", "1.0"], [3, "3.3000001", "3.3"]],
          note: "Use an epsilon or exact <code>DECIMAL</code> storage when equality must be reliable."
        }
      ]
    },
    "viz-joins-issues-functions-on-join-keys": {
      title: "Function-wrapped key: correctness vs pruning",
      color: AMBER,
      summary: [
        { text: "raw key → PartitionFilters(region = east)", opts: green },
        { text: "UPPER(key) → dynamic pruning only", opts: amber },
      ],
      modes: [
        {
          id: "raw",
          label: "Raw partition key",
          query: "PartitionFilters: [(region = east), isnotnull(region)]\nPushedFilters on dim: [IsNotNull(region), EqualTo(region,east)]",
          note: "Spark 4.2 pruned directly to the <code>region=east</code> partition when the join key stayed raw."
        },
        {
          id: "wrapped",
          label: "UPPER(region)",
          query: "PartitionFilters: [isnotnull(region), dynamicpruningexpression(upper(region) IN ...)]\nPushedFilters on dim: [IsNotNull(region)]",
          note: "The clean static partition prune disappeared once the join key was wrapped in <code>UPPER()</code>."
        }
      ]
    },
    "viz-joins-issues-incomplete-join-conditions": {
      title: "Partial key vs full business condition",
      color: RED,
      summary: [
        { text: "product_id only → 4 rows", opts: red },
        { text: "temporal predicate → 2 rows", opts: green },
      ],
      modes: [
        {
          id: "partial",
          label: "Partial key",
          query: "JOIN price_history p ON o.product_id = p.product_id",
          tableCols: ["order_id", "product_id", "matched versions"],
          tableRows: [[100, 1, 3], [101, 2, 1]],
          note: "Order 100 matched every historical version of product 1."
        },
        {
          id: "complete",
          label: "Add date logic",
          query: "... AND p.effective_date <= o.order_date\nQUALIFY ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY p.effective_date DESC) = 1",
          tableCols: ["order_id", "effective_date", "price"],
          tableRows: [[100, "2024-06-01", 12.0], [101, "2024-01-01", 20.0]],
          note: "The exact-date 'fix' produced 0 rows in Spark 4.2, confirming it was too strict for an as-of lookup."
        }
      ]
    },
    "viz-joins-issues-join-order-optimizer": {
      title: "Planner defaults that matter",
      color: PURPLE,
      summary: [
        { text: "cbo.enabled=false", opts: red },
        { text: "joinReorder.enabled=false", opts: red },
        { text: "adaptive.enabled=true", opts: green },
      ],
      modes: [{
        id: "defaults",
        label: "Spark 4.2 defaults",
        tableCols: ["Config", "Observed value", "Why it matters"],
        tableRows: [
          ["spark.sql.cbo.enabled", "false", "no cost-based join reorder"],
          ["spark.sql.cbo.joinReorder.enabled", "false", "written order matters more"],
          ["spark.sql.adaptive.enabled", "true", "runtime strategy fixes still available"],
        ],
        note: "The local PySpark 4.2 run confirmed the defaults directly. AQE helps with strategy and partitioning, but not with business-key correctness."
      }]
    },
    "viz-joins-issues-null-key-trap": {
      title: "NULL equality vs null-safe equality",
      color: PURPLE,
      summary: [
        { text: "= drops NULL keys", opts: red },
        { text: "IS NOT DISTINCT FROM keeps them", opts: green },
      ],
      modes: [
        {
          id: "eq",
          label: "Standard =",
          query: "JOIN customers c ON o.customer_id = c.customer_id",
          tableCols: ["order_id", "customer_id", "name"],
          tableRows: [[1, 101, "Alice"], [2, 102, "Bob"]],
          note: "Both guest orders disappeared because <code>NULL = NULL</code> is <code>UNKNOWN</code>, never <code>TRUE</code>."
        },
        {
          id: "safe",
          label: "IS NOT DISTINCT FROM",
          query: "JOIN customers c ON o.customer_id IS NOT DISTINCT FROM c.customer_id",
          tableCols: ["order_id", "customer_id", "name"],
          tableRows: [[1, 101, "Alice"], [2, 102, "Bob"], [3, "NULL", "Unregistered"], [4, "NULL", "Unregistered"]],
          note: "Spark 4.2 also accepted Spark's shorthand <code>&lt;=&gt;</code>; this viz highlights the ANSI-style spelling used in the docs."
        }
      ]
    },
    "viz-joins-issues-outer-join-filter-pitfall": {
      title: "LEFT JOIN preserved rows disappearing",
      color: RED,
      summary: [
        { text: "WHERE version → 1 row", opts: red },
        { text: "ON version → 3 rows", opts: green },
      ],
      modes: [
        {
          id: "where",
          label: "Filter in WHERE",
          query: "LEFT JOIN orders o ON c.customer_id = o.customer_id\nWHERE o.status = 'COMPLETED'\n=> Spark 4.2 planned an Inner BroadcastHashJoin",
          tableCols: ["customer_id", "name", "order_id", "status"],
          tableRows: [[1, "Alice", 100, "COMPLETED"]],
          note: "Bob and Carol were removed after the join because the post-join filter rejected their right-side NULLs."
        },
        {
          id: "on",
          label: "Filter in ON",
          query: "LEFT JOIN orders o\n  ON c.customer_id = o.customer_id AND o.status = 'COMPLETED'\n=> Spark 4.2 kept a LeftOuter BroadcastHashJoin",
          tableCols: ["customer_id", "name", "order_id", "status"],
          tableRows: [[1, "Alice", 100, "COMPLETED"], [2, "Bob", "NULL", "NULL"], [3, "Carol", "NULL", "NULL"]],
          note: "Moving the predicate into <code>ON</code> changes which right rows can match, not whether left rows survive."
        }
      ]
    },
    "viz-joins-issues-partition-count": {
      title: "AQE coalescing vs fixed 200 partitions",
      color: PURPLE,
      summary: [
        { text: "AQE on → 1 partition", opts: green },
        { text: "AQE off → 200 partitions", opts: amber },
      ],
      modes: [
        {
          id: "aqe-on",
          label: "AQE enabled",
          query: "df.collect(); df.rdd.getNumPartitions()\n=> 1\nfinal plan contains AQEShuffleRead coalesced = True",
          note: "On the local Spark 4.2 run, the tiny shuffle collapsed from the default 200 down to 1 partition."
        },
        {
          id: "aqe-off",
          label: "AQE disabled",
          query: "spark.sql.adaptive.enabled = false\ndf.collect(); df.rdd.getNumPartitions()\n=> 200",
          note: "Disabling AQE pinned the same join at the raw <code>spark.sql.shuffle.partitions</code> value."
        }
      ]
    },
    "viz-joins-issues-predicate-vs-filter": {
      title: "Same predicate, different clause",
      color: PURPLE,
      summary: [
        { text: "INNER: same result", opts: green },
        { text: "OUTER: different result", opts: red },
      ],
      modes: [
        {
          id: "inner",
          label: "INNER JOIN",
          query: "Spark 4.2 produced the same BroadcastHashJoin shape for:\nON c.customer_id = o.customer_id AND o.status = 'COMPLETED'\nvs\nON c.customer_id = o.customer_id WHERE o.status = 'COMPLETED'",
          tableCols: ["customer_id", "name", "order_id", "status"],
          tableRows: [[1, "Alice", 100, "COMPLETED"]],
          note: "For inner joins, both placements returned the same single row in Spark 4.2."
        },
        {
          id: "outer",
          label: "LEFT JOIN",
          query: "ON keeps unmatched left rows NULL-filled; WHERE filters them afterward.",
          tableCols: ["Pattern", "Row count"],
          tableRows: [["predicate in ON", 3], ["predicate in WHERE", 1]],
          note: "Outer joins are where clause placement becomes semantically important."
        }
      ]
    },
    "viz-joins-issues-range-join-pitfalls": {
      title: "Pure range vs keyed range",
      color: AMBER,
      summary: [
        { text: "range only → BroadcastNestedLoopJoin", opts: amber },
        { text: "broadcast off → CartesianProduct", opts: red },
        { text: "equi-key added → SortMergeJoin", opts: green },
      ],
      modes: [
        {
          id: "pure",
          label: "Pure BETWEEN",
          query: "JOIN windows w ON r.ts BETWEEN w.lo AND w.hi\n=> BroadcastNestedLoopJoin BuildLeft, Inner",
          note: "Without an equality key, Spark 4.2 compared each reading against each interval candidate."
        },
        {
          id: "eqrange",
          label: "Equality + range",
          query: "JOIN windows w\n  ON r.sensor_id = w.sensor_id AND r.ts BETWEEN w.lo AND w.hi\n=> SortMergeJoin with the range as a residual condition",
          tableCols: ["sensor_id", "ts", "win_id"],
          tableRows: [[1, 100, 1], [1, 200, 1], [1, 200, 2], [1, 300, 2], [2, 150, 3]],
          note: "Adding the natural key constrains the expensive range check to rows that already share <code>sensor_id</code>."
        }
      ]
    },
    "viz-joins-issues-scd-type2-join": {
      title: "SCD2 version selection",
      color: PURPLE,
      summary: [
        { text: "is_current mislabels history", opts: red },
        { text: "BETWEEN double-matches boundary", opts: amber },
        { text: "half-open interval fixes both", opts: green },
      ],
      modes: [
        {
          id: "current",
          label: "is_current",
          tableCols: ["order_id", "order_date", "tier"],
          tableRows: [[100, "2024-03-15", "Gold"], [101, "2024-06-01", "Gold"], [102, "2024-02-01", "Silver"]],
          note: "Order 100 was historical, but <code>is_current</code> still labeled it with today's tier."
        },
        {
          id: "between",
          label: "BETWEEN",
          tableCols: ["order_id", "order_date", "tier"],
          tableRows: [[100, "2024-03-15", "Bronze"], [101, "2024-06-01", "Bronze"], [101, "2024-06-01", "Gold"], [102, "2024-02-01", "Silver"]],
          note: "Inclusive boundaries duplicated the changeover-day order."
        },
        {
          id: "halfopen",
          label: "[valid_from, valid_to)",
          tableCols: ["order_id", "order_date", "tier"],
          tableRows: [[100, "2024-03-15", "Bronze"], [101, "2024-06-01", "Gold"], [102, "2024-02-01", "Silver"]],
          note: "Spark 4.2 produced exactly one version per fact once the upper bound became exclusive."
        }
      ]
    },
    "viz-joins-issues-skewed-keys": {
      title: "Hot key concentration and salting",
      color: AMBER,
      summary: [
        { text: "customer_id 101 = 5/8 rows", opts: red },
        { text: "salted join row count stays 8", opts: green },
      ],
      modes: [
        {
          id: "skew",
          label: "Hot-key check",
          tableCols: ["customer_id", "order_count"],
          tableRows: [[101, 5], [103, 1], [102, 1], [104, 1]],
          note: "This confirms the skew signature directly; the largest key dominates the distribution."
        },
        {
          id: "salt",
          label: "Manual salting",
          query: "salted_orders.customer_id + random bucket\n× exploded customer buckets\n=> COUNT(*) remains 8",
          note: "The local Spark 4.2 run verified the salted join preserved row count. AQE skew splitting still depends on a partition exceeding the runtime skew thresholds."
        }
      ]
    },
    "viz-joins-issues-troubleshooting-matrix": {
      title: "Matrix reading shortcut",
      color: PURPLE,
      summary: [
        { text: "row count low", opts: red },
        { text: "row count high", opts: amber },
        { text: "plan slow", opts: purple },
      ],
      modes: [{
        id: "matrix",
        label: "Symptom router",
        tableCols: ["Observed symptom", "First page to open"],
        tableRows: [["Rows silently missing", "null-key-trap / type-mismatch / case-whitespace"], ["Rows multiplied", "data-explosion / duplicate-key-explosion / incomplete-join-conditions"], ["LEFT acts INNER", "outer-join-filter-pitfall / predicate-vs-filter"], ["BNLJ / CartesianProduct / spills", "range-join-pitfalls / cartesian-join / partition-count / skewed-keys"]],
        note: "Use the matrix when you already know the symptom and want the shortest path to the matching deep dive."
      }]
    },
    "viz-joins-issues-type-mismatch": {
      title: "Cast failure vs tolerant parsing",
      color: RED,
      summary: [
        { text: "CAST_INVALID_INPUT", opts: red },
        { text: "TRY_CAST → NULL", opts: amber },
        { text: "TRY_TO_DATE(format) recovers row", opts: green },
      ],
      modes: [
        {
          id: "cast",
          label: "Plain CAST",
          query: "CAST(o.order_date_str AS DATE) = c.cal_date\n=> DateTimeException [CAST_INVALID_INPUT] on '01/25/2024'",
          note: "Spark 4.2 aborted the whole query under ANSI mode once it hit the malformed date string."
        },
        {
          id: "try",
          label: "TRY_CAST",
          tableCols: ["order_id", "order_date_str", "parsed_date", "cal_date"],
          tableRows: [[1, "2024-01-15", "2024-01-15", "2024-01-15"], [2, "2024-01-20", "2024-01-20", "2024-01-20"], [3, "01/25/2024", "NULL", "NULL"]],
          note: "The tolerant cast kept the query running, but row 3 silently fell into the NULL-key trap."
        }
      ]
    }
  };

  function init() {
    const specs = Object.entries(SPECS);
    specs.forEach(([id, spec]) => {
      const elm = document.getElementById(id);
      if (elm && !elm.dataset.rendered) { renderIssueViz(elm, spec); elm.dataset.rendered = "1"; }
      else if (elm) { renderIssueViz(elm, spec); }
    });
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
