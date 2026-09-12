/**
 * querying-subquery-viz.js
 * D3 v7 interactive visualizations for docs/querying/subquery/*.md.
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

  function makeCardRow(stage, cards) {
    const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
    cards.forEach((c) => {
      const card = row.append("div")
        .style("min-width", c.width || "120px")
        .style("padding", "8px 10px")
        .style("border-radius", "8px")
        .style("border", `1px solid ${c.border || "#ddd"}`)
        .style("background", c.bg || "#fff");
      card.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-bottom", "4px").text(c.label);
      card.append("div").style("font-size", "0.84rem").style("font-weight", "700").style("color", c.fg || FG).text(c.value);
    });
    return row;
  }

  function renderIndexMap(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const cases = {
      scalar: {
        returns: "1 row × 1 column",
        plan: "Subquery once, or join if correlated",
        note: "Best for AVG/MAX/config thresholds."
      },
      in: {
        returns: "membership boolean",
        plan: "LeftSemi join",
        note: "Great for include-set filters."
      },
      exists: {
        returns: "existence boolean",
        plan: "LeftSemi / LeftAnti join",
        note: "Best default for exclusion on nullable data."
      },
      correlated: {
        returns: "depends on outer row",
        plan: "Hash join or nested loop",
        note: "Check EXPLAIN for heavy non-equality cases."
      },
      derived: {
        returns: "inline table",
        plan: "Inlined relation",
        note: "Alias inner expressions for stable names."
      },
      having: {
        returns: "group filter",
        plan: "Aggregate then compare",
        note: "Good for dynamic thresholds over groups."
      }
    };
    const stage = container.append("div");
    const group = btnGroup(container, [["scalar", "Scalar"], ["in", "IN"], ["exists", "EXISTS"], ["correlated", "Correlated"], ["derived", "Derived table"], ["having", "HAVING"]], render);

    function render(key) {
      const info = cases[key];
      stage.selectAll("*").remove();
      makeCardRow(stage, [
        { label: "Returns", value: info.returns, bg: "rgba(124,77,255,0.08)", border: PURPLE, fg: PURPLE },
        { label: "Typical plan", value: info.plan, bg: "rgba(38,166,154,0.08)", border: TEAL, fg: TEAL }
      ]);
      box(stage, info.note);
    }
    render(group.get());
  }

  function renderScalarCases(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const cases = {
      zero: {
        sql: "SELECT (SELECT customer FROM orders WHERE country = 'ZZ') AS no_customer;",
        result: "+-----------+\n|no_customer|\n+-----------+\n|       NULL|\n+-----------+",
        color: GRAY,
        note: "Zero rows become NULL."
      },
      one: {
        sql: "SELECT (SELECT MAX(amount) FROM orders) AS max_amount;",
        result: "+----------+\n|max_amount|\n+----------+\n|     450.0|\n+----------+",
        color: HIT_COLOR,
        note: "One row and one column is valid."
      },
      manyRows: {
        sql: "SELECT (SELECT customer FROM orders WHERE country = 'US') AS any_customer;",
        result: "[SCALAR_SUBQUERY_TOO_MANY_ROWS]\nMore than one row returned by a subquery used as an expression.",
        color: RED,
        note: "Fails at runtime."
      },
      manyCols: {
        sql: "SELECT (SELECT order_id, amount FROM orders LIMIT 1) AS pair;",
        result: "[INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN]\nScalar subquery must return only one column, but got 2.",
        color: AMBER,
        note: "Fails during analysis."
      }
    };
    const group = btnGroup(container, [["zero", "0 rows"], ["one", "1 row"], ["manyRows", ">1 row"], ["manyCols", "2 columns"]], render);

    function render(key) {
      const info = cases[key];
      stage.selectAll("*").remove();
      chip(stage, info.note, { bg: `${info.color}22`, fg: info.color, border: info.color }).style("margin-bottom", "8px");
      box(stage, info.sql, { border: PURPLE });
      stage.append("div").style("height", "8px");
      box(stage, info.result, { bg: `${info.color}14`, border: info.color, fg: key === "zero" ? FG : info.color });
    }
    render(group.get());
  }

  function renderInNullTrap(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["withNull", "Inner has NULL"], ["withoutNull", "Inner has no NULL"]], render);

    function render(mode) {
      const withNull = mode === "withNull";
      stage.selectAll("*").remove();
      makeCardRow(stage, [
        { label: "Inner set", value: withNull ? "{ 'CA', NULL }" : "{ 'CA' }", bg: "rgba(255,167,38,0.08)", border: AMBER, fg: AMBER },
        { label: "Outer rows", value: "US, CA, MX, NULL", bg: "rgba(144,164,174,0.10)", border: GRAY, fg: FG }
      ]);
      const table = stage.append("table").style("border-collapse", "collapse").style("font-size", "0.78rem");
      const rows = [
        ["Predicate", "Verified rows"],
        ["IN", "CA"],
        ["NOT IN", withNull ? "<none>" : "MX, US"],
        ["NOT EXISTS", "NULL, MX, US"]
      ];
      rows.forEach((r, idx) => {
        const tr = table.append("tr");
        r.forEach((cell) => {
          tr.append(idx === 0 ? "th" : "td")
            .style("border", "1px solid #d7dce2")
            .style("padding", "6px 10px")
            .style("background", idx === 0 ? "#f5f5f7" : "#fff")
            .style("text-align", "left")
            .text(cell);
        });
      });
      stage.append("div").style("height", "8px");
      box(stage, withNull
        ? "Spark 4.2 planned NOT IN with null-aware anti semantics; one NULL on the right poisons every non-match."
        : "Once the right side is proven non-null, NOT IN works for non-null outer values — but outer NULL still does not pass.");
    }
    render(group.get());
  }

  function renderExistsSemiAnti(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["exists", "EXISTS"], ["notExists", "NOT EXISTS"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      makeCardRow(stage, [
        { label: "Plan", value: mode === "exists" ? "LeftSemi join" : "LeftAnti join", bg: mode === "exists" ? "rgba(38,166,154,0.08)" : "rgba(239,83,80,0.08)", border: mode === "exists" ? TEAL : RED, fg: mode === "exists" ? TEAL : RED },
        { label: "Inner rows", value: "1, 1, 2, NULL", bg: "rgba(144,164,174,0.10)", border: GRAY, fg: FG }
      ]);
      const outer = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      [1, 2, 3, null].forEach((v) => {
        const label = v === null ? "NULL" : String(v);
        const keep = mode === "exists" ? (v === 1 || v === 2) : (v === 3 || v === null);
        chip(outer, label, keep
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "rgba(239,83,80,0.10)", fg: RED, border: RED });
      });
      box(stage, mode === "exists"
        ? "Duplicates on the inner side do not fan out the result; a row is kept once if any match exists."
        : "The unrelated inner NULL does not poison the whole predicate, so NOT EXISTS remains safe where NOT IN would fail.");
    }
    render(group.get());
  }

  function renderCorrelatedPlan(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const cases = {
      hash: {
        label: "Equality correlated aggregate",
        plan: "BroadcastHashJoin\nInner join on customer\nFilter: amount > avg(amount)",
        color: TEAL,
        note: "Verified from: amount > (SELECT AVG(amount) ... WHERE i.customer = o.customer)."
      },
      nested: {
        label: "Non-equality EXISTS",
        plan: "BroadcastNestedLoopJoin\nLeftSemi join\nCondition: i.amount > o.amount",
        color: AMBER,
        note: "Spark removed the subquery, but the physical operator is still much heavier."
      },
      uncorrelated: {
        label: "Uncorrelated scalar",
        plan: "Filter\n└─ Subquery subquery#...\n   └─ Aggregate avg(amount)",
        color: PURPLE,
        note: "Verified from: amount > (SELECT AVG(amount) FROM orders)."
      }
    };
    const group = btnGroup(container, [["hash", "Hash join"], ["nested", "Nested loop"], ["uncorrelated", "Scalar subquery"]], render);

    function render(key) {
      const info = cases[key];
      stage.selectAll("*").remove();
      chip(stage, info.label, { bg: `${info.color}22`, fg: info.color, border: info.color }).style("margin-bottom", "8px");
      box(stage, info.plan, { border: info.color, bg: `${info.color}10`, fg: key === "uncorrelated" ? FG : info.color });
      stage.append("div").style("height", "8px");
      box(stage, info.note);
    }
    render(group.get());
  }

  function renderDerivedAliasing(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["noTableAlias", "No relation alias"], ["innerAlias", "Inner column alias"], ["badOuterRef", "Missing column alias"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      if (mode === "noTableAlias") {
        box(stage, "FROM (SELECT customer, SUM(amount) AS total_spent FROM orders GROUP BY customer)\nWHERE total_spent > 200", { border: TEAL, bg: "rgba(38,166,154,0.08)", fg: TEAL });
        stage.append("div").style("height", "8px");
        chip(stage, "Runs in Spark 4.2", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      } else if (mode === "innerAlias") {
        box(stage, "SELECT customer, total_spent\nFROM (SELECT customer, SUM(amount) AS total_spent FROM orders GROUP BY customer) AS customer_totals", { border: PURPLE, bg: "rgba(124,77,255,0.08)", fg: PURPLE });
        stage.append("div").style("height", "8px");
        chip(stage, "Readable outer column name", { bg: "rgba(124,77,255,0.12)", fg: PURPLE, border: PURPLE });
      } else {
        box(stage, "SELECT total_spent\nFROM (SELECT customer, SUM(amount) FROM orders GROUP BY customer) AS customer_totals", { border: RED, bg: "rgba(239,83,80,0.08)", fg: RED });
        stage.append("div").style("height", "8px");
        box(stage, "[UNRESOLVED_COLUMN.WITH_SUGGESTION]\nDid you mean one of the following? [customer, sum(amount)]", { border: RED, bg: "rgba(239,83,80,0.10)", fg: RED });
      }
    }
    render(group.get());
  }

  function renderHavingThreshold(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const totals = [
      ["Alice", 725],
      ["Bob", 570],
      ["Charlie", 80],
      ["Dave", 90]
    ];
    const threshold = 209.29;
    makeCardRow(stage, [{ label: "Scalar threshold", value: "AVG(amount) = 209.29", bg: "rgba(255,167,38,0.10)", border: AMBER, fg: AMBER }]);
    const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
    totals.forEach(([name, value]) => {
      chip(row, `${name}: ${value}`, value > threshold
        ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: "rgba(144,164,174,0.10)", fg: FG, border: GRAY });
    });
    box(stage, "HAVING compares each group aggregate after GROUP BY. A nested aggregate like MAX(SUM(amount)) must be split into another subquery level.");
  }

  function renderJoinFanout(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["dedup", "Right side deduped"], ["dupes", "Right side duplicated"]], render);

    function render(mode) {
      const dupes = mode === "dupes";
      stage.selectAll("*").remove();
      makeCardRow(stage, [
        { label: "IN / EXISTS result", value: "1, 2", bg: "rgba(38,166,154,0.08)", border: TEAL, fg: TEAL },
        { label: "JOIN result", value: dupes ? "1, 1, 2, 2" : "1, 2", bg: dupes ? "rgba(239,83,80,0.08)" : "rgba(124,77,255,0.08)", border: dupes ? RED : PURPLE, fg: dupes ? RED : PURPLE }
      ]);
      box(stage, dupes
        ? "A plain join multiplies rows when the right side contains duplicates. Add DISTINCT or pre-aggregate before calling it equivalent to IN or EXISTS."
        : "Once the join side is deduplicated, the join matches the membership-style subquery result."
      );
    }
    render(group.get());
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-subquery-index-map", renderIndexMap],
      ["viz-subquery-scalar-cases", renderScalarCases],
      ["viz-subquery-in-null-trap", renderInNullTrap],
      ["viz-subquery-exists-semi-anti", renderExistsSemiAnti],
      ["viz-subquery-correlated-plan", renderCorrelatedPlan],
      ["viz-subquery-derived-aliasing", renderDerivedAliasing],
      ["viz-subquery-having-threshold", renderHavingThreshold],
      ["viz-subquery-join-fanout", renderJoinFanout],
    ];
    specs.forEach(([id, fn]) => {
      const elm = document.getElementById(id);
      if (elm && !elm.dataset.rendered) { fn(elm); elm.dataset.rendered = "1"; }
    });
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
