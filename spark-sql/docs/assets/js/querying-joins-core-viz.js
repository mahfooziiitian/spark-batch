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

  function panel(container) {
    return container.append("div")
      .style("display", "grid")
      .style("gap", "10px");
  }

  function chipsRow(parent, items) {
    const row = parent.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
    items.forEach((item) => chip(row, item.text, item.opts));
    return row;
  }

  function renderJoinsOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const cases = {
      equi: {
        title: "Equi join",
        chips: [
          { text: "predicate: l.k = r.k", opts: { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } },
          { text: "usable equi key", opts: { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE } },
        ],
        plan: "EXPLAIN FORMATTED\n... BroadcastHashJoin | SortMergeJoin | ShuffledHashJoin\n(depending on size and hints)",
        note: "Equi keys unlock Spark's hash and sort-merge join families.",
        color: TEAL,
      },
      range: {
        title: "Pure range join",
        chips: [
          { text: "predicate: p >= start AND p < end", opts: { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER } },
          { text: "no extracted equi key", opts: { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED } },
        ],
        plan: "EXPLAIN FORMATTED\n... CartesianProduct\n(test query in PySpark 4.2)",
        note: "Without a usable equi key, the verified OSS Spark 4.2 plan fell into the cartesian/nested-loop family.",
        color: AMBER,
      },
      comma: {
        title: "Comma join without predicate",
        chips: [
          { text: "FROM a, b", opts: { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED } },
          { text: "implicit cartesian product", opts: { bg: "rgba(144,164,174,0.15)", fg: FG, border: GRAY } },
        ],
        plan: "EXPLAIN FORMATTED\n... CartesianProduct",
        note: "Comma syntax with no join filter is just a cartesian product.",
        color: RED,
      },
      commaWhere: {
        title: "Comma join with WHERE equality",
        chips: [
          { text: "FROM a, b WHERE a.id = b.id", opts: { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } },
          { text: "optimized back into equi join", opts: { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE } },
        ],
        plan: "EXPLAIN FORMATTED\n... SortMergeJoin\n(verified test run)",
        note: "Catalyst can recover an equi join from the WHERE clause, but explicit JOIN ... ON is clearer.",
        color: PURPLE,
      },
    };

    const group = btnGroup(container, [["equi", "Equi key"], ["range", "Range"], ["comma", "Comma only"], ["commaWhere", "Comma + WHERE"]], render);
    const stage = panel(container);

    function render(key) {
      stage.selectAll("*").remove();
      const item = cases[key];
      stage.append("div").style("font-weight", "700").style("color", item.color).text(item.title);
      chipsRow(stage, item.chips);
      box(stage, item.plan, { border: item.color, fg: FG });
      stage.append("div").style("font-size", "0.74rem").style("color", GRAY).text(item.note);
    }

    render(group.get());
  }

  function renderJoinExpression(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const data = {
      on: {
        title: "JOIN ... ON",
        plan: "query\nSELECT * FROM customers c JOIN orders o ON c.id = o.customer_id\n\nresolved output\n[id, ..., customer_id, ...]  -- both key columns remain unless projected away",
        chips: [
          { text: "explicit boolean predicate", opts: { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } },
          { text: "schema stays explicit", opts: { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE } },
        ],
      },
      using: {
        title: "JOIN ... USING (id)",
        plan: "verified query\nSELECT * FROM a JOIN b USING (id)\n\nresolved output\n[id, grp, val, grp, val]\n\nonly id is collapsed; other common names still appear twice",
        chips: [
          { text: "same-named keys only", opts: { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER } },
          { text: "listed USING columns collapse", opts: { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } },
        ],
      },
      natural: {
        title: "NATURAL JOIN",
        plan: "verified query\nSELECT * FROM nat_l NATURAL JOIN nat_r\n\nresolved output\n[id, common, left_only, right_only]\n\nall shared names become equality keys and collapse to one copy",
        chips: [
          { text: "joins on every shared name", opts: { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED } },
          { text: "schema drift can change semantics", opts: { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER } },
        ],
      },
      comma: {
        title: "Comma syntax",
        plan: "verified queries\nSELECT * FROM a, b                -> CartesianProduct\nSELECT * FROM a, b WHERE a.id=b.id -> SortMergeJoin\n\ncomma joins rely on later filters for meaning",
        chips: [
          { text: "FROM a, b", opts: { bg: "rgba(144,164,174,0.15)", fg: FG, border: GRAY } },
          { text: "predicate lives in WHERE", opts: { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE } },
        ],
      },
    };

    const group = btnGroup(container, [["on", "ON"], ["using", "USING"], ["natural", "NATURAL"], ["comma", "Comma"]], render);
    const stage = panel(container);

    function render(key) {
      stage.selectAll("*").remove();
      const item = data[key];
      stage.append("div").style("font-weight", "700").style("color", PURPLE).text(item.title);
      chipsRow(stage, item.chips);
      box(stage, item.plan, { border: PURPLE });
    }

    render(group.get());
  }

  function renderJoinHints(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const mapping = {
      broadcast: { alias: "BROADCAST / BROADCASTJOIN / MAPJOIN", op: "BroadcastHashJoin", color: TEAL, note: "Verified on an equi join; all aliases produced the same operator." },
      merge: { alias: "MERGE / SHUFFLE_MERGE / MERGEJOIN", op: "SortMergeJoin", color: PURPLE, note: "Verified on an equi join; all aliases converged to sort-merge." },
      shuffleHash: { alias: "SHUFFLE_HASH", op: "ShuffledHashJoin", color: AMBER, note: "Verified on an equi join with broadcast disabled." },
      srnl: { alias: "SHUFFLE_REPLICATE_NL", op: "CartesianProduct", color: RED, note: "The tested inner join surfaced as CartesianProduct in EXPLAIN FORMATTED." },
      range: { alias: "[Databricks] RANGE_JOIN", op: "No OSS Spark 4.2 strategy change", color: GRAY, note: "The tested PySpark 4.2 plan stayed CartesianProduct." },
    };

    const group = btnGroup(container, [["broadcast", "Broadcast"], ["merge", "Merge"], ["shuffleHash", "Shuffle hash"], ["srnl", "Replicate NL"], ["range", "RANGE_JOIN"]], render);
    const stage = panel(container);

    function render(key) {
      stage.selectAll("*").remove();
      const item = mapping[key];
      chip(stage, item.alias, { bg: "#f5f5f7", fg: item.color, border: item.color });
      box(stage, `physical operator\n${item.op}`, { border: item.color });
      stage.append("div").style("font-size", "0.74rem").style("color", GRAY).text(item.note);
    }

    render(group.get());
  }

  function renderJoinHintOperators(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const data = {
      broadcast: {
        aliases: ["BROADCAST", "BROADCASTJOIN", "MAPJOIN"],
        operator: "BroadcastHashJoin",
        when: "Small build side or explicit broadcast request",
        color: TEAL,
      },
      merge: {
        aliases: ["MERGE", "SHUFFLE_MERGE", "MERGEJOIN"],
        operator: "SortMergeJoin",
        when: "Large equi joins with sortable keys",
        color: PURPLE,
      },
      shuffleHash: {
        aliases: ["SHUFFLE_HASH"],
        operator: "ShuffledHashJoin",
        when: "Equi joins where a shuffled hash table is preferred",
        color: AMBER,
      },
      srnl: {
        aliases: ["SHUFFLE_REPLICATE_NL"],
        operator: "CartesianProduct (formatted plan output)",
        when: "Nested-loop style fallback or forced replicate-NL behavior",
        color: RED,
      },
    };

    const group = btnGroup(container, [["broadcast", "BROADCAST"], ["merge", "MERGE"], ["shuffleHash", "SHUFFLE_HASH"], ["srnl", "SHUFFLE_REPLICATE_NL"]], render);
    const stage = panel(container);

    function render(key) {
      stage.selectAll("*").remove();
      const item = data[key];
      stage.append("div").style("font-weight", "700").style("color", item.color).text(item.operator);
      chipsRow(stage, item.aliases.map((alias) => ({ text: alias, opts: { bg: "#f5f5f7", fg: item.color, border: item.color } })));
      box(stage, `best fit\n${item.when}`, { border: item.color });
    }

    render(group.get());
  }

  function renderRangeHint(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const group = btnGroup(container, [["oss", "OSS Spark 4.2"], ["dbx", "Databricks docs"]], render);
    const stage = panel(container);

    function render(key) {
      stage.selectAll("*").remove();
      if (key === "oss") {
        stage.append("div").style("font-weight", "700").style("color", RED).text("Open-source Spark 4.2 verification");
        chipsRow(stage, [
          { text: "RANGE_JOIN parsed", opts: { bg: "rgba(144,164,174,0.15)", fg: FG, border: GRAY } },
          { text: "plan still CartesianProduct", opts: { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED } },
          { text: "databricks binSize conf had no effect", opts: { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER } },
        ]);
        box(stage, "SELECT /*+ RANGE_JOIN(ranges, 10) */ ...\n=> EXPLAIN FORMATTED stayed in the ordinary non-equi join path", { border: RED });
      } else {
        stage.append("div").style("font-weight", "700").style("color", PURPLE).text("[Databricks] documented feature");
        chipsRow(stage, [
          { text: "hint: RANGE_JOIN(table, bin)", opts: { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE } },
          { text: "conf: spark.databricks.optimizer.rangeJoin.binSize", opts: { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } },
        ]);
        box(stage, "Use this only when your target runtime is Databricks Runtime.\nDo not document it as an OSS Spark 4.2 feature.", { border: PURPLE });
      }
    }

    render(group.get());
  }

  function renderResolver(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const data = {
      precedence: {
        title: "Conflicting strategy hints",
        rows: [
          { text: "BROADCAST > MERGE", opts: { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } },
          { text: "MERGE > SHUFFLE_HASH", opts: { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE } },
          { text: "SHUFFLE_HASH > SHUFFLE_REPLICATE_NL", opts: { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER } },
        ],
        plan: "verified winners\nBroadcastHashJoin\nSortMergeJoin\nShuffledHashJoin",
      },
      alias: {
        title: "Alias-aware resolution",
        rows: [
          { text: "/*+ BROADCAST(dim) */", opts: { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } },
          { text: "JOIN (...) AS dim", opts: { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE } },
          { text: "resolved and applied", opts: { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER } },
        ],
        plan: "subquery alias test\n=> BroadcastHashJoin",
      },
      ignored: {
        title: "Wrong relation name",
        rows: [
          { text: "hinted name: right_t", opts: { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED } },
          { text: "actual alias: r", opts: { bg: "rgba(144,164,174,0.15)", fg: FG, border: GRAY } },
          { text: "hint ignored", opts: { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER } },
        ],
        plan: "wrong-alias test\n=> SortMergeJoin fallback",
      },
    };

    const group = btnGroup(container, [["precedence", "Precedence"], ["alias", "Subquery alias"], ["ignored", "Ignored hint"]], render);
    const stage = panel(container);

    function render(key) {
      stage.selectAll("*").remove();
      const item = data[key];
      stage.append("div").style("font-weight", "700").style("color", PURPLE).text(item.title);
      chipsRow(stage, item.rows);
      box(stage, item.plan, { border: PURPLE });
    }

    render(group.get());
  }

  const VIZ_MAP = {
    "viz-joins-overview-core": renderJoinsOverview,
    "viz-join-expression-core": renderJoinExpression,
    "viz-join-hints-core": renderJoinHints,
    "viz-join-hint-operators-core": renderJoinHintOperators,
    "viz-join-range-hint-core": renderRangeHint,
    "viz-join-resolver-core": renderResolver,
  };

  function init() {
    for (const [id, fn] of Object.entries(VIZ_MAP)) {
      const el = document.getElementById(id);
      if (el) { el.innerHTML = ""; fn(el); }
    }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
