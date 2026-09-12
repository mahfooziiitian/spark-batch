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

  function renderAggIndexMap(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      group: { rows: ["East -> 650", "West -> 760", "North -> 300"], note: "One output row per grouping key." },
      rollup: { rows: ["East, Widget -> 200", "East, NULL -> 650", "NULL, NULL -> 1710"], note: "ROLLUP adds hierarchy subtotals plus the grand total." },
      cube: { rows: ["East, Widget -> 200", "NULL, Widget -> 440", "East, NULL -> 650", "NULL, NULL -> 1710"], note: "CUBE emits every subtotal combination." },
      pivot: { rows: ["2023 | east=7000 west=4400 south=5200", "2024 | east=7400 west=5000 south=1600"], note: "PIVOT rotates one dimension into multiple columns." },
      unpivot: { rows: ["East, Q1 -> 100", "East, Q2 -> 200", "North, Q2 -> NULL (include nulls)"], note: "UNPIVOT turns wide columns back into labelled rows." },
    };
    const group = btnGroup(container, [["group", "GROUP BY"], ["rollup", "ROLLUP"], ["cube", "CUBE"], ["pivot", "PIVOT"], ["unpivot", "UNPIVOT"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      modes[mode].rows.forEach((r) => chip(row, r, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE }));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function renderGroupByFlow(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const cases = {
      where: {
        title: "WHERE filters before grouping",
        rows: ["rows >= 100 -> East, West, North", "then SUM(amount) per region"],
        result: "Fewer input rows reach the aggregate." },
      having: {
        title: "HAVING filters after grouping",
        rows: ["group East -> 650", "group West -> 760", "group North -> 300"],
        result: "Then HAVING SUM(amount) > 500 keeps East and West only." },
      nulls: {
        title: "NULL keys still form a group",
        rows: ["East", "West", "North", "NULL"],
        result: "Spark groups every NULL key together instead of dropping it." },
    };
    const group = btnGroup(container, [["where", "WHERE"], ["having", "HAVING"], ["nulls", "NULL key"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      stage.append("div").style("font-size", "0.72rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px").text(cases[mode].title);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      cases[mode].rows.forEach((r) => chip(row, r, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(cases[mode].result);
    }
    render(group.get());
  }

  function renderStatsDistribution(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const data = [1, 2, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10];
    let p = 0.5;
    const row = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "0.6rem");
    row.append("span").style("font-size", "0.76rem").style("color", FG).text("percentile p =");
    const label = row.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px").text(p.toFixed(2));
    row.append("input").attr("type", "range").attr("min", 0).attr("max", 100).attr("value", p * 100).style("width", "160px")
      .on("input", function () { p = +this.value / 100; update(); });
    const bars = container.append("div").style("display", "flex").style("align-items", "flex-end").style("gap", "3px").style("height", "80px");
    const result = container.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-top", "6px");
    function percentile(sorted, pct) {
      const idx = pct * (sorted.length - 1);
      const lo = Math.floor(idx), hi = Math.ceil(idx);
      if (lo === hi) return sorted[lo];
      return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
    }
    function update() {
      label.text(p.toFixed(2));
      const sorted = [...data].sort((a, b) => a - b);
      const val = percentile(sorted, p);
      bars.selectAll("*").remove();
      sorted.forEach((v) => {
        const near = Math.abs(v - val) < 0.5;
        bars.append("div").style("width", "16px").style("height", `${v * 7}px`)
          .style("background", near ? HIT_COLOR : "#e0e0e0").style("border-radius", "2px 2px 0 0");
      });
      result.html(`PERCENTILE(x, ${p.toFixed(2)}) ≈ <b style="color:${HIT_COLOR}">${val.toFixed(2)}</b>${Math.abs(p - 0.5) < 0.001 ? " &nbsp;(median)" : ""}`);
    }
    update();
  }

  function renderCubeLevels(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const rows = {
      0: ["East, Widget -> 200", "West, Gadget -> 610", "g_region=0 g_product=0"],
      1: ["East, NULL -> 650", "West, NULL -> 760", "product rolled up"],
      2: ["NULL, Widget -> 490", "NULL, Gadget -> 1270", "region rolled up"],
      3: ["NULL, NULL -> 1760", "grand total", "both rolled up"],
    };
    const group = btnGroup(container, [["0", "gid 0"], ["1", "gid 1"], ["2", "gid 2"], ["3", "gid 3"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      rows[mode].forEach((r, idx) => chip(row, r, idx < 2 ? { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE } : { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
    }
    render(group.get());
  }

  function renderGroupingSets(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["detail", "(region, product)"], ["region", "(region)"], ["product", "(product)"], ["grand", "()"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const rows = {
        detail: { gid: 0, labels: ["East, Widget", "West, Gadget"], note: "Exact detail groups only." },
        region: { gid: 1, labels: ["East, NULL", "West, NULL"], note: "Only region subtotals appear because this set was explicitly listed." },
        product: { gid: 2, labels: ["NULL, Widget", "NULL, Gadget"], note: "Product-only subtotal rows appear only if you enumerate `(product)`." },
        grand: { gid: 3, labels: ["NULL, NULL"], note: "The empty grouping set adds a single grand-total row." },
      }[mode];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      chip(row, `GROUPING_ID = ${rows.gid}`, { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      rows.labels.forEach((r) => chip(row, r, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE }));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(rows.note);
    }
    render(group.get());
  }

  function renderRollupPath(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      detail: { gid: 0, rows: ["East, Widget -> 200", "East, Gadget -> 450"], note: "Both keys participate." },
      region: { gid: 1, rows: ["East, NULL -> 650", "West, NULL -> 760"], note: "The rightmost key drops first." },
      grand: { gid: 3, rows: ["NULL, NULL -> 1710"], note: "Both keys are rolled up into the grand total." },
    };
    const group = btnGroup(container, [["detail", "detail"], ["region", "region subtotal"], ["grand", "grand total"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const m = modes[mode];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      chip(row, `gid ${m.gid}`, { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      m.rows.forEach((r) => chip(row, r, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(m.note);
    }
    render(group.get());
  }

  function renderPivotCaseCells(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      sum_null: { expr: "SUM(CASE WHEN region='South' THEN revenue END)", result: "NULL", note: "No matching row means SUM sees only NULLs." },
      sum_zero: { expr: "SUM(CASE WHEN region='South' THEN revenue ELSE 0 END)", result: "0", note: "ELSE 0 turns missing rows into additive zeros." },
      count_case: { expr: "COUNT(CASE WHEN revenue >= 4000 THEN 1 END)", result: "3", note: "COUNT ignores the NULL rows produced by non-matches." },
    };
    const group = btnGroup(container, [["sum_null", "SUM no ELSE"], ["sum_zero", "SUM ELSE 0"], ["count_case", "COUNT CASE"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, modes[mode].expr, { bg: "#f5f5f7", border: "#ddd" });
      chip(stage.append("div").style("margin-top", "6px"), `result = ${modes[mode].result}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px").text(modes[mode].note);
    }
    render(group.get());
  }

  function renderPivotNativeLayout(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["good", "group by yr"], ["oops", "leave sale_id in"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      if (mode === "good") {
        const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
        chip(row, "yr", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
        chip(row, "east_revenue", { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
        chip(row, "west_revenue", { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
        chip(row, "south_revenue", { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
        stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px").text("Only yr remains outside the pivot, so Spark returns one row per year.");
      } else {
        const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
        chip(row, "sale_id", { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
        chip(row, "yr", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
        chip(row, "east_revenue", { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
        stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px").text("Leaving sale_id in the subquery creates one pivot row per sale_id + yr combination.");
      }
    }
    render(group.get());
  }

  function renderUnpivotRows(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["drop", "default"], ["keep", "INCLUDE NULLS"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const rows = mode === "drop"
        ? ["North, Q1 -> 90", "North, Q3 -> 110", "North, Q4 -> 140"]
        : ["North, Q1 -> 90", "North, Q2 -> NULL", "North, Q3 -> 110", "North, Q4 -> 140"];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
      rows.forEach((r) => chip(row, r, r.includes("NULL")
        ? { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }
        : { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
    }
    render(group.get());
  }

  function renderSimpleAggregateNull(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let hasNull = true;
    const values = [120, 80, 450, 270];
    const toggle = container.append("button")
      .style("padding", "5px 14px").style("border-radius", "5px").style("cursor", "pointer").style("font-size", "0.76rem").style("margin-bottom", "0.6rem");
    const rowsWrap = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "8px").style("flex-wrap", "wrap");
    const statsWrap = container.append("div").style("font-family", "monospace").style("font-size", "0.8rem");
    function update() {
      toggle.text(hasNull ? "Remove NULL row" : "Add NULL row").style("background", hasNull ? RED : "#fff").style("color", hasNull ? "#fff" : FG).style("border", `1px solid ${hasNull ? RED : "#ccc"}`);
      const rows = hasNull ? [...values, null] : values;
      rowsWrap.selectAll("*").remove();
      rows.forEach((v) => chip(rowsWrap, v === null ? "NULL" : String(v), v === null
        ? { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }
        : { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      const nonNull = rows.filter((v) => v !== null);
      const sum = nonNull.reduce((a, b) => a + b, 0);
      const avg = sum / nonNull.length;
      statsWrap.html(`COUNT(*) = ${rows.length} &nbsp;&nbsp; COUNT(col) = ${nonNull.length} &nbsp;&nbsp; SUM(col) = ${sum} &nbsp;&nbsp; AVG(col) = ${avg.toFixed(2)}`);
    }
    toggle.on("click", () => { hasNull = !hasNull; update(); });
    update();
  }

  function renderAvgTypeBehavior(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const info = {
      int: { expr: "AVG(CAST(1 AS INT))", out: "1.0", type: "double" },
      decimal: { expr: "AVG(CAST(1.23 AS DECIMAL(10,2)))", out: "1.230000", type: "decimal(14,6)" },
      weighted: { expr: "SUM(avg * count) / SUM(count)", out: "50.00", type: "recombined overall average" },
    };
    const group = btnGroup(container, [["int", "integral"], ["decimal", "decimal"], ["weighted", "pre-aggregated"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, info[mode].expr);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-top", "6px");
      chip(row, `result = ${info[mode].out}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      chip(row, `type = ${info[mode].type}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
    }
    render(group.get());
  }

  function renderApproxCountRsd(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const exact = 10000;
    const values = { "0.01": 9965, "0.05": 9571, "0.20": 7939 };
    const group = btnGroup(container, [["0.01", "rsd 0.01"], ["0.05", "rsd 0.05"], ["0.20", "rsd 0.20"]], render);
    function render(rsd) {
      stage.selectAll("*").remove();
      const est = values[rsd];
      const delta = est - exact;
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      chip(row, `exact = ${exact}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      chip(row, `estimate = ${est}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      chip(row, `delta = ${delta}`, delta === 0 ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } : { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text("Observed on 1,000,000 rows with 10,000 true distinct values in PySpark 4.2.");
    }
    render(group.get());
  }

  function renderCountNullRules(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const rows = ["(East, Widget)", "(East, NULL)", "(NULL, Widget)", "(West, Gadget)"];
    const group = btnGroup(container, [["star", "COUNT(*)"], ["col", "COUNT(product)"], ["tuple", "COUNT(DISTINCT region, product)"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      rows.forEach((r) => {
        const active = mode === "star" || (mode === "col" && !r.includes("NULL)")) || (mode === "tuple" && !r.includes("(NULL") && !r.includes(", NULL"));
        chip(row, r, active
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      });
      const msg = mode === "star" ? "All four rows count." : mode === "col" ? "Rows where product is NULL drop out." : "Any NULL in the distinct tuple removes that row before deduplication.";
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(msg);
    }
    render(group.get());
  }

  function renderMinMaxTypeOrder(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      numeric: ["80 < 120 < 450", "MIN = 80", "MAX = 450"],
      string: ["'10' < '9' is false", "MIN('East','West','North') = East", "MAX(...) = West"],
      date: ["2024-01-15 < 2024-03-25", "MIN(date) = earliest", "MAX(date) = latest"],
      greatest: ["GREATEST(10, NULL, 5) = 10", "LEAST(10, NULL, 5) = 5", "all NULL -> NULL"],
    };
    const group = btnGroup(container, [["numeric", "numeric"], ["string", "string"], ["date", "date"], ["greatest", "GREATEST/LEAST"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
      modes[mode].forEach((r, idx) => chip(row, r, idx === 0
        ? { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }
        : { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE }));
    }
    render(group.get());
  }

  function renderSumOverflowTypes(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      int: { rows: ["SUM(INT) -> BIGINT", "2 rows of 1 -> 2"] },
      float: { rows: ["SUM(FLOAT) -> DOUBLE", "2 rows of 1.5 -> 3.0"] },
      decimal: { rows: ["SUM(DECIMAL(10,2)) -> DECIMAL(20,2)", "2 rows of 1.23 -> 2.46"] },
      overflow: { rows: ["ANSI on -> ARITHMETIC_OVERFLOW", "ANSI off -> -9223372036854775808"] },
    };
    const group = btnGroup(container, [["int", "integral"], ["float", "float"], ["decimal", "decimal"], ["overflow", "overflow"]], render);
    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
      modes[mode].rows.forEach((r, idx) => chip(row, r, idx === 0
        ? { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE }
        : mode === "overflow"
          ? { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED }
          : { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-agg-index-map", renderAggIndexMap],
      ["viz-group-by-flow", renderGroupByFlow],
      ["viz-stats-distribution", renderStatsDistribution],
      ["viz-olap-cube-levels", renderCubeLevels],
      ["viz-olap-grouping-sets", renderGroupingSets],
      ["viz-olap-rollup-path", renderRollupPath],
      ["viz-pivot-case-cells", renderPivotCaseCells],
      ["viz-pivot-native-layout", renderPivotNativeLayout],
      ["viz-unpivot-rows", renderUnpivotRows],
      ["viz-simple-aggregate-null", renderSimpleAggregateNull],
      ["viz-avg-type-behavior", renderAvgTypeBehavior],
      ["viz-approx-count-rsd", renderApproxCountRsd],
      ["viz-count-null-rules", renderCountNullRules],
      ["viz-minmax-type-order", renderMinMaxTypeOrder],
      ["viz-sum-overflow-types", renderSumOverflowTypes],
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
