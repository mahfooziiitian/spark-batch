/**
 * aggregate-viz.js
 * D3 v7 interactive visualizations for the Aggregate Functions pages
 * (docs/functions/aggregate/{index,simple,count,any-value,first,last,
 * every,group,stats,strings,array,map}.md).
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

  /* 1. Category explorer (index.md) */
  function renderAggregateCategories(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const cats = {
      Simple: { fns: ["SUM", "AVG", "MIN", "MAX", "MEAN"], note: "Basic numeric reduction, NULLs skipped." },
      Count: { fns: ["COUNT", "COUNT_IF", "APPROX_COUNT_DISTINCT"], note: "Row / distinct-value counting." },
      "First/Last": { fns: ["FIRST", "LAST", "FIRST_VALUE", "LAST_VALUE"], note: "Positional retrieval — non-deterministic without ORDER BY." },
      Boolean: { fns: ["EVERY", "BOOL_AND", "SOME", "BOOL_OR"], note: "All-true / any-true group checks." },
      Array: { fns: ["ARRAY_AGG", "COLLECT_LIST", "COLLECT_SET"], note: "Collect grouped values into an array." },
      Map: { fns: ["MAP_FROM_ENTRIES", "MAP_FROM_ARRAYS"], note: "Pivot rows into a single map value." },
      "Group identity": { fns: ["GROUPING", "GROUPING_ID"], note: "Distinguish real NULLs from ROLLUP/CUBE super-aggregate NULLs." },
      Statistics: { fns: ["STDDEV", "VARIANCE", "CORR", "PERCENTILE", "MEDIAN"], note: "Descriptive statistics; sample vs population variants." },
    };
    const group = btnGroup(container, Object.keys(cats).map((k) => [k, k]), render);
    function render(cat) {
      stage.selectAll("*").remove();
      const c = cats[cat];
      const row = stage.append("div").style("display", "flex").style("flex-wrap", "wrap").style("gap", "4px").style("margin-bottom", "6px");
      c.fns.forEach((f) => chip(row, f, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE }));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(c.note);
    }
    render(group.get());
  }

  /* 2. NULL skipping in simple aggregates (simple.md) */
  function renderNullSkip(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let hasNull = false;
    const values = [10, 20, 30];

    const toggleRow = container.append("div").style("margin-bottom", "0.6rem");
    const btn = toggleRow.append("button")
      .style("padding", "5px 14px").style("border-radius", "5px").style("cursor", "pointer").style("font-size", "0.76rem");

    const rowsWrap = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "8px");
    const statsWrap = container.append("div").style("font-family", "monospace").style("font-size", "0.8rem");

    function update() {
      btn.text(hasNull ? "Remove the NULL row" : "Add a NULL row").style("background", hasNull ? RED : "#fff").style("color", hasNull ? "#fff" : FG).style("border", `1px solid ${hasNull ? RED : "#ccc"}`);
      const rows = hasNull ? [...values, null] : values;
      rowsWrap.selectAll("*").remove();
      rows.forEach((v) => chip(rowsWrap, v === null ? "NULL" : String(v), v === null
        ? { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }
        : { bg: "rgba(38,166,154,0.1)", fg: HIT_COLOR, border: HIT_COLOR }));
      const nonNull = rows.filter((v) => v !== null);
      const sum = nonNull.reduce((a, b) => a + b, 0);
      const avg = sum / nonNull.length;
      statsWrap.html(`COUNT(*) = ${rows.length} &nbsp;&nbsp; COUNT(col) = ${nonNull.length} &nbsp;&nbsp; SUM(col) = ${sum} &nbsp;&nbsp; AVG(col) = ${avg.toFixed(2)}`);
    }
    btn.on("click", () => { hasNull = !hasNull; update(); });
    update();
  }

  /* 3. COUNT(DISTINCT a, b) tuple counting (count.md) */
  function renderCountDistinct(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const rows = [[1, 1], [1, 1], [1, 2], [2, 1]];

    const group = btnGroup(container, [["single", "COUNT(DISTINCT a)"], ["tuple", "COUNT(DISTINCT a, b)"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const rowsWrap = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px").style("margin-bottom", "8px");
      const seen = new Set();
      let count = 0;
      rows.forEach(([a, b]) => {
        const key = mode === "single" ? String(a) : `${a},${b}`;
        const isNew = !seen.has(key);
        if (isNew) { seen.add(key); count++; }
        const r = rowsWrap.append("div").style("display", "flex").style("gap", "6px").style("align-items", "center");
        chip(r, `(a=${a}, b=${b})`, { bg: "#f5f5f7", border: "#ccc" });
        chip(r, isNew ? `new: ${key}` : `dup: ${key}`, isNew
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "rgba(239,83,80,0.08)", fg: RED, border: RED });
      });
      stage.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-top", "4px")
        .text(`Result: ${count}`);
    }
    render(group.get());
  }

  /* 4. ANY_VALUE non-determinism (any-value.md) */
  function renderAnyValueRuns(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const candidates = ["Alice", "Bob", "Eve"];
    const btn = container.append("button")
      .style("padding", "5px 14px").style("border-radius", "5px").style("cursor", "pointer").style("font-size", "0.76rem").style("margin-bottom", "0.6rem")
      .text("Run again ▶");
    const log = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px");
    let runNum = 0;
    function runOnce() {
      runNum++;
      const pick = candidates[Math.floor(Math.random() * candidates.length)];
      const row = log.append("div").style("display", "flex").style("gap", "6px").style("align-items", "center");
      row.append("span").style("font-size", "0.68rem").style("color", GRAY).style("width", "50px").text(`run ${runNum}:`);
      chip(row, `ANY_VALUE(rep) = '${pick}'`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      if (log.selectChildren().size() > 5) log.select(":first-child").remove();
    }
    btn.on("click", runOnce);
    runOnce(); runOnce();
  }

  /* 5. First/Last window frame boundaries (first.md, last.md) */
  function renderFirstLastFrame(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const rowsData = [
      { date: "01-01", amount: null },
      { date: "01-02", amount: 100 },
      { date: "01-03", amount: 300 },
      { date: "01-04", amount: null },
    ];
    const group = btnGroup(container, [["unbounded", "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"], ["running", "UNBOUNDED PRECEDING AND CURRENT ROW"]], render);
    const stage = container.append("div");

    function render(mode) {
      stage.selectAll("*").remove();
      rowsData.forEach((r, i) => {
        const inFrame = mode === "unbounded" ? true : true; // running frame always includes rows up to i, we show cumulative
        const frameRows = mode === "unbounded" ? rowsData : rowsData.slice(0, i + 1);
        const firstNonNull = frameRows.find((x) => x.amount !== null);
        const lastNonNull = [...frameRows].reverse().find((x) => x.amount !== null);
        const row = stage.append("div").style("display", "flex").style("align-items", "center").style("gap", "6px").style("margin-bottom", "4px");
        chip(row, `${r.date}: ${r.amount === null ? "NULL" : r.amount}`, r.amount === null
          ? { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER }
          : { bg: "#f5f5f7", fg: FG, border: "#ccc" });
        row.append("span").style("font-size", "0.68rem").style("color", GRAY)
          .text(`→ FIRST_VALUE(true)=${firstNonNull ? firstNonNull.amount : "NULL"}, LAST_VALUE(true)=${lastNonNull ? lastNonNull.amount : "NULL"}`);
      });
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px")
        .text(mode === "unbounded"
          ? "Whole-partition frame: FIRST_VALUE/LAST_VALUE are the same on every row."
          : "Running frame: LAST_VALUE(true) changes row-by-row — 'most recent non-NULL so far'.");
    }
    render(group.get());
  }

  /* 6. EVERY / SOME truth table (every.md) */
  function renderEverySome(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let vals = [true, null, true];
    const rowsWrap = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "8px");
    const resultWrap = container.append("div").style("font-family", "monospace").style("font-size", "0.8rem");

    function cycle(v) { return v === true ? false : v === false ? null : true; }

    function update() {
      rowsWrap.selectAll("*").remove();
      vals.forEach((v, i) => {
        const label = v === null ? "NULL" : String(v);
        chip(rowsWrap, label, v === null
          ? { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }
          : v ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR } : { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED })
          .style("cursor", "pointer")
          .on("click", () => { vals[i] = cycle(v); update(); });
      });
      const nonNull = vals.filter((v) => v !== null);
      const every = nonNull.length > 0 ? nonNull.every((v) => v) : null;
      const some = nonNull.length > 0 ? nonNull.some((v) => v) : null;
      resultWrap.html(`EVERY = <b style="color:${every ? HIT_COLOR : RED}">${every === null ? "NULL" : every}</b> &nbsp;&nbsp; SOME = <b style="color:${some ? HIT_COLOR : RED}">${some === null ? "NULL" : some}</b>`);
    }
    container.append("div").style("font-size", "0.68rem").style("color", GRAY).style("margin-bottom", "4px").text("Click a chip to cycle true → false → NULL → true");
    update();
  }

  /* 7. GROUPING CUBE explorer (group.md) */
  function renderGroupingCube(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const rows = [
      { name: "Alice", height: 165, gName: 0, gHeight: 0, gid: 0 },
      { name: "Bob", height: 180, gName: 0, gHeight: 0, gid: 0 },
      { name: null, height: 165, gName: 1, gHeight: 0, gid: 2 },
      { name: null, height: 180, gName: 1, gHeight: 0, gid: 2 },
      { name: "Alice", height: null, gName: 0, gHeight: 1, gid: 1 },
      { name: "Bob", height: null, gName: 0, gHeight: 1, gid: 1 },
      { name: null, height: null, gName: 1, gHeight: 1, gid: 3 },
    ];
    const stage = container.append("div");
    rows.forEach((r) => {
      const row = stage.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "4px");
      chip(row, `name=${r.name === null ? "NULL" : r.name}`, r.name === null ? { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER } : { bg: "#f5f5f7", border: "#ccc" });
      chip(row, `height=${r.height === null ? "NULL" : r.height}`, r.height === null ? { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER } : { bg: "#f5f5f7", border: "#ccc" });
      chip(row, `GROUPING_ID=${r.gid}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      row.append("span").style("font-size", "0.68rem").style("color", GRAY)
        .text(r.gid === 0 ? "grouped by both" : r.gid === 1 ? "grouped by name only" : r.gid === 2 ? "grouped by height only" : "grand total");
    });
  }

  /* 8. Percentile marker on distribution (stats.md) */
  function renderPercentileMarker(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const data = [1, 2, 2, 3, 4, 5, 5, 6, 7, 8, 9, 10];
    let p = 0.5;

    const row = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "0.6rem");
    row.append("span").style("font-size", "0.76rem").style("color", FG).text("percentile p =");
    const label = row.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px").text(p.toFixed(2));
    row.append("input").attr("type", "range").attr("min", 0).attr("max", 100).attr("value", p * 100).style("width", "160px")
      .on("input", function () { p = +this.value / 100; update(); });

    const barWrap = container.append("div").style("display", "flex").style("align-items", "flex-end").style("gap", "3px").style("height", "80px");
    const resultWrap = container.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-top", "6px");

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
      barWrap.selectAll("*").remove();
      sorted.forEach((v) => {
        const near = Math.abs(v - val) < 0.5;
        barWrap.append("div").style("width", "16px").style("height", `${v * 7}px`)
          .style("background", near ? HIT_COLOR : "#e0e0e0").style("border-radius", "2px 2px 0 0")
          .attr("title", v);
      });
      resultWrap.html(`PERCENTILE(col, ${p.toFixed(2)}) ≈ <b style="color:${HIT_COLOR}">${val.toFixed(2)}</b>${Math.abs(p - 0.5) < 0.001 ? " &nbsp;(= MEDIAN)" : ""}`);
    }
    update();
  }

  /* 9. String aggregation pipeline (strings.md) */
  function renderStringAggPipeline(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const names = ["Bob", "Alice", "Charlie"];
    const stage = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "8px");

    function step(label, chips, color) {
      const row = stage.append("div");
      row.append("div").style("font-size", "0.68rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "3px").text(label);
      const c = row.append("div").style("display", "flex").style("gap", "4px").style("flex-wrap", "wrap");
      chips.forEach((v) => chip(c, v, { bg: `${color}1a`, fg: color, border: color }));
    }
    step("1. COLLECT_LIST(name) — order not guaranteed", names, GRAY);
    step("2. SORT_ARRAY(...) — deterministic order", [...names].sort(), PURPLE);
    stage.append("div").style("font-family", "monospace").style("font-size", "0.8rem")
      .html(`3. CONCAT_WS(', ', ...) → <span style="color:${HIT_COLOR}">'${[...names].sort().join(", ")}'</span>`);
  }

  /* 10. array_agg ORDER BY unsupported vs workaround (array.md) */
  function renderArrayAggOrderBy(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const group = btnGroup(container, [["standard", "Standard SQL syntax"], ["sort_after", "Sort after aggregate"], ["window", "Ordered window frame"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      if (mode === "standard") {
        stage.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("margin-bottom", "6px")
          .text("ARRAY_AGG(col ORDER BY col DESC)");
        chip(stage, "✕ [PARSE_SYNTAX_ERROR] not supported in Spark", { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      } else if (mode === "sort_after") {
        stage.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("margin-bottom", "6px")
          .text("SORT_ARRAY(ARRAY_AGG(col), false)");
        chip(stage, "✓ [3, 2, 1]", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      } else {
        stage.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("margin-bottom", "6px")
          .text("ARRAY_AGG(col) OVER (ORDER BY sort_col ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
        chip(stage, "✓ works — needed when sort key ≠ collected column", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      }
    }
    render(group.get());
  }

  /* 11. MAP pivot from rows (map.md) */
  function renderMapPivot(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const rows = [["timeout", "30"], ["retries", "3"]];
    const stage = container.append("div");

    stage.append("div").style("font-size", "0.68rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px").text("Rows (key, value)");
    const rowsWrap = stage.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "8px");
    rows.forEach(([k, v]) => chip(rowsWrap, `(${k}, ${v})`, { bg: "#f5f5f7", border: "#ccc" }));

    stage.append("div").style("text-align", "center").style("color", GRAY).style("font-size", "0.85rem").style("margin", "4px 0").text("↓ COLLECT_LIST(STRUCT(key,value)) → MAP_FROM_ENTRIES(...) ↓");

    stage.append("div").style("font-family", "monospace").style("font-size", "0.8rem")
      .html(`<span style="color:${HIT_COLOR}">{${rows.map(([k, v]) => `${k} -> ${v}`).join(", ")}}</span>  (one map value per group)`);
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-aggregate-categories", renderAggregateCategories],
      ["viz-null-skip", renderNullSkip],
      ["viz-count-distinct", renderCountDistinct],
      ["viz-any-value-runs", renderAnyValueRuns],
      ["viz-first-last-frame", renderFirstLastFrame],
      ["viz-every-some", renderEverySome],
      ["viz-grouping-cube", renderGroupingCube],
      ["viz-percentile-marker", renderPercentileMarker],
      ["viz-string-agg-pipeline", renderStringAggPipeline],
      ["viz-array-agg-orderby", renderArrayAggOrderBy],
      ["viz-map-pivot", renderMapPivot],
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
