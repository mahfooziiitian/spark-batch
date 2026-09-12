/**
 * querying-nulls-viz.js
 * D3 v7 interactive visualizations for docs/querying/nulls/*.md.
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

  function valueColor(v) {
    if (v === "NULL") return RED;
    if (v === "TRUE") return HIT_COLOR;
    if (v === "FALSE") return AMBER;
    return PURPLE;
  }

  function renderAggregate(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const rows = [30, null, 18, 50, null, 30, 50];
    const specs = {
      countAll: { label: "COUNT(*)", included: rows.map((_, i) => i), result: "7" },
      countAge: { label: "COUNT(age)", included: rows.map((v, i) => v === null ? null : i).filter((v) => v !== null), result: "5" },
      sumAge: { label: "SUM(age)", included: rows.map((v, i) => v === null ? null : i).filter((v) => v !== null), result: "178" },
      avgAge: { label: "AVG(age)", included: rows.map((v, i) => v === null ? null : i).filter((v) => v !== null), result: "35.6" },
    };
    const stage = container.append("div");
    const group = btnGroup(container, [["countAll", "COUNT(*)"], ["countAge", "COUNT(age)"], ["sumAge", "SUM(age)"], ["avgAge", "AVG(age)"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      rows.forEach((v, i) => {
        const hit = spec.included.includes(i);
        chip(row, v === null ? "NULL" : String(v), hit
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      });
      box(stage, `${spec.label} => ${spec.result}`, { bg: "#fff", border: PURPLE, fg: FG });
    }
    render(group.get());
  }

  function renderCheck(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const specs = {
      isNull: { sql: "NULL IS NULL", result: "TRUE" },
      isNotNull: { sql: "NULL IS NOT NULL", result: "FALSE" },
      coalesce: { sql: "COALESCE(NULL, NULL, 7)", result: "7" },
      nullif: { sql: "NULLIF(5, 5)", result: "NULL" },
      ifnull: { sql: "IFNULL(NULL, 'x')", result: "x" },
      nvl: { sql: "NVL(NULL, 'y')", result: "y" },
      nvl2: { sql: "NVL2(NULL, 'yes', 'no')", result: "no" },
    };
    const stage = container.append("div");
    const group = btnGroup(container, [["isNull", "IS NULL"], ["isNotNull", "IS NOT NULL"], ["coalesce", "COALESCE"], ["nullif", "NULLIF"], ["ifnull", "IFNULL"], ["nvl", "NVL"], ["nvl2", "NVL2"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      box(stage, spec.sql, { bg: "#fff", border: "#ccc" });
      stage.append("div").style("margin-top", "8px");
      chip(stage.append("div"), `Result: ${spec.result}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
    }
    render(group.get());
  }

  function renderComparison(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const specs = {
      eq: { sql: "NULL = NULL", result: "NULL" },
      nullsafe: { sql: "NULL <=> NULL", result: "TRUE" },
      mixed: { sql: "5 <=> NULL", result: "FALSE" },
      indf: { sql: "NULL IS NOT DISTINCT FROM NULL", result: "TRUE" },
      isnull: { sql: "NULL IS NULL", result: "TRUE" },
    };
    const stage = container.append("div");
    const group = btnGroup(container, [["eq", "NULL = NULL"], ["nullsafe", "NULL <=> NULL"], ["mixed", "5 <=> NULL"], ["indf", "IS NOT DISTINCT FROM"], ["isnull", "IS NULL"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      box(stage, spec.sql, { bg: "#fff", border: "#ccc" });
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("margin-top", "8px");
      chip(row, `Result: ${spec.result}`, { bg: `${valueColor(spec.result)}22`, fg: valueColor(spec.result), border: valueColor(spec.result) });
    }
    render(group.get());
  }

  function renderExpression(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const specs = {
      concat: { sql: "CONCAT('John', NULL)", kind: "Null-intolerant", result: "NULL" },
      plus: { sql: "CAST(NULL AS INT) + 5", kind: "Null-intolerant", result: "NULL" },
      upper: { sql: "UPPER(NULL)", kind: "Null-intolerant", result: "NULL" },
      concatWs: { sql: "CONCAT_WS(',', 'a', NULL, 'b')", kind: "Null-aware", result: "a,b" },
      nvl2: { sql: "NVL2(NULL, 'yes', 'no')", kind: "Null-aware", result: "no" },
      nanvl: { sql: "NANVL(NaN, 0.0)", kind: "NaN-aware", result: "0.0" },
    };
    const stage = container.append("div");
    const group = btnGroup(container, [["concat", "CONCAT"], ["plus", "+"], ["upper", "UPPER"], ["concatWs", "CONCAT_WS"], ["nvl2", "NVL2"], ["nanvl", "NANVL"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      chip(stage.append("div").style("margin-bottom", "8px"), spec.kind, { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY });
      box(stage, spec.sql, { bg: "#fff", border: "#ccc" });
      chip(stage.append("div").style("margin-top", "8px"), `Result: ${spec.result}`, { bg: `${valueColor(spec.result)}22`, fg: valueColor(spec.result), border: valueColor(spec.result) });
    }
    render(group.get());
  }

  function renderFilter(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const rows = [
      { name: "Joe", age: 30 },
      { name: "Marry", age: null },
      { name: "Mike", age: 18 },
      { name: "Fred", age: 50 },
      { name: "Albert", age: null },
    ];
    const stage = container.append("div");
    const specs = {
      gt0: { label: "age > 0", evalFn: (r) => r.age === null ? "NULL" : (r.age > 0 ? "TRUE" : "FALSE") },
      gt0OrNull: { label: "age > 0 OR age IS NULL", evalFn: (r) => r.age === null ? "TRUE" : (r.age > 0 ? "TRUE" : "FALSE") },
      isNull: { label: "age IS NULL", evalFn: (r) => r.age === null ? "TRUE" : "FALSE" },
    };
    const group = btnGroup(container, [["gt0", "age > 0"], ["gt0OrNull", "age > 0 OR age IS NULL"], ["isNull", "age IS NULL"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      box(stage, `WHERE ${spec.label}`, { bg: "#fff", border: "#ccc" });
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-top", "8px");
      rows.forEach((r) => {
        const out = spec.evalFn(r);
        const keep = out === "TRUE";
        chip(row, `${r.name}: ${r.age === null ? "NULL" : r.age} => ${out}`, keep
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: `${valueColor(out)}22`, fg: valueColor(out), border: valueColor(out) });
      });
    }
    render(group.get());
  }

  function renderLogical(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    let left = "TRUE";
    let right = "NULL";
    let op = "AND";

    function evalBinary(a, b, oper) {
      if (oper === "AND") {
        if (a === "FALSE" || b === "FALSE") return "FALSE";
        if (a === "TRUE" && b === "TRUE") return "TRUE";
        return "NULL";
      }
      if (a === "TRUE" || b === "TRUE") return "TRUE";
      if (a === "FALSE" && b === "FALSE") return "FALSE";
      return "NULL";
    }

    btnGroup(container.append("div"), [["AND", "AND"], ["OR", "OR"], ["NOT", "NOT"]], (v) => { op = v; render(); });
    btnGroup(container.append("div"), [["TRUE", "Left TRUE"], ["FALSE", "Left FALSE"], ["NULL", "Left NULL"]], (v) => { left = v; render(); });
    btnGroup(container.append("div"), [["TRUE", "Right TRUE"], ["FALSE", "Right FALSE"], ["NULL", "Right NULL"]], (v) => { right = v; render(); });

    function render() {
      stage.selectAll("*").remove();
      const sql = op === "NOT" ? `NOT ${left}` : `${left} ${op} ${right}`;
      const result = op === "NOT"
        ? (left === "TRUE" ? "FALSE" : left === "FALSE" ? "TRUE" : "NULL")
        : evalBinary(left, right, op);
      box(stage, sql, { bg: "#fff", border: "#ccc" });
      chip(stage.append("div").style("margin-top", "8px"), `Result: ${result}`, { bg: `${valueColor(result)}22`, fg: valueColor(result), border: valueColor(result) });
    }
    render();
  }

  function renderOperator(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const rows = ["30", "NULL", "18", "50", "NULL", "30", "50"];
    const stage = container.append("div");
    const group = btnGroup(container, [["groupBy", "GROUP BY"], ["distinct", "DISTINCT"], ["partitionBy", "PARTITION BY"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const src = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      rows.forEach((r) => chip(src, r, { bg: "#fff", border: "#ccc" }));
      if (mode === "distinct") {
        const uniq = ["NULL", "18", "30", "50"];
        const out = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
        uniq.forEach((r) => chip(out, r, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE }));
        return;
      }
      const groups = [
        { key: "NULL", count: 2 },
        { key: "18", count: 1 },
        { key: "30", count: 2 },
        { key: "50", count: 2 },
      ];
      groups.forEach((g) => {
        chip(stage.append("div").style("margin-bottom", "6px"), `${g.key} => ${g.count} row(s)`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      });
    }
    render(group.get());
  }

  function renderOrdering(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    let direction = "ASC";
    let nulls = "DEFAULT";
    const rows = [30, null, 18, 50, null];

    btnGroup(container.append("div"), [["ASC", "ASC"], ["DESC", "DESC"]], (v) => { direction = v; render(); });
    btnGroup(container.append("div"), [["DEFAULT", "Default"], ["FIRST", "NULLS FIRST"], ["LAST", "NULLS LAST"]], (v) => { nulls = v; render(); });

    function compare(a, b) {
      const aNull = a === null;
      const bNull = b === null;
      const nullPos = nulls === "DEFAULT" ? (direction === "ASC" ? "FIRST" : "LAST") : nulls;
      if (aNull || bNull) {
        if (aNull && bNull) return 0;
        if (nullPos === "FIRST") return aNull ? -1 : 1;
        return aNull ? 1 : -1;
      }
      return direction === "ASC" ? a - b : b - a;
    }

    function render() {
      stage.selectAll("*").remove();
      const sorted = [...rows].sort(compare);
      box(stage, `ORDER BY age ${direction}${nulls === "DEFAULT" ? "" : ` NULLS ${nulls}`}`, { bg: "#fff", border: "#ccc" });
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-top", "8px");
      sorted.forEach((v) => chip(row, v === null ? "NULL" : String(v), v === null
        ? { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED }
        : { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
    }
    render();
  }

  function renderSets(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const left = ["1", "NULL", "NULL"];
    const right = ["NULL", "2"];
    const specs = {
      union: { label: "UNION", result: ["NULL", "1", "2"] },
      unionAll: { label: "UNION ALL", result: ["NULL", "NULL", "NULL", "1", "2"] },
      intersect: { label: "INTERSECT", result: ["NULL"] },
      except: { label: "EXCEPT", result: ["1"] },
    };
    const group = btnGroup(container, [["union", "UNION"], ["unionAll", "UNION ALL"], ["intersect", "INTERSECT"], ["except", "EXCEPT"]], render);

    function drawRow(parent, label, values, color) {
      parent.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin", "4px 0").text(label);
      const row = parent.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      values.forEach((v) => chip(row, v, { bg: `${color}22`, fg: color, border: color }));
    }

    function render(key) {
      stage.selectAll("*").remove();
      drawRow(stage, "Left input", left, PURPLE);
      drawRow(stage, "Right input", right, AMBER);
      drawRow(stage, specs[key].label, specs[key].result, HIT_COLOR);
    }
    render(group.get());
  }

  function renderSubquery(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    let outer = "30";
    let predicate = "IN";
    const subquery = [50, null];

    btnGroup(container.append("div"), [["30", "Outer 30"], ["50", "Outer 50"], ["NULL", "Outer NULL"]], (v) => { outer = v; render(); });
    btnGroup(container.append("div"), [["IN", "IN"], ["NOT IN", "NOT IN"], ["NOT EXISTS", "NOT EXISTS"]], (v) => { predicate = v; render(); });

    function render() {
      stage.selectAll("*").remove();
      drawSubquery(stage);
      const result = evaluate();
      box(stage, result.sql, { bg: "#fff", border: "#ccc" });
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("margin-top", "8px");
      chip(row, `Predicate result: ${result.value}`, { bg: `${valueColor(result.value)}22`, fg: valueColor(result.value), border: valueColor(result.value) });
      chip(row, result.kept ? "WHERE keeps row" : "WHERE drops row", result.kept
        ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
    }

    function drawSubquery(parent) {
      parent.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "4px").text("Subquery result: [50, NULL]");
      const row = parent.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "8px");
      subquery.forEach((v) => chip(row, v === null ? "NULL" : String(v), v === null
        ? { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED }
        : { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }));
    }

    function evaluate() {
      const outerValue = outer === "NULL" ? null : Number(outer);
      if (predicate === "NOT EXISTS") {
        const found = outerValue !== null && subquery.some((v) => v !== null && v === outerValue);
        return { sql: `${outer} with NOT EXISTS`, value: found ? "FALSE" : "TRUE", kept: !found };
      }
      const match = outerValue !== null && subquery.some((v) => v !== null && v === outerValue);
      const hasNull = subquery.some((v) => v === null);
      if (predicate === "IN") {
        if (match) return { sql: `${outer} IN (50, NULL)`, value: "TRUE", kept: true };
        return { sql: `${outer} IN (50, NULL)`, value: hasNull ? "NULL" : "FALSE", kept: false };
      }
      if (match) return { sql: `${outer} NOT IN (50, NULL)`, value: "FALSE", kept: false };
      return { sql: `${outer} NOT IN (50, NULL)`, value: hasNull ? "NULL" : "TRUE", kept: false };
    }

    render();
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-null-aggregate", renderAggregate],
      ["viz-null-check", renderCheck],
      ["viz-null-comparison", renderComparison],
      ["viz-null-expression", renderExpression],
      ["viz-null-filter", renderFilter],
      ["viz-null-logical", renderLogical],
      ["viz-null-operator", renderOperator],
      ["viz-null-ordering", renderOrdering],
      ["viz-null-sets", renderSets],
      ["viz-null-subquery", renderSubquery],
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
