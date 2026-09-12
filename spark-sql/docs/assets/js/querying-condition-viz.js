/**
 * querying-condition-viz.js
 * D3 v7 interactive visualizations for the Querying > Condition pages.
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

  function truthColor(value) {
    if (value === "TRUE") return { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR };
    if (value === "FALSE") return { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED };
    return { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER };
  }

  function renderCaseWhen(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const cases = {
      searched: {
        rows: [
          { label: "total_spend = 12000", hit: "WHEN total_spend >= 10000 THEN 'Platinum'", result: "Platinum", note: "First matching branch wins." },
          { label: "total_spend = 3500", hit: "WHEN total_spend >= 1000 THEN 'Silver'", result: "Silver", note: "Spark keeps checking until one branch is true." },
          { label: "total_spend = NULL", hit: "ELSE 'Standard'", result: "Standard", note: "Nullable comparisons do not match unless you handle NULL explicitly." },
          { label: "no ELSE + no match", hit: "implicit ELSE NULL", result: "NULL", note: "Verified in PySpark 4.2: unmatched CASE without ELSE returns NULL." }
        ],
        sql: `CASE
  WHEN total_spend >= 10000 THEN 'Platinum'
  WHEN total_spend >= 1000 THEN 'Silver'
  ELSE 'Standard'
END`
      },
      simple: {
        rows: [
          { label: "status_code = 2", hit: "WHEN 2 THEN 'Processing'", result: "Processing", note: "Simple CASE is concise for equality mapping." },
          { label: "status_code = NULL", hit: "ELSE 'Unknown'", result: "Unknown", note: "Verified in PySpark 4.2: CASE expr WHEN NULL does not match NULL." },
          { label: "status_code = 9", hit: "ELSE 'Unknown'", result: "Unknown", note: "Unmatched values fall through to ELSE." }
        ],
        sql: `CASE status_code
  WHEN 1 THEN 'Pending'
  WHEN 2 THEN 'Processing'
  ELSE 'Unknown'
END`
      }
    };
    const layout = container.append("div");
    const group = btnGroup(container, [["searched", "Searched CASE"], ["simple", "Simple CASE"]], render);

    function render(mode) {
      const cfg = cases[mode];
      layout.selectAll("*").remove();
      box(layout, cfg.sql, { bg: "rgba(124,77,255,0.06)", border: PURPLE, fg: FG });
      const rowWrap = layout.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "8px").style("margin-top", "10px");
      cfg.rows.forEach((row) => {
        const card = rowWrap.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px").style("padding", "10px").style("border", "1px solid #e0e0e0").style("border-radius", "8px");
        const top = card.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
        chip(top, row.label, { bg: "#f5f5f7", border: "#ccc" });
        chip(top, row.hit, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
        chip(top, `result = ${row.result}`, truthColor(row.result === "NULL" ? "NULL" : "TRUE"));
        card.append("div").style("font-size", "0.72rem").style("color", GRAY).text(row.note);
      });
    }
    render(group.get());
  }

  function compareValue(a, b, op) {
    if (op === "eq") return a === "NULL" || b === "NULL" ? "NULL" : String(a === b).toUpperCase();
    if (op === "neq") return a === "NULL" || b === "NULL" ? "NULL" : String(a !== b).toUpperCase();
    if (op === "nseq") {
      if (a === "NULL" && b === "NULL") return "TRUE";
      if (a === "NULL" || b === "NULL") return "FALSE";
      return String(a === b).toUpperCase();
    }
    if (op === "idf") {
      if (a === "NULL" && b === "NULL") return "FALSE";
      if (a === "NULL" || b === "NULL") return "TRUE";
      return String(a !== b).toUpperCase();
    }
    if (op === "indf") return compareValue(a, b, "nseq");
    return "NULL";
  }

  function renderComparison(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let left = "NULL";
    let right = "NULL";
    const stage = container.append("div");
    const leftGroup = btnGroup(container.append("div"), [["NULL", "left = NULL"], ["1", "left = 1"], ["2", "left = 2"]], (v) => { left = v; draw(); });
    const rightGroup = btnGroup(container.append("div"), [["NULL", "right = NULL"], ["1", "right = 1"], ["2", "right = 2"]], (v) => { right = v; draw(); });
    const opGroup = btnGroup(container, [["eq", "="], ["neq", "<>"], ["nseq", "<=>"], ["idf", "IS DISTINCT FROM"], ["indf", "IS NOT DISTINCT FROM"]], draw);

    function draw() {
      left = leftGroup.get();
      right = rightGroup.get();
      const op = opGroup.get();
      const result = compareValue(left, right, op);
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("align-items", "center").style("margin-bottom", "8px");
      chip(row, left, truthColor(left));
      chip(row, op === "eq" ? "=" : op === "neq" ? "<>" : op === "nseq" ? "<=>" : op === "idf" ? "IS DISTINCT FROM" : "IS NOT DISTINCT FROM", { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      chip(row, right, truthColor(right));
      chip(row, result, truthColor(result));
      const note = op === "eq" || op === "neq"
        ? "Ordinary comparisons return NULL as soon as either operand is NULL."
        : op === "nseq" || op === "indf"
          ? "Null-safe equality treats NULL = NULL as TRUE."
          : "IS DISTINCT FROM treats NULL and non-NULL as different values.";
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(note);
    }
    draw();
  }

  function renderIfIif(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const modes = {
      IF: {
        sql: "IF(cond, true_val, false_val)",
        cases: [
          ["cond = TRUE", "returns true_val"],
          ["cond = FALSE", "returns false_val"],
          ["cond = NULL", "verified: returns false_val"]
        ]
      },
      IFNULL: {
        sql: "IFNULL(expr, replacement)",
        cases: [
          ["expr = 5", "returns 5"],
          ["expr = NULL", "returns replacement"],
          ["replacement = raise_error('boom')", "evaluated only when expr is NULL"]
        ]
      },
      NULLIF: {
        sql: "NULLIF(expr, comparand)",
        cases: [
          ["expr = 5, comparand = 5", "returns NULL"],
          ["expr = 5, comparand = 6", "returns 5"],
          ["comparand = raise_error('boom')", "verified: Spark still evaluates it"]
        ]
      },
      COALESCE: {
        sql: "COALESCE(e1, e2, e3, ...)",
        cases: [
          ["e1 = 'Alice'", "returns 'Alice' immediately"],
          ["e1 = NULL, e2 = 'Bob'", "returns 'Bob'"],
          ["later arg = raise_error('boom')", "evaluated only if every earlier arg is NULL"]
        ]
      }
    };
    const stage = container.append("div");
    const group = btnGroup(container, Object.keys(modes).map((k) => [k, k]), render);

    function render(mode) {
      const cfg = modes[mode];
      stage.selectAll("*").remove();
      box(stage, cfg.sql, { bg: "rgba(38,166,154,0.08)", border: TEAL, fg: FG });
      const rows = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px").style("margin-top", "10px");
      cfg.cases.forEach(([lhs, rhs]) => {
        const row = rows.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
        chip(row, lhs, { bg: "#f5f5f7", border: "#ccc" });
        chip(row, rhs, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      });
      if (mode === "IF") {
        stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "8px")
          .text("Open-source Spark 4.2 supports IF(...), but not IIF(...). Keep IIF labeled as Databricks-only.");
      }
    }
    render(group.get());
  }

  function logicValue(a, b, op) {
    if (op === "NOT") return a === "NULL" ? "NULL" : (a === "TRUE" ? "FALSE" : "TRUE");
    if (op === "AND") {
      if (a === "FALSE" || b === "FALSE") return "FALSE";
      if (a === "NULL" || b === "NULL") return "NULL";
      return "TRUE";
    }
    if (op === "OR") {
      if (a === "TRUE" || b === "TRUE") return "TRUE";
      if (a === "NULL" || b === "NULL") return "NULL";
      return "FALSE";
    }
    return "NULL";
  }

  function renderLogical(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let left = "TRUE";
    let right = "NULL";
    const stage = container.append("div");
    const leftGroup = btnGroup(container.append("div"), [["TRUE", "A = TRUE"], ["FALSE", "A = FALSE"], ["NULL", "A = NULL"]], (v) => { left = v; draw(); });
    const rightGroup = btnGroup(container.append("div"), [["TRUE", "B = TRUE"], ["FALSE", "B = FALSE"], ["NULL", "B = NULL"]], (v) => { right = v; draw(); });
    const opGroup = btnGroup(container, [["AND", "A AND B"], ["OR", "A OR B"], ["NOT", "NOT A"]], draw);

    function draw() {
      left = leftGroup.get();
      right = rightGroup.get();
      const op = opGroup.get();
      const result = logicValue(left, right, op);
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("align-items", "center").style("margin-bottom", "8px");
      chip(row, left, truthColor(left));
      if (op !== "NOT") {
        chip(row, op, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
        chip(row, right, truthColor(right));
      } else {
        chip(row, "NOT", { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      }
      chip(row, result, truthColor(result));
      const note = op === "AND"
        ? "FALSE dominates AND: once one side is FALSE, the whole expression is FALSE."
        : op === "OR"
          ? "TRUE dominates OR: once one side is TRUE, the whole expression is TRUE."
          : "NOT flips TRUE and FALSE but leaves NULL unknown.";
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(note);
    }
    draw();
  }

  function renderPattern(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const samples = {
      like: [
        { expr: "'alice' LIKE 'A%'", result: "FALSE", note: "LIKE is case-sensitive." },
        { expr: "'' LIKE '%'", result: "TRUE", note: "% can match an empty string." },
        { expr: "'AB' LIKE 'A_'", result: "TRUE", note: "_ matches exactly one character." }
      ],
      ilike: [
        { expr: "'alice' ILIKE 'A%'", result: "TRUE", note: "ILIKE ignores case." },
        { expr: "'Admin' ILIKE 'admin'", result: "TRUE", note: "Useful for case-insensitive exact matches too." }
      ],
      escape: [
        { expr: "'10%' LIKE '%\\\\%%' ESCAPE '\\\\'", result: "TRUE", note: "ESCAPE lets % act as a literal percent sign." },
        { expr: "'A_B' LIKE 'A\\\\_B' ESCAPE '\\\\'", result: "TRUE", note: "ESCAPE also works for literal underscores." }
      ],
      rlike: [
        { expr: "'abc' RLIKE '^[A-Z]{3}$'", result: "FALSE", note: "RLIKE uses Java regex and is case-sensitive by default." },
        { expr: "'abc' RLIKE '(?i)^[A-Z]{3}$'", result: "TRUE", note: "Inline (?i) makes the regex case-insensitive." }
      ]
    };
    const stage = container.append("div");
    const group = btnGroup(container, [["like", "LIKE"], ["ilike", "ILIKE"], ["escape", "ESCAPE"], ["rlike", "RLIKE"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      samples[mode].forEach((item) => {
        const card = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px").style("padding", "10px").style("border", "1px solid #e0e0e0").style("border-radius", "8px").style("margin-bottom", "8px");
        const row = card.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
        chip(row, item.expr, { bg: "#f5f5f7", border: "#ccc" });
        chip(row, item.result, truthColor(item.result));
        card.append("div").style("font-size", "0.72rem").style("color", GRAY).text(item.note);
      });
    }
    render(group.get());
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-case-when", renderCaseWhen],
      ["viz-comparison", renderComparison],
      ["viz-if-iif", renderIfIif],
      ["viz-logical", renderLogical],
      ["viz-pattern", renderPattern],
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
