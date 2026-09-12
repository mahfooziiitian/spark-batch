/**
 * scalar-core-viz.js
 * D3 v7 interactive visualizations for scalar function pages
 * (string, math, null, control, conversion).
 * Compatible with MkDocs Material instant navigation.
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
      .style("display", "inline-flex")
      .style("align-items", "center")
      .style("padding", "5px 12px")
      .style("border-radius", "5px")
      .style("font-size", "0.74rem")
      .style("font-family", "monospace")
      .style("cursor", opts.clickable ? "pointer" : "default")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .style("opacity", opts.opacity != null ? opts.opacity : 1)
      .text(text);
  }

  function box(parent, title, opts) {
    opts = opts || {};
    const wrap = parent.append("div")
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .style("border-radius", "8px")
      .style("padding", "10px 12px")
      .style("background", opts.bg || "#fff")
      .style("min-width", opts.minWidth || "140px");
    wrap.append("div")
      .style("font-size", "0.68rem")
      .style("color", GRAY)
      .style("font-weight", "700")
      .style("margin-bottom", "4px")
      .text(title);
    return wrap;
  }

  function renderStringOps(el) {
    const modes = [
      {
        label: "delimiter '|'",
        sql: "split('a|b|c', '|')",
        delimiter: "regex alternation",
        result: ["a", "|", "b", "|", "c", ""],
        note: "Naive regex delimiter: the pipe is treated as alternation, not a literal character.",
        color: RED,
      },
      {
        label: "delimiter '\\|'",
        sql: "split('a|b|c', '\\|')",
        delimiter: "escaped literal pipe",
        result: ["a", "b", "c"],
        note: "Escaping the metacharacter produces the intended literal split.",
        color: HIT_COLOR,
      },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.6rem");
    modes.forEach((mode, i) => {
      tabs.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .style("cursor", "pointer")
        .on("click", () => { idx = i; update(); })
        .text(mode.label);
    });

    const sqlLine = container.append("div")
      .style("font-family", "monospace")
      .style("font-size", "0.8rem")
      .style("margin-bottom", "0.5rem")
      .style("color", FG);
    const infoRow = container.append("div")
      .style("display", "flex")
      .style("gap", "12px")
      .style("align-items", "flex-start")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.55rem");
    const resultRow = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap");
    const note = container.append("div")
      .style("font-size", "0.74rem")
      .style("margin-top", "0.55rem");

    function update() {
      const mode = modes[idx];
      tabs.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
      sqlLine.text(mode.sql);
      infoRow.selectAll("*").remove();
      resultRow.selectAll("*").remove();
      box(infoRow, "Delimiter meaning", { border: mode.color, bg: "#fff" })
        .append("div")
        .style("font-size", "0.82rem")
        .style("font-family", "monospace")
        .style("color", mode.color)
        .text(mode.delimiter);
      box(infoRow, "Result size", { border: GRAY, bg: "#fff" })
        .append("div")
        .style("font-size", "0.82rem")
        .style("font-family", "monospace")
        .style("color", FG)
        .text(String(mode.result.length));
      mode.result.forEach((item) => {
        chip(resultRow, item === "" ? "''" : item, {
          bg: mode.color === RED ? "rgba(239,83,80,0.08)" : "rgba(38,166,154,0.12)",
          fg: mode.color,
          border: mode.color,
        });
      });
      note.style("color", mode.color).text(mode.note);
    }
    update();
  }

  function renderRounding(el) {
    const cases = [
      { label: "2.5", value: 2.5, round: 3, bround: 2 },
      { label: "3.5", value: 3.5, round: 4, bround: 4 },
      { label: "-2.5", value: -2.5, round: -3, bround: -2 },
      { label: "-3.5", value: -3.5, round: -4, bround: -4 },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.6rem");
    cases.forEach((item, i) => {
      tabs.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .style("cursor", "pointer")
        .on("click", () => { idx = i; update(); })
        .text(item.label);
    });

    const statement = container.append("div")
      .style("font-family", "monospace")
      .style("font-size", "0.8rem")
      .style("margin-bottom", "0.6rem")
      .style("color", FG);
    const cards = container.append("div")
      .style("display", "flex")
      .style("gap", "12px")
      .style("flex-wrap", "wrap");
    const summary = container.append("div")
      .style("font-size", "0.74rem")
      .style("color", FG)
      .style("margin-top", "0.55rem");

    function update() {
      const item = cases[idx];
      tabs.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
      statement.text(`SELECT round(${item.value}, 0), bround(${item.value}, 0)`);
      cards.selectAll("*").remove();
      const roundBox = box(cards, "ROUND", { border: TEAL, bg: "rgba(38,166,154,0.05)" });
      roundBox.append("div")
        .style("font-size", "1rem")
        .style("font-family", "monospace")
        .style("font-weight", "700")
        .style("color", TEAL)
        .text(String(item.round));
      roundBox.append("div")
        .style("font-size", "0.72rem")
        .style("color", FG)
        .text("half-up");
      const broundBox = box(cards, "BROUND", { border: AMBER, bg: "rgba(255,167,38,0.08)" });
      broundBox.append("div")
        .style("font-size", "1rem")
        .style("font-family", "monospace")
        .style("font-weight", "700")
        .style("color", AMBER)
        .text(String(item.bround));
      broundBox.append("div")
        .style("font-size", "0.72rem")
        .style("color", FG)
        .text("half-even");
      summary.text(item.round === item.bround
        ? "No difference here because the tie already rounds to an even whole number."
        : "Tie case: ROUND moves away from zero, while BROUND chooses the nearest even whole number.");
    }
    update();
  }

  function renderNullSafeEq(el) {
    const rows = [
      { left: "NULL", right: "NULL", eq: "NULL", ns: "TRUE" },
      { left: "5", right: "NULL", eq: "NULL", ns: "FALSE" },
      { left: "5", right: "5", eq: "TRUE", ns: "TRUE" },
      { left: "5", right: "7", eq: "FALSE", ns: "FALSE" },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();
    container.append("div")
      .style("font-size", "0.72rem")
      .style("color", GRAY)
      .style("margin-bottom", "0.55rem")
      .text("Click a pair to compare standard equality with null-safe equality.");

    const pairRow = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.6rem");
    rows.forEach((row, i) => {
      chip(pairRow, `${row.left} vs ${row.right}`, { clickable: true })
        .attr("data-i", i)
        .on("click", () => { idx = i; update(); });
    });

    const sqlBox = container.append("div")
      .style("font-family", "monospace")
      .style("font-size", "0.8rem")
      .style("margin-bottom", "0.6rem")
      .style("color", FG);
    const resultRow = container.append("div")
      .style("display", "flex")
      .style("gap", "12px")
      .style("flex-wrap", "wrap");

    function update() {
      const row = rows[idx];
      pairRow.selectAll("div").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? PURPLE : "#f5f5f7")
          .style("color", active ? "#fff" : FG)
          .style("border", `1.5px solid ${active ? PURPLE : "#ddd"}`);
      });
      sqlBox.text(`SELECT ${row.left} = ${row.right}, ${row.left} <=> ${row.right}`);
      resultRow.selectAll("*").remove();
      const eqBox = box(resultRow, "=", { border: row.eq === "NULL" ? GRAY : RED, bg: "#fff" });
      eqBox.append("div")
        .style("font-size", "1rem")
        .style("font-family", "monospace")
        .style("font-weight", "700")
        .style("color", row.eq === "TRUE" ? HIT_COLOR : row.eq === "FALSE" ? RED : GRAY)
        .text(row.eq);
      const nsBox = box(resultRow, "<=>", { border: row.ns === "TRUE" ? HIT_COLOR : RED, bg: "#fff" });
      nsBox.append("div")
        .style("font-size", "1rem")
        .style("font-family", "monospace")
        .style("font-weight", "700")
        .style("color", row.ns === "TRUE" ? HIT_COLOR : RED)
        .text(row.ns);
    }
    update();
  }

  function renderCaseFallthrough(el) {
    const salaries = [75000, 25000, 0, null];
    let selected = 2;
    let withElse = false;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const controls = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.6rem");
    const elseBtn = controls.append("button")
      .style("padding", "5px 12px")
      .style("border-radius", "5px")
      .style("font-size", "0.74rem")
      .style("cursor", "pointer")
      .on("click", () => { withElse = !withElse; update(); });
    salaries.forEach((salary, i) => {
      chip(controls, salary === null ? "salary = NULL" : `salary = ${salary}`, { clickable: true })
        .attr("data-i", i)
        .on("click", () => { selected = i; update(); });
    });

    const flow = container.append("div")
      .style("display", "grid")
      .style("gap", "8px")
      .style("margin-bottom", "0.6rem");
    const result = container.append("div")
      .style("font-family", "monospace")
      .style("font-size", "0.82rem")
      .style("font-weight", "700");

    function classify(salary) {
      if (salary === null) return { branch: "salary IS NULL", value: "Unknown" };
      if (salary >= 50000) return { branch: "salary >= 50000", value: "High" };
      if (salary >= 20000) return { branch: "salary >= 20000", value: "Medium" };
      if (withElse) return { branch: "ELSE", value: "Low" };
      return { branch: "no match", value: "NULL" };
    }

    function update() {
      elseBtn
        .style("background", withElse ? PURPLE : "#fff")
        .style("color", withElse ? "#fff" : FG)
        .style("border", `1px solid ${withElse ? PURPLE : "#ccc"}`)
        .text(withElse ? "ELSE 'Low' enabled" : "ELSE omitted");
      controls.selectAll("div").each(function (_, i) {
        const active = i === selected;
        d3.select(this)
          .style("background", active ? TEAL : "#f5f5f7")
          .style("color", active ? "#fff" : FG)
          .style("border", `1.5px solid ${active ? TEAL : "#ddd"}`);
      });
      flow.selectAll("*").remove();
      const current = salaries[selected];
      const checks = [
        { label: "salary IS NULL", ok: current === null },
        { label: "salary >= 50000", ok: current !== null && current >= 50000 },
        { label: "salary >= 20000", ok: current !== null && current >= 20000 },
        { label: withElse ? "ELSE 'Low'" : "ELSE missing", ok: withElse && current !== null && current < 20000 },
      ];
      checks.forEach((check) => {
        chip(flow, check.label, {
          bg: check.ok ? "rgba(38,166,154,0.12)" : "#f5f5f7",
          fg: check.ok ? HIT_COLOR : FG,
          border: check.ok ? HIT_COLOR : "#ddd",
          opacity: check.ok || check.label.includes("ELSE") ? 1 : 0.7,
        });
      });
      const outcome = classify(current);
      result
        .style("color", outcome.value === "NULL" ? RED : PURPLE)
        .text(`Result: ${outcome.value} (${outcome.branch})`);
    }
    update();
  }

  function renderCastVsTryCast(el) {
    const cases = [
      {
        label: "'123' AS INT",
        sql: "SELECT CAST('123' AS INT), TRY_CAST('123' AS INT)",
        cast: "123",
        tryCast: "123",
        note: "Clean input succeeds in both forms.",
      },
      {
        label: "'abc' AS INT",
        sql: "SELECT CAST('abc' AS INT), TRY_CAST('abc' AS INT)",
        cast: "ERROR",
        tryCast: "NULL",
        note: "Malformed numeric text throws with CAST but safely returns NULL with TRY_CAST.",
      },
      {
        label: "'42.9' AS INT",
        sql: "SELECT CAST('42.9' AS INT), TRY_CAST('42.9' AS INT)",
        cast: "ERROR",
        tryCast: "NULL",
        note: "Decimal-looking text is not silently truncated to INT in ANSI mode.",
      },
      {
        label: "'128' AS TINYINT",
        sql: "SELECT CAST('128' AS TINYINT), TRY_CAST('128' AS TINYINT)",
        cast: "ERROR",
        tryCast: "NULL",
        note: "Overflow is also tolerated by TRY_CAST via a NULL result.",
      },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.6rem");
    cases.forEach((item, i) => {
      tabs.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .style("cursor", "pointer")
        .on("click", () => { idx = i; update(); })
        .text(item.label);
    });

    const sqlLine = container.append("div")
      .style("font-family", "monospace")
      .style("font-size", "0.8rem")
      .style("margin-bottom", "0.6rem")
      .style("color", FG);
    const resultRow = container.append("div")
      .style("display", "flex")
      .style("gap", "12px")
      .style("flex-wrap", "wrap");
    const note = container.append("div")
      .style("font-size", "0.74rem")
      .style("margin-top", "0.55rem")
      .style("color", FG);

    function update() {
      const item = cases[idx];
      tabs.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
      sqlLine.text(item.sql);
      resultRow.selectAll("*").remove();
      const castBox = box(resultRow, "CAST", {
        border: item.cast === "ERROR" ? RED : TEAL,
        bg: item.cast === "ERROR" ? "rgba(239,83,80,0.05)" : "rgba(38,166,154,0.05)",
      });
      castBox.append("div")
        .style("font-size", "1rem")
        .style("font-family", "monospace")
        .style("font-weight", "700")
        .style("color", item.cast === "ERROR" ? RED : TEAL)
        .text(item.cast);
      const tryBox = box(resultRow, "TRY_CAST", {
        border: item.tryCast === "NULL" ? AMBER : HIT_COLOR,
        bg: item.tryCast === "NULL" ? "rgba(255,167,38,0.08)" : "rgba(38,166,154,0.05)",
      });
      tryBox.append("div")
        .style("font-size", "1rem")
        .style("font-family", "monospace")
        .style("font-weight", "700")
        .style("color", item.tryCast === "NULL" ? AMBER : HIT_COLOR)
        .text(item.tryCast);
      note.text(item.note);
    }
    update();
  }

  function init() {
    const specs = [
      ["viz-string-split-regex", renderStringOps],
      ["viz-math-rounding", renderRounding],
      ["viz-null-safe-eq", renderNullSafeEq],
      ["viz-control-case-flow", renderCaseFallthrough],
      ["viz-conversion-cast-trycast", renderCastVsTryCast],
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
