/**
 * macro-viz.js
 * D3 v7 interactive visualizations for the Macros & Dynamic SQL pages
 * (macro.md, intro.md).
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-macro-expansion — toggle Macro vs UDF to compare inline expansion
 *                          (visible to Catalyst) vs an opaque runtime call.
 *   #viz-identifier      — compare naive string concatenation (injectable)
 *                          against identifier() (safely quoted, name-only).
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

  function box(parent, text, opts) {
    opts = opts || {};
    return parent.append("div")
      .style("padding", "8px 12px").style("border-radius", "6px")
      .style("font-family", "monospace").style("font-size", "0.76rem")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .html(text);
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. MACRO EXPANSION vs UDF CALL
   * ══════════════════════════════════════════════════════════════════ */
  function renderMacroExpansion(el) {
    let mode = "macro";
    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem");
    ["macro", "udf"].forEach((m) => {
      ctrl.append("button")
        .attr("data-m", m)
        .style("padding", "5px 14px").style("border-radius", "5px")
        .style("cursor", "pointer").style("font-size", "0.76rem")
        .text(m === "macro" ? "SQL Macro" : "UDF")
        .on("click", () => { mode = m; update(); });
    });

    const stage = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "10px");

    function update() {
      ctrl.selectAll("button").each(function () {
        const active = d3.select(this).attr("data-m") === mode;
        d3.select(this).style("background", active ? PURPLE : "#fff").style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
      stage.selectAll("*").remove();

      const row1 = stage.append("div");
      row1.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").style("margin-bottom", "4px").text("Query as written");
      box(row1, mode === "macro"
        ? "SELECT double_it(amount) FROM orders WHERE amount &gt; 10"
        : "SELECT double_it_udf(amount) FROM orders WHERE amount &gt; 10");

      stage.append("div").style("text-align", "center").style("color", GRAY).style("font-size", "0.9rem").text("↓ analysis / optimization ↓");

      const row2 = stage.append("div");
      row2.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").style("margin-bottom", "4px")
        .text(mode === "macro" ? "What Catalyst actually sees (macro is textually substituted)" : "What Catalyst sees (UDF call stays opaque)");
      if (mode === "macro") {
        box(row2, "SELECT amount * 2 FROM orders WHERE amount &gt; 10", { bg: "rgba(38,166,154,0.1)", fg: HIT_COLOR, border: HIT_COLOR });
      } else {
        box(row2, "SELECT <span style=\"color:" + RED + "\">double_it_udf(amount)</span> FROM orders WHERE amount &gt; 10  <span style=\"color:" + GRAY + "\">// UDF stays as a black-box node</span>", { bg: "rgba(239,83,80,0.08)", fg: FG, border: RED });
      }

      const note = stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "4px");
      note.html(mode === "macro"
        ? "✓ Constant folding, predicate pushdown, and cost-based optimization all see the expanded expression <code>amount * 2</code> directly."
        : "✕ The optimizer cannot rewrite or push predicates through the UDF call — it is invoked once per row, unmodified, at execution time.");
    }
    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. identifier() vs naive concatenation
   * ══════════════════════════════════════════════════════════════════ */
  function renderIdentifier(el) {
    const inputs = [
      { label: "sales_data", malicious: false },
      { label: "order", malicious: false }, // reserved keyword
      { label: "x; DROP TABLE users; --", malicious: true },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem").style("flex-wrap", "wrap");
    inputs.forEach((inp, i) => {
      ctrl.append("button")
        .attr("data-i", i)
        .style("padding", "5px 10px").style("border-radius", "5px")
        .style("cursor", "pointer").style("font-size", "0.72rem").style("font-family", "monospace")
        .text(`"${inp.label}"`)
        .on("click", () => { idx = i; update(); });
    });

    const stage = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "10px");

    function update() {
      ctrl.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this).style("background", active ? PURPLE : "#fff").style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
      stage.selectAll("*").remove();
      const inp = inputs[idx];

      const naiveRow = stage.append("div");
      naiveRow.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").style("margin-bottom", "4px").text("Naive f-string concatenation");
      const naiveSql = `SELECT COUNT(*) FROM ${inp.label}`;
      box(naiveRow, naiveSql.replace("&", "&amp;").replace("<", "&lt;"), inp.malicious || inp.label === "order"
        ? { bg: "rgba(239,83,80,0.08)", fg: RED, border: RED }
        : { bg: "#f5f5f7", fg: FG, border: "#ddd" });
      naiveRow.append("div").style("font-size", "0.7rem").style("margin-top", "3px")
        .style("color", inp.malicious ? RED : inp.label === "order" ? AMBER : HIT_COLOR)
        .text(inp.malicious ? "✕ injected DROP TABLE statement executes!" : inp.label === "order" ? "✕ syntax error — 'order' is a reserved keyword" : "✓ happens to work for simple names");

      const safeRow = stage.append("div");
      safeRow.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").style("margin-bottom", "4px").text("identifier() — parsed as a single name, always quoted");
      box(safeRow, `SELECT COUNT(*) FROM identifier('${inp.label}')`, { bg: "rgba(38,166,154,0.1)", fg: HIT_COLOR, border: HIT_COLOR });
      safeRow.append("div").style("font-size", "0.7rem").style("margin-top", "3px").style("color", HIT_COLOR)
        .text("✓ the entire string is resolved as one name — no SQL syntax is parsed from it, even if it contains ';' or keywords");
    }
    update();
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-macro-expansion", renderMacroExpansion],
      ["viz-identifier", renderIdentifier],
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
