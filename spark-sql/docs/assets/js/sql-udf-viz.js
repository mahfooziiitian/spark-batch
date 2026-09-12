/**
 * sql-udf-viz.js
 * D3 v7 interactive visualizations for the SQL User-Defined Functions page
 * (docs/functions/sql-udf/index.md).
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-sql-udf-expansion — toggle SQL UDF vs Python UDF to contrast inline
 *                            expansion (visible to Catalyst) against an
 *                            opaque cross-process call.
 *   #viz-tvf-rows          — a table-valued function call fanning out into
 *                            multiple output rows; adjust the date range live.
 */
(function () {
  "use strict";

  const PURPLE = "#7c4dff";
  const TEAL = "#26a69a";
  const RED = "#ef5350";
  const GRAY = "#90a4ae";
  const FG = "#546e7a";
  const HIT_COLOR = "#26a69a";

  function box(parent, html, opts) {
    opts = opts || {};
    return parent.append("div")
      .style("padding", "8px 12px").style("border-radius", "6px")
      .style("font-family", "monospace").style("font-size", "0.76rem")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .html(html);
  }

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

  /* ══════════════════════════════════════════════════════════════════
   * 1. SQL UDF vs Python UDF expansion
   * ══════════════════════════════════════════════════════════════════ */
  function renderSqlUdfExpansion(el) {
    let mode = "sql";
    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem");
    ["sql", "python"].forEach((m) => {
      ctrl.append("button")
        .attr("data-m", m)
        .style("padding", "5px 14px").style("border-radius", "5px")
        .style("cursor", "pointer").style("font-size", "0.76rem")
        .text(m === "sql" ? "SQL UDF" : "Python UDF")
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
      box(row1, mode === "sql"
        ? "SELECT area(width, height) FROM shapes"
        : "SELECT area_py(width, height) FROM shapes");

      stage.append("div").style("text-align", "center").style("color", GRAY).style("font-size", "0.9rem").text("↓ analysis / execution ↓");

      const row2 = stage.append("div");
      row2.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").style("margin-bottom", "4px")
        .text(mode === "sql" ? "What Catalyst executes (SQL UDF body inlined)" : "What actually happens (Python UDF stays opaque)");
      if (mode === "sql") {
        box(row2, "SELECT width * height FROM shapes", { bg: "rgba(38,166,154,0.1)", fg: HIT_COLOR, border: HIT_COLOR });
      } else {
        box(row2, `SELECT <span style="color:${RED}">area_py(width, height)</span> FROM shapes  <span style="color:${GRAY}">// row serialized to Python, result serialized back</span>`, { bg: "rgba(239,83,80,0.08)", fg: FG, border: RED });
      }

      const note = stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "4px");
      note.html(mode === "sql"
        ? "✓ No process boundary crossed — the expression runs entirely inside the JVM, fully optimized by Catalyst."
        : "✕ Each row's arguments cross the JVM ↔ Python boundary and back — much slower at scale than an inlined SQL body.");
    }
    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. TVF row fan-out
   * ══════════════════════════════════════════════════════════════════ */
  function renderTvfRows(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    container.append("div")
      .style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "0.5rem")
      .text("SELECT * FROM date_range(start, end) — one call, one row per date.");

    const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "0.6rem").style("margin-bottom", "0.6rem").style("font-size", "0.76rem");
    ctrl.append("span").style("color", FG).style("font-weight", "600").text("date_range('2024-01-01', end = 2024-01-");
    const label = ctrl.append("code").style("padding", "2px 8px").style("border-radius", "4px").style("background", "#eee").text("05");
    ctrl.append("span").style("color", FG).style("font-weight", "600").text(")");
    let endDay = 5;
    ctrl.append("input")
      .attr("type", "range").attr("min", 1).attr("max", 10).attr("value", endDay)
      .style("width", "140px")
      .on("input", function () { endDay = +this.value; update(); });

    const callRow = container.append("div").style("margin-bottom", "0.5rem");
    const rowsWrap = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px");

    function update() {
      label.text(String(endDay).padStart(2, "0"));
      callRow.selectAll("*").remove();
      chip(callRow, `date_range(DATE'2024-01-01', DATE'2024-01-${String(endDay).padStart(2, "0")}')`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      callRow.append("span").style("color", GRAY).style("font-size", "0.8rem").style("margin-left", "8px").text("↓ one call");

      rowsWrap.selectAll("*").remove();
      for (let d = 1; d <= endDay; d++) {
        chip(rowsWrap, `2024-01-${String(d).padStart(2, "0")}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      }
      rowsWrap.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "4px")
        .text(`→ ${endDay} output row${endDay === 1 ? "" : "s"} from a single FROM-clause call`);
    }
    update();
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-sql-udf-expansion", renderSqlUdfExpansion],
      ["viz-tvf-rows", renderTvfRows],
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
