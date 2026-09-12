/**
 * udf-viz.js
 * D3 v7 interactive visualizations for the User-Defined Functions pages
 * (docs/functions/udf/index.md and docs/functions/udf/udf.md).
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-udf-row-path — toggle Row UDF (legacy BatchEvalPython) vs Arrow UDF
 *                        (Spark 4.0+ default) to see the serialization unit
 *                        change from "one row" to "one column batch".
 *   #viz-udtf-fanout   — scalar UDF (always 1 row out) vs UDTF (0..N rows out)
 *                        fan-out comparison.
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

  /* ══════════════════════════════════════════════════════════════════
   * 1. Row UDF vs Arrow UDF transfer path
   * ══════════════════════════════════════════════════════════════════ */
  function renderUdfRowPath(el) {
    let mode = "arrow"; // Spark 4.0+ default
    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem");
    [["row", "Row UDF (legacy)"], ["arrow", "Arrow UDF (Spark 4.0+ default)"]].forEach(([m, label]) => {
      ctrl.append("button")
        .attr("data-m", m)
        .style("padding", "5px 14px").style("border-radius", "5px")
        .style("cursor", "pointer").style("font-size", "0.74rem")
        .text(label)
        .on("click", () => { mode = m; update(); });
    });

    const rows = 6;
    const stage = container.append("div");

    function update() {
      ctrl.selectAll("button").each(function () {
        const active = d3.select(this).attr("data-m") === mode;
        d3.select(this).style("background", active ? PURPLE : "#fff").style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
      stage.selectAll("*").remove();

      stage.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").style("margin-bottom", "6px")
        .text("JVM  ⇄  Python worker");

      const lane = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px");

      if (mode === "row") {
        for (let i = 0; i < rows; i++) {
          const r = lane.append("div").style("display", "flex").style("align-items", "center").style("gap", "6px");
          chip(r, `row[${i}]`, { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
          r.append("span").style("color", GRAY).style("font-size", "0.85rem").text("→ pickle →  eval()  → pickle →");
          chip(r, `result[${i}]`, { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
        }
        stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px")
          .text(`✕ ${rows} separate round trips — one Python (de)serialization call per row.`);
      } else {
        const batch = lane.append("div").style("display", "flex").style("align-items", "center").style("gap", "6px");
        const cells = batch.append("div").style("display", "flex").style("gap", "2px");
        for (let i = 0; i < rows; i++) {
          cells.append("div").style("width", "22px").style("height", "22px").style("border-radius", "3px")
            .style("background", "rgba(38,166,154,0.15)").style("border", `1px solid ${HIT_COLOR}`)
            .style("display", "flex").style("align-items", "center").style("justify-content", "center")
            .style("font-size", "0.62rem").style("color", HIT_COLOR).text(i);
        }
        batch.append("span").style("color", GRAY).style("font-size", "0.85rem").text("→ one Arrow batch →  eval()  → one Arrow batch →");
        const outCells = batch.append("div").style("display", "flex").style("gap", "2px");
        for (let i = 0; i < rows; i++) {
          outCells.append("div").style("width", "22px").style("height", "22px").style("border-radius", "3px")
            .style("background", "rgba(38,166,154,0.15)").style("border", `1px solid ${HIT_COLOR}`)
            .style("display", "flex").style("align-items", "center").style("justify-content", "center")
            .style("font-size", "0.62rem").style("color", HIT_COLOR).text(i);
        }
        stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px")
          .text(`✓ 1 round trip for all ${rows} rows — columnar batch transfer, same mechanism as @pandas_udf.`);
      }
    }
    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. Scalar UDF vs UDTF fan-out
   * ══════════════════════════════════════════════════════════════════ */
  function renderUdtfFanout(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const wrap = container.append("div").style("display", "flex").style("gap", "1.5rem").style("flex-wrap", "wrap");

    // Scalar UDF column
    const scalarCol = wrap.append("div").style("flex", "1").style("min-width", "220px");
    scalarCol.append("div").style("font-size", "0.72rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "6px")
      .text("Scalar UDF — square(x)");
    [2, 3, 4].forEach((x) => {
      const r = scalarCol.append("div").style("display", "flex").style("align-items", "center").style("gap", "6px").style("margin-bottom", "4px");
      chip(r, `in: ${x}`, { bg: "#f5f5f7", border: "#ccc" });
      r.append("span").style("color", GRAY).text("→");
      chip(r, `out: ${x * x}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
    });
    scalarCol.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "4px")
      .text("Always exactly 1 output row per input row.");

    // UDTF column
    const udtfCol = wrap.append("div").style("flex", "1").style("min-width", "220px");
    udtfCol.append("div").style("font-size", "0.72rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "6px")
      .text("UDTF — split_words(s)");
    const inputs = [["a b", 2], ["c d e", 3], ["", 0]];
    inputs.forEach(([s, n]) => {
      const r = udtfCol.append("div").style("display", "flex").style("align-items", "flex-start").style("gap", "6px").style("margin-bottom", "6px");
      chip(r, `in: '${s}'`, { bg: "#f5f5f7", border: "#ccc" });
      r.append("span").style("color", GRAY).text("→");
      const outs = r.append("div").style("display", "flex").style("flex-wrap", "wrap").style("gap", "4px");
      if (n === 0) {
        chip(outs, "(0 rows)", { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER });
      } else {
        s.split(" ").forEach((w) => chip(outs, w, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      }
    });
    udtfCol.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "4px")
      .text("0, 1, or many output rows per input row — driven by how many times eval() yields.");
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-udf-row-path", renderUdfRowPath],
      ["viz-udtf-fanout", renderUdtfFanout],
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
