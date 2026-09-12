/**
 * scalar-index-viz.js
 * D3 v7 interactive visualization for the Scalar Functions overview page
 * (docs/functions/scalar/index.md).
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-category-explorer — click a scalar-function category to see its
 *                             key functions and a one-line usage hint.
 */
(function () {
  "use strict";

  const PURPLE = "#7c4dff";
  const FG = "#546e7a";
  const GRAY = "#90a4ae";

  const CATEGORIES = [
    { name: "String", fns: "UPPER · TRIM · SPLIT · SUBSTRING · CONCAT_WS", hint: "Normalise, parse, or format text values." },
    { name: "Math", fns: "ABS · ROUND · MOD · POWER · GREATEST", hint: "Arithmetic, rounding, and numeric transforms." },
    { name: "DateTime", fns: "DATE_ADD · DATE_TRUNC · TO_TIMESTAMP", hint: "Date/time arithmetic, parsing, and formatting." },
    { name: "NULL", fns: "COALESCE · NVL · NULLIF · <=>", hint: "Safe NULL handling and NULL-safe comparisons." },
    { name: "Conversion", fns: "CAST · TRY_CAST · TO_DATE", hint: "Convert between types; TRY_CAST is NULL-safe." },
    { name: "Regex", fns: "REGEXP_EXTRACT · REGEXP_REPLACE · RLIKE", hint: "Pattern matching and text extraction." },
    { name: "Encryption", fns: "MD5 · SHA2 · CRC32 · MASK", hint: "Hashing for dedup/integrity or masking for PII." },
    { name: "Predicate", fns: "ISNULL · ISNAN · IN · BETWEEN", hint: "Boolean checks — mind three-valued logic with IN/NULL." },
    { name: "Bitwise", fns: "BIT_AND · SHIFTLEFT · XOR", hint: "Low-level integer bit manipulation." },
    { name: "Web", fns: "PARSE_URL · URL_ENCODE", hint: "Parse or encode URL components." },
    { name: "ACL", fns: "CURRENT_USER · IS_MEMBER", hint: "Session/identity introspection." },
  ];

  function renderCategoryExplorer(el) {
    let selected = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabRow = container.append("div")
      .style("display", "flex").style("flex-wrap", "wrap").style("gap", "6px").style("margin-bottom", "0.7rem");

    CATEGORIES.forEach((c, i) => {
      tabRow.append("div")
        .attr("data-i", i)
        .style("cursor", "pointer").style("padding", "5px 11px").style("border-radius", "5px")
        .style("font-size", "0.74rem").style("font-weight", "600")
        .text(c.name)
        .on("click", () => { selected = i; update(); });
    });

    const detail = container.append("div").style("padding", "10px 12px").style("border-radius", "6px").style("background", "#f5f5f7");
    const fnsLine = detail.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("color", PURPLE).style("margin-bottom", "6px");
    const hintLine = detail.append("div").style("font-size", "0.78rem").style("color", FG);

    function update() {
      tabRow.selectAll("div").each(function (_, i) {
        const active = i === selected;
        d3.select(this).style("background", active ? PURPLE : "#fff").style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
      const c = CATEGORIES[selected];
      fnsLine.text(c.fns);
      hintLine.html(`<b>When to use:</b> ${c.hint}`);
    }
    update();
  }

  function init() {
    const elm = document.getElementById("viz-category-explorer");
    if (elm && !elm.dataset.rendered) { renderCategoryExplorer(elm); elm.dataset.rendered = "1"; }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
