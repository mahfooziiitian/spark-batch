/**
 * structure-viz.js
 * D3 v7 interactive visualizations for the Structure Functions pages
 * (docs/functions/structure/index.md, json.md, csv.md, xml.md).
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-structure-direction — pick a format (JSON/CSV/XML) to see which
 *                               functions parse in vs serialize out.
 *   #viz-json-parse-tree      — well-formed vs malformed JSON, PERMISSIVE vs
 *                               FAILFAST outcome.
 *   #viz-csv-position-map     — positional (not name-based) field mapping.
 *   #viz-xpath-tree           — pick an XPath expression, see matched nodes.
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

  /* ══════════════════════════════════════════════════════════════════
   * 1. Parse vs Serialize direction, by format
   * ══════════════════════════════════════════════════════════════════ */
  function renderStructureDirection(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");

    const data = {
      json: { parse: ["FROM_JSON"], serialize: ["TO_JSON"], inspect: ["SCHEMA_OF_JSON", "JSON_OBJECT_KEYS", "JSON_ARRAY_LENGTH"], extract: ["GET_JSON_OBJECT", "JSON_TUPLE"] },
      csv: { parse: ["FROM_CSV"], serialize: ["TO_CSV"], inspect: ["SCHEMA_OF_CSV"], extract: [] },
      xml: { parse: ["FROM_XML"], serialize: ["TO_XML"], inspect: ["SCHEMA_OF_XML"], extract: ["XPATH", "XPATH_STRING", "XPATH_INT", "…"] },
    };

    const group = btnGroup(container, [["json", "JSON"], ["csv", "CSV"], ["xml", "XML"]], render);

    function render(fmt) {
      stage.selectAll("*").remove();
      const d = data[fmt];
      const cols = [
        ["Parse (string → struct)", d.parse, HIT_COLOR],
        ["Serialize (struct → string)", d.serialize, PURPLE],
        ["Extract single value(s)", d.extract, AMBER],
        ["Inspect / infer schema", d.inspect, GRAY],
      ];
      const row = stage.append("div").style("display", "flex").style("gap", "1rem").style("flex-wrap", "wrap");
      cols.forEach(([title, fns, color]) => {
        const col = row.append("div").style("flex", "1").style("min-width", "160px");
        col.append("div").style("font-size", "0.68rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px").text(title);
        if (fns.length === 0) {
          col.append("div").style("font-size", "0.7rem").style("color", GRAY).text("— not available —");
        } else {
          fns.forEach((f) => chip(col.append("div").style("margin-bottom", "3px"), f, { bg: `${color}1a`, fg: color, border: color }));
        }
      });
    }
    render(group.get());
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. JSON parse tree — PERMISSIVE vs FAILFAST
   * ══════════════════════════════════════════════════════════════════ */
  function renderJsonParseTree(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");

    const scenarios = {
      wellformed: { input: '{"a":1, "b":2}', schema: "a INT, b INT", permissive: "{a: 1, b: 2}", failfast: "{a: 1, b: 2}", ok: true },
      malformed: { input: '{"a":1, bad}', schema: "a INT", permissive: "{a: NULL}", failfast: "ERROR: MALFORMED_RECORD_IN_PARSING", ok: false },
    };

    const group = btnGroup(container, [["wellformed", "Well-formed JSON"], ["malformed", "Malformed JSON"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const s = scenarios[key];
      stage.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("margin-bottom", "8px")
        .html(`FROM_JSON(<span style="color:${PURPLE}">'${s.input}'</span>, '${s.schema}', ...)`);

      const row = stage.append("div").style("display", "flex").style("gap", "1rem").style("flex-wrap", "wrap");
      const p = row.append("div").style("flex", "1").style("min-width", "180px");
      p.append("div").style("font-size", "0.68rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px").text("mode = PERMISSIVE (default)");
      chip(p, s.permissive, { bg: s.ok ? "rgba(38,166,154,0.12)" : "rgba(255,167,38,0.12)", fg: s.ok ? HIT_COLOR : AMBER, border: s.ok ? HIT_COLOR : AMBER });
      p.append("div").style("font-size", "0.68rem").style("color", GRAY).style("margin-top", "4px").text(s.ok ? "parses cleanly" : "no error thrown — struct nulled");

      const f = row.append("div").style("flex", "1").style("min-width", "180px");
      f.append("div").style("font-size", "0.68rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px").text("mode = FAILFAST");
      chip(f, s.failfast, { bg: s.ok ? "rgba(38,166,154,0.12)" : "rgba(239,83,80,0.1)", fg: s.ok ? HIT_COLOR : RED, border: s.ok ? HIT_COLOR : RED });
      f.append("div").style("font-size", "0.68rem").style("color", GRAY).style("margin-top", "4px").text(s.ok ? "parses cleanly" : "job aborts with an exception");
    }
    render(group.get());
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. CSV positional field mapping
   * ══════════════════════════════════════════════════════════════════ */
  function renderCsvPositionMap(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    container.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "0.5rem")
      .text("FROM_CSV('Alice,30,NYC', 'name STRING, age INT, city STRING')");

    const values = ["Alice", "30", "NYC"];
    const fields = ["name STRING", "age INT", "city STRING"];

    const row = container.append("div").style("display", "flex").style("gap", "1.2rem");
    const valCol = row.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px");
    const schemaCol = row.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px");

    values.forEach((v, i) => {
      const r = valCol.append("div").style("display", "flex").style("align-items", "center").style("gap", "6px");
      r.append("span").style("font-size", "0.68rem").style("color", GRAY).style("width", "18px").text(`#${i}`);
      chip(r, v, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
    });
    fields.forEach((f, i) => {
      const r = schemaCol.append("div").style("display", "flex").style("align-items", "center").style("gap", "6px");
      chip(r, f, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      r.append("span").style("font-size", "0.68rem").style("color", GRAY).text(`← position #${i}`);
    });

    container.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "8px")
      .text("Matching is purely positional — field names in the schema are labels only, not lookups against a CSV header.");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. XPath node selection
   * ══════════════════════════════════════════════════════════════════ */
  function renderXpathTree(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const xml = { tag: "a", children: [
      { tag: "b", text: "b1" }, { tag: "b", text: "b2" }, { tag: "b", text: "b3" },
      { tag: "c", text: "c1" }, { tag: "c", text: "c2" },
    ] };

    const exprs = {
      "a/b/text()": (n) => n.tag === "b",
      "a/c/text()": (n) => n.tag === "c",
      "a/*/text()": () => true,
    };

    const stage = container.append("div");
    const group = btnGroup(container, Object.keys(exprs).map((k) => [k, k]), render);

    function render(expr) {
      stage.selectAll("*").remove();
      const match = exprs[expr];
      const tree = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px");

      const rootRow = tree.append("div");
      chip(rootRow, `<${xml.tag}>`, { bg: "#f5f5f7", border: "#ccc" });

      const kids = tree.append("div").style("display", "flex").style("gap", "6px").style("margin-left", "1.2rem").style("flex-wrap", "wrap");
      xml.children.forEach((c) => {
        const hit = match(c);
        chip(kids, `<${c.tag}>${c.text}</${c.tag}>`, hit
          ? { bg: "rgba(38,166,154,0.15)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "#f5f5f7", fg: GRAY, border: "#ddd" });
      });

      const matched = xml.children.filter(match).map((c) => c.text);
      stage.append("div").style("font-family", "monospace").style("font-size", "0.74rem").style("margin-top", "8px")
        .html(`XPATH(xml, '${expr}') → <span style="color:${HIT_COLOR}">[${matched.map((m) => `'${m}'`).join(", ")}]</span>`);
      stage.append("div").style("font-family", "monospace").style("font-size", "0.74rem").style("color", GRAY)
        .html(`XPATH_STRING(xml, '${expr}') → <span style="color:${PURPLE}">'${matched[0] || ""}'</span>  (first match only)`);
    }
    render(group.get());
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-structure-direction", renderStructureDirection],
      ["viz-json-parse-tree", renderJsonParseTree],
      ["viz-csv-position-map", renderCsvPositionMap],
      ["viz-xpath-tree", renderXpathTree],
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
