/**
 * lambda-viz.js
 * D3 v7 interactive visualizations for the Lambda Expressions pages
 * (index, syntax, array-hof, map-hof, aggregate-hof, patterns).
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-lambda-anatomy — click "params" / "body" to see each lambda part's role.
 *   #viz-param-binding  — click a lambda signature to see positional argument binding.
 *   #viz-zip-with-pad   — ZIP_WITH pairing with NULL padding for the shorter array.
 *   #viz-map-zip-with   — MAP_ZIP_WITH key-union merge with NULL for the missing side.
 *   #viz-aggregate-fold — step-through fold/reduce showing the running accumulator.
 *   #viz-pipeline       — two-stage FILTER -> TRANSFORM pipeline, elements dropped
 *                         in stage 1 never reach stage 2.
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
      .style("cursor", opts.clickable ? "pointer" : "default")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .style("opacity", opts.opacity != null ? opts.opacity : 1)
      .text(text);
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. LAMBDA ANATOMY — params vs body
   * ══════════════════════════════════════════════════════════════════ */
  function renderLambdaAnatomy(el) {
    let selected = null;
    const container = d3.select(el);
    container.selectAll("*").remove();

    const exprRow = container.append("div")
      .style("display", "flex").style("gap", "4px").style("align-items", "center")
      .style("font-family", "monospace").style("font-size", "1rem").style("margin-bottom", "0.7rem");

    const paramsSpan = exprRow.append("span")
      .style("cursor", "pointer").style("padding", "3px 8px").style("border-radius", "4px")
      .text("(x, i)").on("click", () => { selected = selected === "params" ? null : "params"; update(); });
    exprRow.append("span").style("color", GRAY).text(" -> ");
    const bodySpan = exprRow.append("span")
      .style("cursor", "pointer").style("padding", "3px 8px").style("border-radius", "4px")
      .text("x * (i + 1)").on("click", () => { selected = selected === "body" ? null : "body"; update(); });

    const desc = container.append("div").style("font-size", "0.76rem").style("color", FG).style("min-height", "2.4em");

    function update() {
      paramsSpan.style("background", selected === "params" ? PURPLE : "transparent")
        .style("color", selected === "params" ? "#fff" : PURPLE).style("font-weight", "700");
      bodySpan.style("background", selected === "body" ? TEAL : "transparent")
        .style("color", selected === "body" ? "#fff" : TEAL).style("font-weight", "700");
      if (selected === "params") {
        desc.html("<b>Parameters</b> — one name per value the HOF passes in (element, index, key, value, or accumulator). Names are arbitrary; only position and count matter.");
      } else if (selected === "body") {
        desc.html("<b>Body</b> — any SQL expression using the parameter names. Evaluated once per element/entry; cannot reference UDFs or run as a standalone SELECT.");
      } else {
        desc.html("Click a part above to see its role.");
      }
    }
    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. PARAMETER BINDING — positional argument binding
   * ══════════════════════════════════════════════════════════════════ */
  function renderParamBinding(el) {
    const forms = [
      { sig: "(val, idx)", args: ["'gold'", "0"], names: ["val", "idx"] },
      { sig: "(k, v)", args: ["'city'", "'NYC'"], names: ["k", "v"] },
      { sig: "(acc, x)", args: ["0", "5"], names: ["acc", "x"] },
      { sig: "(a, b)", args: ["1", "100"], names: ["a", "b"] },
    ];
    let idx = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabRow = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "0.6rem");
    forms.forEach((f, i) => {
      tabRow.append("div")
        .attr("data-i", i)
        .style("cursor", "pointer").style("padding", "4px 10px").style("border-radius", "5px")
        .style("font-family", "monospace").style("font-size", "0.72rem")
        .text(f.sig)
        .on("click", () => { idx = i; update(); });
    });

    const bindRow = container.append("div").style("display", "flex").style("gap", "24px").style("margin-top", "0.5rem");

    function update() {
      tabRow.selectAll("div").each(function (_, i) {
        const active = i === idx;
        d3.select(this).style("background", active ? PURPLE : "#f5f5f7").style("color", active ? "#fff" : FG);
      });
      bindRow.selectAll("*").remove();
      const f = forms[idx];
      f.names.forEach((name, i) => {
        const col = bindRow.append("div").style("text-align", "center");
        col.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("color", PURPLE).style("font-weight", "700").text(name);
        col.append("div").style("color", GRAY).style("font-size", "0.9rem").text("↑");
        col.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("color", TEAL).text(f.args[i]);
      });
    }
    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. ZIP_WITH padding
   * ══════════════════════════════════════════════════════════════════ */
  function renderZipWithPad(el) {
    const a = [1, 2, 3];
    const b = [10, 20, 30, 40, 50];
    const len = Math.max(a.length, b.length);

    const container = d3.select(el);
    container.selectAll("*").remove();
    container.append("div")
      .style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "0.5rem")
      .text("ZIP_WITH(a, b, (x, y) -> x + y) — the shorter array is padded with NULL, never truncated.");

    function makeRow(label, arr) {
      const wrap = container.append("div").style("margin-bottom", "0.35rem");
      wrap.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").text(label);
      const r = wrap.append("div").style("display", "flex").style("gap", "8px").style("margin-top", "3px");
      for (let i = 0; i < len; i++) {
        const v = arr[i];
        const isPad = v === undefined;
        chip(r, isPad ? "NULL" : String(v), {
          bg: isPad ? "rgba(239,83,80,0.08)" : "#f5f5f7",
          fg: isPad ? RED : FG,
          border: isPad ? RED : "#ddd",
        });
      }
    }

    makeRow("array a", a);
    makeRow("array b", b);
    container.append("div")
      .style("text-align", "center").style("color", GRAY).style("font-size", "0.9rem").style("margin", "2px 0")
      .text("↓ (x, y) -> x + y ↓");

    const resultRow = container.append("div").style("display", "flex").style("gap", "8px").style("margin-top", "3px");
    for (let i = 0; i < len; i++) {
      const x = a[i], y = b[i];
      const isNull = x === undefined || y === undefined;
      chip(resultRow, isNull ? "NULL" : String(x + y), {
        bg: isNull ? "rgba(239,83,80,0.1)" : "rgba(38,166,154,0.12)",
        fg: isNull ? RED : HIT_COLOR,
        border: isNull ? RED : HIT_COLOR,
      });
    }
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. MAP_ZIP_WITH key union
   * ══════════════════════════════════════════════════════════════════ */
  function renderMapZipWith(el) {
    const m1 = { a: 10, b: 20 };
    const m2 = { a: 5, c: 7 };
    const keys = Array.from(new Set([...Object.keys(m1), ...Object.keys(m2)])).sort();

    const container = d3.select(el);
    container.selectAll("*").remove();
    container.append("div")
      .style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "0.5rem")
      .text("MAP_ZIP_WITH(m1, m2, (k, v1, v2) -> COALESCE(v1,0) + COALESCE(v2,0)) — union of keys.");

    function makeRow(label, map) {
      const wrap = container.append("div").style("margin-bottom", "0.35rem");
      wrap.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").text(label);
      const r = wrap.append("div").style("display", "flex").style("gap", "8px").style("margin-top", "3px");
      keys.forEach((k) => {
        const has = k in map;
        chip(r, `${k}:${has ? map[k] : "NULL"}`, {
          bg: has ? "#f5f5f7" : "rgba(239,83,80,0.08)",
          fg: has ? FG : RED,
          border: has ? "#ddd" : RED,
        });
      });
    }

    makeRow("map1", m1);
    makeRow("map2", m2);
    container.append("div")
      .style("text-align", "center").style("color", GRAY).style("font-size", "0.9rem").style("margin", "2px 0")
      .text("↓ merge by key ↓");

    const resultRow = container.append("div").style("display", "flex").style("gap", "8px").style("margin-top", "3px");
    keys.forEach((k) => {
      const v1 = k in m1 ? m1[k] : 0;
      const v2 = k in m2 ? m2[k] : 0;
      const bothMissingReal = !(k in m1) || !(k in m2);
      chip(resultRow, `${k}:${v1 + v2}`, {
        bg: bothMissingReal ? "rgba(255,167,38,0.12)" : "rgba(38,166,154,0.12)",
        fg: bothMissingReal ? AMBER : HIT_COLOR,
        border: bothMissingReal ? AMBER : HIT_COLOR,
      });
    });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. AGGREGATE fold step-through
   * ══════════════════════════════════════════════════════════════════ */
  function renderAggregateFold(el) {
    const input = [10, 20, 30, 40];
    let step = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();
    container.append("div")
      .style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "0.5rem")
      .text("AGGREGATE(array, 0, (acc, x) -> acc + x) — step through the merge lambda.");

    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem");
    const stepBtn = ctrl.append("button")
      .style("padding", "5px 14px").style("border-radius", "5px").style("border", `1px solid ${PURPLE}`)
      .style("background", PURPLE).style("color", "#fff").style("cursor", "pointer").style("font-size", "0.74rem")
      .text("Step ▶")
      .on("click", () => { if (step < input.length) { step++; update(); } });
    ctrl.append("button")
      .style("padding", "5px 14px").style("border-radius", "5px").style("border", "1px solid #ccc")
      .style("background", "#fff").style("color", FG).style("cursor", "pointer").style("font-size", "0.74rem")
      .text("Reset ⟲")
      .on("click", () => { step = 0; update(); });

    const chipRow = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.6rem");
    const accDisplay = container.append("div").style("font-size", "0.9rem").style("font-family", "monospace").style("font-weight", "700");

    function update() {
      chipRow.selectAll("*").remove();
      let acc = 0;
      input.forEach((v, i) => {
        const merged = i < step;
        const isCurrent = i === step - 1;
        acc = merged ? acc + v : acc;
        chip(chipRow, String(v), {
          bg: isCurrent ? AMBER : merged ? "rgba(124,77,255,0.12)" : "#f5f5f7",
          fg: isCurrent ? "#fff" : merged ? PURPLE : GRAY,
          border: isCurrent ? AMBER : merged ? PURPLE : "#ddd",
        });
      });
      const done = step >= input.length;
      accDisplay
        .style("color", done ? HIT_COLOR : FG)
        .html(`acc = <span style="color:${done ? HIT_COLOR : PURPLE}">${acc}</span>` + (done ? "  ✓ finish_lambda would run on this value now" : `  (after ${step}/${input.length} elements)`));
      stepBtn.style("opacity", done ? 0.5 : 1);
    }
    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 6. PIPELINE — FILTER -> TRANSFORM
   * ══════════════════════════════════════════════════════════════════ */
  function renderPipeline(el) {
    const input = [1, 2, 3, 4, 5, 6];
    const filterPred = (x) => x % 2 === 0;
    const transformFn = (x) => x * x;

    const container = d3.select(el);
    container.selectAll("*").remove();
    container.append("div")
      .style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "0.5rem")
      .text("TRANSFORM(FILTER(array, x -> x % 2 = 0), x -> x * x)");

    function stageLabel(text) {
      container.append("div").style("font-size", "0.68rem").style("color", FG).style("font-weight", "700").style("margin-bottom", "3px").text(text);
    }

    stageLabel("Input");
    const inRow = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.5rem");
    input.forEach((v) => chip(inRow, String(v), { bg: "#f5f5f7", fg: FG, border: "#ddd" }));

    container.append("div").style("text-align", "center").style("color", GRAY).style("font-size", "0.85rem").style("margin", "2px 0").text("↓ Stage 1: FILTER(x -> x % 2 = 0) ↓");
    stageLabel("After FILTER (odd elements dropped — never reach stage 2)");
    const filterRow = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.5rem");
    const kept = [];
    input.forEach((v) => {
      const keep = filterPred(v);
      if (keep) kept.push(v);
      chip(filterRow, String(v), keep
        ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: "#f5f5f7", fg: GRAY, border: "#ddd", opacity: 0.35 });
    });

    container.append("div").style("text-align", "center").style("color", GRAY).style("font-size", "0.85rem").style("margin", "2px 0").text("↓ Stage 2: TRANSFORM(x -> x * x) ↓");
    stageLabel("Final result");
    const outRow = container.append("div").style("display", "flex").style("gap", "8px");
    kept.forEach((v) => chip(outRow, String(transformFn(v)), { bg: "rgba(124,77,255,0.12)", fg: PURPLE, border: PURPLE }));
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-lambda-anatomy", renderLambdaAnatomy],
      ["viz-param-binding", renderParamBinding],
      ["viz-zip-with-pad", renderZipWithPad],
      ["viz-map-zip-with", renderMapZipWith],
      ["viz-aggregate-fold", renderAggregateFold],
      ["viz-pipeline", renderPipeline],
    ];
    specs.forEach(([id, fn]) => {
      const el = document.getElementById(id);
      if (el && !el.dataset.rendered) { fn(el); el.dataset.rendered = "1"; }
    });
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
