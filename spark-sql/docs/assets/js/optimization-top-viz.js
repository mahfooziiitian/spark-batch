/**
 * optimization-top-viz.js
 * D3 v7 interactive visualizations for the top-level Optimization pages
 * (docs/optimization/{index,optimization,predicate-pushdown,shuffling,profiling}.md).
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

  /* 1. Optimization layer picker: technique -> layer -> impact/effort (index.md) */
  function renderLayerPicker(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const techniques = {
      "predicate-pushdown": { layer: "Catalyst / storage", impact: "High", effort: "Zero — automatic", when: "Runtime, on every scan" },
      "aqe-coalesce": { layer: "AQE (runtime)", impact: "Medium", effort: "Zero — enabled by default", when: "After shuffle stages complete" },
      "aqe-skew": { layer: "AQE (runtime)", impact: "High", effort: "Zero — enabled by default", when: "Detected from shuffle map stats" },
      "broadcast-join": { layer: "Catalyst / AQE", impact: "High", effort: "Low — hint or threshold config", when: "Compile-time or AQE re-plan" },
      "zorder": { layer: "Storage (Delta)", impact: "High", effort: "Low — OPTIMIZE … ZORDER", when: "Batch maintenance job" },
      "bucketing": { layer: "Storage / join", impact: "High", effort: "Medium — schema change", when: "Table design time" },
    };
    const group = btnGroup(container, [
      ["predicate-pushdown", "Predicate pushdown"],
      ["aqe-coalesce", "AQE coalesce"],
      ["aqe-skew", "AQE skew join"],
      ["broadcast-join", "Broadcast join"],
      ["zorder", "Z-ORDER"],
      ["bucketing", "Bucketing"],
    ], render);
    const stage = container.append("div");

    function render(key) {
      stage.selectAll("*").remove();
      const t = techniques[key];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
      chip(row, `layer: ${t.layer}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      chip(row, `impact: ${t.impact}`, t.impact === "High"
        ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      chip(row, `effort: ${t.effort}`, { bg: "#f5f5f7", fg: FG, border: "#ccc" });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(`Applied: ${t.when}`);
    }
    render(group.get());
  }

  /* 2. UDF-in-WHERE pushdown comparison (optimization.md / predicate-pushdown.md) */
  function renderPushdownCompare(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const modes = {
      sql: {
        label: "WHERE region = 'US'",
        plan: "Scan parquet\n  PushedFilters: [IsNotNull(region), EqualTo(region,US)]\n  ReadSchema: struct<id:bigint,region:string>",
        pushed: true,
      },
      udf: {
        label: "WHERE my_udf(region) = 'US'",
        plan: "Filter (pythonUDF0#3)\n+- BatchEvalPython [my_udf(region#1)]\n   +- Scan parquet\n        PushedFilters: []   <- verified empty with PySpark 4.2\n        ReadSchema: struct<id:bigint,region:string>",
        pushed: false,
      },
    };
    const group = btnGroup(container, [["sql", "Direct column filter"], ["udf", "UDF wraps column"]], render);
    const stage = container.append("div");

    function render(key) {
      stage.selectAll("*").remove();
      const m = modes[key];
      stage.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("margin-bottom", "6px").text(m.label);
      box(stage, m.plan, m.pushed
        ? { bg: "rgba(38,166,154,0.08)", border: HIT_COLOR, fg: FG }
        : { bg: "rgba(239,83,80,0.08)", border: RED, fg: FG });
      chip(stage, m.pushed ? "Pushdown: active" : "Pushdown: blocked — full column scan + row-by-row filter", m.pushed
        ? { bg: HIT_COLOR, fg: "#fff", border: HIT_COLOR }
        : { bg: RED, fg: "#fff", border: RED });
    }
    render(group.get());
  }

  /* 3. Shuffle mechanics: map -> exchange -> reduce, with partition count slider (shuffling.md) */
  function renderShuffleMechanics(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let n = 3;

    const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "10px");
    ctrl.append("span").style("font-size", "0.74rem").text("Shuffle partitions:");
    const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px").text(n);
    ctrl.append("input").attr("type", "range").attr("min", 2).attr("max", 6).attr("value", n).style("width", "140px")
      .on("input", function () { n = +this.value; render(); });

    const svgWrap = container.append("div");

    function render() {
      label.text(n);
      svgWrap.selectAll("*").remove();
      const width = 480, height = 170;
      const svg = svgWrap.append("svg").attr("viewBox", `0 0 ${width} ${height}`).style("width", "100%").style("max-width", "480px");
      const mapY = 20, exY = 80, redY = 140;
      const xs = d3.range(n).map((i) => 40 + (i * (width - 80)) / Math.max(1, n - 1));

      xs.forEach((x, i) => {
        svg.append("rect").attr("x", x - 26).attr("y", mapY).attr("width", 52).attr("height", 22).attr("rx", 4)
          .attr("fill", "rgba(124,77,255,0.12)").attr("stroke", PURPLE);
        svg.append("text").attr("x", x).attr("y", mapY + 15).attr("text-anchor", "middle").attr("font-size", "9px").attr("fill", FG).text(`Map ${i + 1}`);
      });

      svg.append("rect").attr("x", width / 2 - 60).attr("y", exY).attr("width", 120).attr("height", 22).attr("rx", 4)
        .attr("fill", "rgba(255,167,38,0.15)").attr("stroke", AMBER);
      svg.append("text").attr("x", width / 2).attr("y", exY + 15).attr("text-anchor", "middle").attr("font-size", "9px").attr("fill", FG).text("Exchange (hash by key)");

      xs.forEach((x, i) => {
        svg.append("rect").attr("x", x - 26).attr("y", redY).attr("width", 52).attr("height", 22).attr("rx", 4)
          .attr("fill", "rgba(38,166,154,0.12)").attr("stroke", HIT_COLOR);
        svg.append("text").attr("x", x).attr("y", redY + 15).attr("text-anchor", "middle").attr("font-size", "9px").attr("fill", FG).text(`Reduce ${i + 1}`);

        svg.append("line").attr("x1", x).attr("y1", mapY + 22).attr("x2", width / 2).attr("y2", exY)
          .attr("stroke", GRAY).attr("stroke-width", 1).attr("marker-end", null);
        svg.append("line").attr("x1", width / 2).attr("y1", exY + 22).attr("x2", x).attr("y2", redY)
          .attr("stroke", GRAY).attr("stroke-width", 1);
      });
    }
    render();
  }

  /* 4. REPARTITION vs COALESCE (shuffling.md) */
  function renderRepartitionVsCoalesce(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const modes = {
      repartition: { shuffle: true, note: "Exchange hashpartitioning(...) — full shuffle, can increase or decrease partitions, balances rows.", color: AMBER },
      coalesce: { shuffle: false, note: "Coalesce — narrow dependency, no Exchange node, can only decrease partitions, may leave partitions skewed.", color: HIT_COLOR },
    };
    const group = btnGroup(container, [["repartition", "REPARTITION(n)"], ["coalesce", "COALESCE(n)"]], render);
    const stage = container.append("div");

    function render(key) {
      stage.selectAll("*").remove();
      const m = modes[key];
      chip(stage, m.shuffle ? "Shuffle: YES (Exchange)" : "Shuffle: NO (local merge)", { bg: m.color, fg: "#fff", border: m.color });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px").text(m.note);
    }
    render(group.get());
  }

  /* 5. EXPLAIN FORMATTED plan tree with tooltips (profiling.md) */
  function renderExplainTree(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const nodes = [
      { id: 1, label: "AdaptiveSparkPlan", note: "AQE wraps the plan; re-optimizes after each shuffle stage completes." },
      { id: 2, label: "HashAggregate (final)", note: "Merges the partial aggregates coming out of the shuffle." },
      { id: 3, label: "Exchange", note: "Shuffle boundary — check estimated size here to judge cost." },
      { id: 4, label: "HashAggregate (partial)", note: "Map-side combine before the shuffle — reduces bytes written." },
      { id: 5, label: "Project", note: "Column pruning applied — only referenced columns survive here." },
      { id: 6, label: "Filter", note: "Predicate applied after the scan for anything not pushed down." },
      { id: 7, label: "FileScan parquet", note: "Check PartitionFilters / PushedFilters / ReadSchema on this node." },
    ];
    const list = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px");
    const noteBox = container.append("div").style("margin-top", "8px");
    box(noteBox, "Click a plan node above to see what to check.");

    nodes.forEach((n) => {
      const row = list.append("div")
        .style("padding", "5px 10px").style("margin-left", `${(n.id - 1) * 14}px`)
        .style("border", "1px solid #ccc").style("border-radius", "4px")
        .style("font-family", "monospace").style("font-size", "0.75rem")
        .style("cursor", "pointer").style("background", "#f5f5f7").style("color", FG)
        .text(`(${n.id}) ${n.label}`)
        .on("click", () => {
          list.selectAll("div").style("background", "#f5f5f7").style("border-color", "#ccc");
          row.style("background", "rgba(124,77,255,0.12)").style("border-color", PURPLE);
          noteBox.selectAll("*").remove();
          box(noteBox, n.note, { bg: "rgba(124,77,255,0.08)", border: PURPLE });
        });
    });
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-optimization-layers", renderLayerPicker],
      ["viz-pushdown-compare", renderPushdownCompare],
      ["viz-shuffle-mechanics", renderShuffleMechanics],
      ["viz-repartition-vs-coalesce", renderRepartitionVsCoalesce],
      ["viz-explain-tree", renderExplainTree],
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
