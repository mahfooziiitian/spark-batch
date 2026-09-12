/**
 * internals-viz.js
 * D3 v7 interactive visualizations for the Internals pages
 * (docs/internals/planner/{query-parsing,query-planner}.md,
 * docs/internals/filereader/sql-file-reader.md).
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

  /* 1. CSV/JSON/Parquet/ORC schema resolution (sql-file-reader.md) */
  function renderSchemaResolution(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const info = {
      parquet: { typed: true, cols: ["id: BIGINT", "amount: DOUBLE"], note: "Schema is embedded in the file footer — always fully typed, no scan needed." },
      orc: { typed: true, cols: ["id: BIGINT", "amount: DOUBLE"], note: "Same as Parquet — self-describing columnar format." },
      json: { typed: true, cols: ["id: BIGINT"], note: "Shorthand infers types by sampling the file (numeric literal -> BIGINT/DOUBLE)." },
      csv: { typed: false, cols: ["_c0: STRING", "_c1: STRING"], note: "inferSchema defaults to false for the bare shorthand -- every column is STRING, positionally named." },
    };
    const group = btnGroup(container, [["parquet", "parquet.`path`"], ["orc", "orc.`path`"], ["json", "json.`path`"], ["csv", "csv.`path`"]], render);

    function render(fmt) {
      stage.selectAll("*").remove();
      const i = info[fmt];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "6px");
      i.cols.forEach((c) => chip(row, c, i.typed
        ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(i.note);
    }
    render(group.get());
  }

  /* 2. Pipeline stage stepper (query-parsing.md) */
  function renderPipelineStages(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stages = [
      { name: "Parser (ANTLR)", plan: "'Project [*]\n+- 'Filter ('amount > 100)\n   +- 'UnresolvedRelation [sales]" },
      { name: "Analyzer", plan: "Project [id#0L, amount#1L]\n+- Filter (amount#1L > cast(100 as bigint))\n   +- SubqueryAlias sales" },
      { name: "Catalyst Optimizer", plan: "Project [id#0L, (id#0L * 10) AS amount#1L]\n+- Filter ((id#0L * 10) > 100)\n   +- Range (0, 10, step=1)" },
      { name: "Physical Planner", plan: "Project [id#0L, (id#0L * 10) AS amount#1L]\n+- Filter ((id#0L * 10) > 100)\n   +- Range (0, 10, step=1)" },
      { name: "Code Generation", plan: "*(1) Project [id#0L, (id#0L * 10) AS amount#1L]\n+- *(1) Filter ((id#0L * 10) > 100)\n   +- *(1) Range (0, 10, step=1, splits=1)" },
    ];
    const tabRow = container.append("div").style("display", "flex").style("gap", "4px").style("margin-bottom", "8px").style("flex-wrap", "wrap");
    const planBox = container.append("div");
    let idx = 0;

    function render() {
      tabRow.selectAll("*").remove();
      stages.forEach((s, i) => {
        chip(tabRow, `${i + 1}. ${s.name}`, i === idx
          ? { bg: PURPLE, fg: "#fff", border: PURPLE }
          : { bg: "#f5f5f7", fg: FG, border: "#ccc" })
          .style("cursor", "pointer")
          .on("click", () => { idx = i; render(); });
      });
      planBox.selectAll("*").remove();
      box(planBox, stages[idx].plan);
    }
    render();
  }

  /* 3. Join strategy selector by side sizes (query-planner.md) */
  function renderJoinStrategy(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let leftMB = 500, rightMB = 5;
    const thresholdMB = 10;

    const rows = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "8px").style("margin-bottom", "8px");
    const leftRow = rows.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px");
    leftRow.append("span").style("font-size", "0.74rem").style("width", "110px").text("Left side (MB):");
    const leftLabel = leftRow.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    leftRow.append("input").attr("type", "range").attr("min", 1).attr("max", 1000).attr("value", leftMB).style("width", "160px")
      .on("input", function () { leftMB = +this.value; update(); });

    const rightRow = rows.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px");
    rightRow.append("span").style("font-size", "0.74rem").style("width", "110px").text("Right side (MB):");
    const rightLabel = rightRow.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    rightRow.append("input").attr("type", "range").attr("min", 1).attr("max", 1000).attr("value", rightMB).style("width", "160px")
      .on("input", function () { rightMB = +this.value; update(); });

    const resultWrap = container.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-top", "4px");

    function pickStrategy() {
      const minMB = Math.min(leftMB, rightMB);
      if (minMB <= thresholdMB) return "BroadcastHashJoin";
      if (minMB <= thresholdMB * 3) return "ShuffleHashJoin";
      return "SortMergeJoin";
    }

    function update() {
      leftLabel.text(leftMB);
      rightLabel.text(rightMB);
      const strat = pickStrategy();
      const color = strat === "BroadcastHashJoin" ? HIT_COLOR : strat === "ShuffleHashJoin" ? AMBER : PURPLE;
      resultWrap.html(`Planner picks: <b style="color:${color}">${strat}</b> &nbsp; (threshold = ${thresholdMB} MB)`);
    }
    update();
  }

  /* 4. Two-phase aggregation flow (query-planner.md) */
  function renderTwoPhaseAgg(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const partitions = [
      { name: "Partition 1", rows: 4, partial: 4 },
      { name: "Partition 2", rows: 6, partial: 6 },
      { name: "Partition 3", rows: 3, partial: 3 },
    ];
    const stage = container.append("div");

    stage.append("div").style("font-size", "0.68rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px")
      .text("Phase 1: partial_count(1) runs locally per partition (no shuffle)");
    const p1 = stage.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "8px");
    partitions.forEach((p) => {
      const col = p1.append("div").style("text-align", "center");
      col.append("div").style("font-size", "0.66rem").style("color", GRAY).text(p.name);
      chip(col, `${p.rows} rows -> partial_count = ${p.partial}`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
    });

    stage.append("div").style("text-align", "center").style("color", GRAY).style("font-size", "0.85rem").style("margin", "4px 0")
      .text("↓ Exchange (shuffle only the 3 small partial counts) ↓");

    const total = partitions.reduce((a, p) => a + p.partial, 0);
    stage.append("div").style("font-size", "0.68rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px")
      .text("Phase 2: final HashAggregate combines the partials");
    chip(stage, `count(1) = ${total}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-schema-resolution", renderSchemaResolution],
      ["viz-pipeline-stages", renderPipelineStages],
      ["viz-join-strategy", renderJoinStrategy],
      ["viz-two-phase-agg", renderTwoPhaseAgg],
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
