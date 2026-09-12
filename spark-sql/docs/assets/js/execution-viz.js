/**
 * execution-viz.js
 * D3 v7 interactive visualizations for docs/optimization/execution/
 * (index.md, query-lifecycle.md, explain.md).
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

  function renderLazyVsAction(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const steps = {
      build: {
        chips: [
          ["spark.sql(...)", PURPLE],
          ["where(...)", TEAL],
          ["groupBy(...)", TEAL],
          ["select(...)", TEAL],
        ],
        note: "Spark keeps extending a logical plan. No tasks run yet.",
        plan: "LogicalPlan\n+- Project\n   +- Aggregate\n      +- Filter\n         +- Range",
        status: { text: "Scheduler state: idle", color: GRAY },
      },
      explain: {
        chips: [
          ["spark.sql(...)", PURPLE],
          ["transformations", TEAL],
          ["explain()", AMBER],
        ],
        note: "Verified in PySpark 4.2: explain() compiles and prints the plan, but does not execute a failing Python UDF.",
        plan: "PhysicalPlan\n*(2) HashAggregate\n+- Exchange\n   +- *(1) HashAggregate\n      +- *(1) Range",
        status: { text: "Scheduler state: still idle", color: AMBER },
      },
      action: {
        chips: [
          ["spark.sql(...)", PURPLE],
          ["transformations", TEAL],
          ["collect()", RED],
        ],
        note: "An action turns the plan into jobs, stages, and tasks. This is when scans, UDFs, shuffles, and writes actually happen.",
        plan: "Job 0\n Stage 0 -> partial work\n Stage 1 -> post-shuffle work\n Tasks execute on executors",
        status: { text: "Scheduler state: running tasks", color: RED },
      },
    };
    const group = btnGroup(container, [["build", "Build plan"], ["explain", "Inspect plan"], ["action", "Run action"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const cfg = steps[key];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      cfg.chips.forEach(([label, color]) => {
        chip(row, label, { bg: `${color}1a`, fg: color, border: color });
      });
      chip(stage.append("div").style("margin-bottom", "8px"), cfg.status.text, { bg: `${cfg.status.color}1a`, fg: cfg.status.color, border: cfg.status.color });
      box(stage.append("div").style("margin-bottom", "8px"), cfg.plan, { bg: "#f5f5f7", border: "#ddd" });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(cfg.note);
    }
    render(group.get());
  }

  function renderStageBoundaries(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      no_exchange: {
        summary: ["Query", "SELECT * FROM range(100) WHERE id >= 95"],
        plan: "*(1) Filter (id >= 95)\n+- *(1) Range (0, 100, step=1, splits=2)",
        stages: [
          { name: "Stage 0", tasks: "2 tasks", color: HIT_COLOR, note: "One input stage because there is no Exchange." },
        ],
        footer: "Verified with statusTracker(): one job, one stage, two tasks on local[2].",
      },
      one_exchange: {
        summary: ["Query", "SELECT id % 5 AS g, COUNT(*) AS c FROM range(1000) GROUP BY id % 5"],
        plan: "*(2) HashAggregate\n+- Exchange hashpartitioning(..., 200)\n   +- *(1) HashAggregate\n      +- *(1) Range (0, 1000, step=1, splits=2)",
        stages: [
          { name: "Stage 0", tasks: "2 tasks", color: PURPLE, note: "Upstream scan + partial aggregate over 2 input splits." },
          { name: "Stage 1", tasks: "200 tasks", color: AMBER, note: "Downstream post-shuffle stage uses spark.sql.shuffle.partitions=200." },
        ],
        footer: "Verified with AQE off: one Exchange created two stages in one job.",
      },
    };
    const group = btnGroup(container, [["no_exchange", "No Exchange"], ["one_exchange", "One Exchange"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const cfg = modes[key];
      const info = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(info, `${cfg.summary[0]}:`, { bg: "#f5f5f7", fg: FG, border: "#ccc" });
      chip(info, cfg.summary[1], { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      box(stage.append("div").style("margin-bottom", "10px"), cfg.plan, { bg: "#f5f5f7", border: "#ddd" });

      const lane = stage.append("div").style("display", "flex").style("align-items", "flex-start").style("gap", "10px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(lane, "Action → Job 0", { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      cfg.stages.forEach((s, idx) => {
        const col = lane.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px");
        chip(col, s.name, { bg: `${s.color}1a`, fg: s.color, border: s.color });
        chip(col, s.tasks, { bg: "#f5f5f7", fg: FG, border: "#ccc" });
        col.append("div").style("font-size", "0.7rem").style("color", GRAY).style("max-width", "180px").text(s.note);
        if (idx < cfg.stages.length - 1) {
          lane.append("div").style("font-size", "1.1rem").style("color", AMBER).style("padding-top", "6px").text("→");
        }
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(cfg.footer);
    }
    render(group.get());
  }

  function renderExplainModes(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      explain: {
        note: "Bare EXPLAIN: physical plan only.",
        sample: "== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=false\n+- HashAggregate\n   +- Exchange\n      +- HashAggregate\n         +- Range",
        badge: { text: "Best for: quick operator check", color: HIT_COLOR },
      },
      extended: {
        note: "Adds Parsed, Analyzed, and Optimized logical plans before the physical plan.",
        sample: "== Parsed Logical Plan ==\n'Aggregate ...\n\n== Analyzed Logical Plan ==\ng: bigint, c: bigint\n\n== Optimized Logical Plan ==\nAggregate ...\n\n== Physical Plan ==\nAdaptiveSparkPlan isFinalPlan=false",
        badge: { text: "Best for: rewrite history", color: PURPLE },
      },
      formatted: {
        note: "Numbered tree plus per-node detail blocks.",
        sample: "== Physical Plan ==\nAdaptiveSparkPlan (6)\n+- HashAggregate (5)\n   +- Exchange (4)\n      +- HashAggregate (3)\n         +- Project (2)\n            +- Range (1)",
        badge: { text: "Best for: readable large plans", color: AMBER },
      },
      cost: {
        note: "Shows optimized-plan statistics. sizeInBytes may appear before ANALYZE; rowCount improves after statistics collection.",
        sample: "== Optimized Logical Plan ==\nAggregate ..., Statistics(sizeInBytes=240.0 B, rowCount=10)\n+- Project ..., Statistics(sizeInBytes=1600.0 B, rowCount=100)\n   +- Relation ..., Statistics(sizeInBytes=2.3 KiB, rowCount=100)",
        badge: { text: "Best for: CBO visibility", color: TEAL },
      },
      codegen: {
        note: "Shows whole-stage-codegen subtrees and generated Java for fuseable plans.",
        sample: "Found 1 WholeStageCodegen subtrees.\n== Subtree 1 / 1 ==\n*(1) Project [(id + 1) AS v]\n+- *(1) Filter (id > 1)\n   +- *(1) Range (0, 5, step=1, splits=2)\n\nGenerated code:\n/* 001 */ public Object generate(...) {",
        badge: { text: "Best for: fusion diagnostics", color: RED },
      },
    };
    const group = btnGroup(container, [["explain", "EXPLAIN"], ["extended", "EXTENDED"], ["formatted", "FORMATTED"], ["cost", "COST"], ["codegen", "CODEGEN"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const cfg = modes[key];
      chip(stage.append("div").style("margin-bottom", "8px"), cfg.badge.text, { bg: `${cfg.badge.color}1a`, fg: cfg.badge.color, border: cfg.badge.color });
      box(stage.append("div").style("margin-bottom", "8px"), cfg.sample, { bg: "#f5f5f7", border: "#ddd" });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(cfg.note);
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-execution-lazy-action", renderLazyVsAction],
      ["viz-execution-stage-boundaries", renderStageBoundaries],
      ["viz-explain-mode-comparer", renderExplainModes],
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
