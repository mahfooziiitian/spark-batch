/**
 * catalyst-viz.js
 * D3 v7 interactive visualizations for docs/optimization/catalyst/**.
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

  function renderCatalystFixedPoint(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const data = {
      parse: {
        line: "Parser → unresolved logical plan",
        tags: ["'Project", "'Filter", "'UnresolvedRelation"],
        note: "First observable stage in EXPLAIN EXTENDED; raw ANTLR tree is not shown.",
        color: AMBER,
      },
      analyze: {
        line: "Analyzer → resolved logical plan",
        tags: ["attributes get #ids", "casts inserted", "catalog lookup"],
        note: "Names and types are bound before optimization starts.",
        color: TEAL,
      },
      optimize: {
        line: "Optimizer → fixed-point rule batches",
        tags: ["PushDownPredicates", "ColumnPruning", "ConstantFolding", "BooleanSimplification"],
        note: "Spark 4.2 repeats batches such as 'Operator Optimization before/after Inferring Filters' until the plan stops changing.",
        color: PURPLE,
      },
      physical: {
        line: "Physical planner → selected SparkPlan",
        tags: ["BroadcastHashJoin", "SortMergeJoin", "HashAggregate"],
        note: "EXPLAIN FORMATTED shows the chosen operators, not every candidate strategy.",
        color: HIT_COLOR,
      },
      codegen: {
        line: "Whole-stage codegen → generated Java subtrees",
        tags: ["*(1) markers", "codegen id : 1", "GeneratedIteratorForCodegenStage1"],
        note: "EXPLAIN CODEGEN prints Java source for compatible fused subtrees.",
        color: RED,
      },
    };
    const group = btnGroup(container, [
      ["parse", "Parse"],
      ["analyze", "Analyze"],
      ["optimize", "Optimize"],
      ["physical", "Plan"],
      ["codegen", "Codegen"],
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const item = data[key];
      chip(stage, item.line, { bg: `${item.color}1a`, fg: item.color, border: item.color });
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0");
      item.tags.forEach((tag) => chip(row, tag, { bg: "#f5f5f7", fg: FG, border: "#ccc" }));
      box(stage, item.note, { bg: "rgba(124,77,255,0.05)", border: PURPLE, fg: FG });
    }
    render(group.get());
  }

  function renderLogicalRuleDiff(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const examples = {
      fold: {
        before: `Analyzed
Project [(1 + 1) AS x, (true AND (1 = 1)) AS ok]
+- OneRowRelation`,
        after: `Optimized
Project [2 AS x, true AS ok]
+- OneRowRelation`,
        tag: "ConstantFolding + BooleanSimplification",
      },
      pushdown: {
        before: `Analyzed
Project [id]
+- Filter (region = 'US')
   +- Project [id, amount, region]
      +- Filter (amount > 30)
         +- orders`,
        after: `Optimized
Filter ((amount > 30) AND (region = 'US'))
+- base scan / range`,
        tag: "PushDownPredicates + CombineFilters + ColumnPruning",
      },
      outer: {
        before: `Analyzed
Project [left_t.k]
+- Filter isnotnull(right_t.rv)
   +- Join LeftOuter, (left_t.k = right_t.k)`,
        after: `Optimized
Project [left_t.k]
+- Join Inner, (left_t.k = right_t.k)`,
        tag: "EliminateOuterJoin",
      },
    };
    const group = btnGroup(container, [
      ["fold", "Fold constants"],
      ["pushdown", "Push filters"],
      ["outer", "Eliminate outer join"],
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const ex = examples[key];
      chip(stage, ex.tag, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      const row = stage.append("div").style("display", "grid").style("grid-template-columns", "1fr 1fr").style("gap", "10px").style("margin-top", "8px");
      box(row.append("div"), ex.before, { bg: "rgba(255,167,38,0.1)", border: AMBER, fg: FG });
      box(row.append("div"), ex.after, { bg: "rgba(38,166,154,0.08)", border: HIT_COLOR, fg: FG });
    }
    render(group.get());
  }

  function renderPhysicalStrategyTree(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const cases = {
      bhj: {
        summary: "Small build side → BroadcastExchange + BroadcastHashJoin",
        plan: `BroadcastExchange
  ↓
BroadcastHashJoin Inner BuildRight`,
        color: HIT_COLOR,
      },
      smj: {
        summary: "Broadcast disabled or too large → Exchange + Sort + SortMergeJoin",
        plan: `Exchange(hashpartitioning) → Sort
Exchange(hashpartitioning) → Sort
             ↓
        SortMergeJoin`,
        color: PURPLE,
      },
      agg: {
        summary: "Grouped aggregate → partial HashAggregate before shuffle, final HashAggregate after shuffle",
        plan: `HashAggregate [partial_count(1)]
        ↓
Exchange(hashpartitioning(groupKey))
        ↓
HashAggregate [count(1)]`,
        color: TEAL,
      },
    };
    const group = btnGroup(container, [
      ["bhj", "Broadcast hash join"],
      ["smj", "Sort-merge join"],
      ["agg", "Two-phase aggregate"],
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const item = cases[key];
      chip(stage, item.summary, { bg: `${item.color}1a`, fg: item.color, border: item.color });
      box(stage, item.plan, { bg: "#f5f5f7", border: "#ccc", fg: FG }).style("margin-top", "8px");
    }
    render(group.get());
  }

  function renderCodegenFusion(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      ops: {
        chips: ["Range", "Filter", "Project"],
        note: "Spark 4.2 marked all three operators with the same codegen id.",
        color: AMBER,
      },
      fused: {
        chips: ["*(1) Range", "*(1) Filter", "*(1) Project"],
        note: "A shared *(1) / codegen id: 1 means one whole-stage subtree.",
        color: PURPLE,
      },
      java: {
        chips: ["GeneratedIteratorForCodegenStage1", "processNext()", "project_doConsume_0()"],
        note: "EXPLAIN CODEGEN printed Java source with the filter guard and projection logic fused into one iterator class.",
        color: HIT_COLOR,
      },
    };
    const group = btnGroup(container, [
      ["ops", "Physical ops"],
      ["fused", "Fused subtree"],
      ["java", "Generated Java"],
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const item = modes[key];
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      item.chips.forEach((c) => chip(row, c, { bg: `${item.color}1a`, fg: item.color, border: item.color }));
      box(stage, item.note, { bg: "rgba(38,166,154,0.06)", border: TEAL, fg: FG });
    }
    render(group.get());
  }

  function renderAstNodeExplorer(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const views = {
      sql: {
        text: "SELECT k + 1 AS v FROM big_t WHERE k < 3",
        note: "The original SQL text.",
        color: AMBER,
      },
      parser: {
        text: `ANTLR parser
↓
internal parse tree
(not exposed by EXPLAIN)`,
        note: "Spark uses the parse tree internally, but SQL-facing explain output skips directly to AstBuilder output.",
        color: RED,
      },
      parsed: {
        text: `'Project [('k + 1) AS v]
+- 'Filter ('k < 3)
   +- 'UnresolvedRelation [big_t]`,
        note: "This is the first stage you can observe in EXPLAIN EXTENDED.",
        color: PURPLE,
      },
      analyzed: {
        text: `Project [(k#1L + cast(1 as bigint)) AS v]
+- Filter (k#1L < cast(3 as bigint))
   +- SubqueryAlias big_t`,
        note: "The Analyzer resolved names and inserted casts.",
        color: HIT_COLOR,
      },
    };
    const group = btnGroup(container, [
      ["sql", "SQL text"],
      ["parser", "ANTLR"],
      ["parsed", "Parsed plan"],
      ["analyzed", "Analyzed plan"],
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const item = views[key];
      box(stage, item.text, { bg: `${item.color}12`, border: item.color, fg: FG });
      chip(stage.append("div").style("margin-top", "8px"), item.note, { bg: "#f5f5f7", fg: FG, border: "#ccc" });
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-catalyst-fixed-point", renderCatalystFixedPoint],
      ["viz-logical-rule-diff", renderLogicalRuleDiff],
      ["viz-physical-strategy-tree", renderPhysicalStrategyTree],
      ["viz-codegen-fusion", renderCodegenFusion],
      ["viz-ast-node-explorer", renderAstNodeExplorer],
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
