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

  function panel(container) {
    return container.append("div")
      .style("display", "grid")
      .style("gap", "10px")
      .style("background", "#fff")
      .style("border", "1px solid #e0e0e0")
      .style("border-radius", "8px")
      .style("padding", "12px");
  }

  function metricRow(container, label, value, color) {
    const row = container.append("div").style("display", "grid").style("grid-template-columns", "130px 1fr").style("gap", "10px");
    row.append("div").style("font-size", "0.72rem").style("color", GRAY).text(label);
    row.append("div").style("font-size", "0.78rem").style("color", color || FG).style("font-weight", color ? "700" : "400").text(value);
  }

  const STRATEGY_DATA = {
    overview: {
      title: "Verified join strategy outcomes",
      color: PURPLE,
      cards: [
        { name: "BHJ", operator: "BroadcastHashJoin", predicate: "equi-join", trigger: "small side or BROADCAST hint", shuffle: "broadcast only", memory: "broadcast hash table", status: HIT_COLOR },
        { name: "BNLJ", operator: "BroadcastNestedLoopJoin", predicate: "non-equi / cross", trigger: "broadcastable side", shuffle: "broadcast only", memory: "broadcast rows + nested loop", status: AMBER },
        { name: "SHJ", operator: "ShuffledHashJoin", predicate: "equi-join", trigger: "SHUFFLE_HASH hint or planner choice", shuffle: "both sides", memory: "per-partition hash table", status: TEAL },
        { name: "SMJ", operator: "SortMergeJoin", predicate: "equi-join", trigger: "general large-table fallback", shuffle: "both sides + sort", memory: "streaming merge", status: PURPLE },
        { name: "SRNL", operator: "CartesianProduct", predicate: "cross / non-equi path", trigger: "no broadcast nested-loop path", shuffle: "replicate/cartesian path", memory: "potentially huge output", status: RED },
      ],
      plan: "BroadcastHashJoin / BroadcastNestedLoopJoin / ShuffledHashJoin / SortMergeJoin / CartesianProduct",
      note: "Spark 4.2 uses operator names above in EXPLAIN FORMATTED; informal labels such as SSMJ or BMPJ are not distinct plan nodes.",
    },
    bhj: {
      title: "Broadcast hash join",
      color: HIT_COLOR,
      cards: [
        { name: "Predicate", operator: "equality only", predicate: "b.k = s.k", trigger: "small side under threshold", shuffle: "big side not shuffled", memory: "build side broadcast", status: HIT_COLOR },
        { name: "Threshold", operator: "spark.sql.autoBroadcastJoinThreshold", predicate: "10485760 by default", trigger: "-1 disables auto broadcast", shuffle: "hint still works", memory: "driver + executors", status: PURPLE },
      ],
      plan: "BroadcastExchange\nBroadcastHashJoin [k], [k], Inner, BuildRight",
      note: "Disabling auto broadcast changed the verified example to SortMergeJoin.",
    },
    bnlj: {
      title: "Broadcast nested-loop join",
      color: AMBER,
      cards: [
        { name: "Predicate", operator: "non-equi or cross", predicate: "b.k < s.k", trigger: "cannot use equi-join operators", shuffle: "broadcast only", memory: "broadcast side repeated per executor", status: AMBER },
        { name: "Join types", operator: "Inner / LeftOuter / FullOuter", predicate: "verified", trigger: "with broadcastable side", shuffle: "none on streamed side", memory: "nested loop cost", status: RED },
      ],
      plan: "BroadcastExchange\nBroadcastNestedLoopJoin Inner BuildRight\nJoin condition: (k < k)",
      note: "If no side can be broadcast, Spark may fall back to CartesianProduct instead.",
    },
    hashFamily: {
      title: "Hash join family",
      color: TEAL,
      cards: [
        { name: "Build", operator: "hash table", predicate: "on equi-join keys", trigger: "smaller side becomes build side", shuffle: "depends on variant", memory: "hash-based", status: TEAL },
        { name: "Probe", operator: "stream other side", predicate: "equality lookup", trigger: "per row match", shuffle: "broadcast or repartitioned", memory: "variant-specific", status: HIT_COLOR },
      ],
      plan: "BroadcastHashJoin ... BuildRight\nShuffledHashJoin ... BuildRight\n(no standalone HashJoin node seen)",
      note: "In Spark 4.2, 'hash join' is a family name, not the verified physical operator label.",
    },
    shj: {
      title: "Shuffled hash join",
      color: TEAL,
      cards: [
        { name: "Predicate", operator: "equality only", predicate: "b.k = s.k", trigger: "SHUFFLE_HASH hint verified", shuffle: "both sides repartition", memory: "per-partition build hash", status: TEAL },
        { name: "Preference", operator: "preferSortMergeJoin=false", predicate: "eligibility only", trigger: "did not force SHJ", shuffle: "planner may still pick SMJ", memory: "hint is clearer", status: PURPLE },
      ],
      plan: "ShuffledHashJoin [k], [k], Inner, BuildRight",
      note: "A tested FULL OUTER join with SHUFFLE_HASH still produced ShuffledHashJoin in Spark 4.2.",
    },
    smj: {
      title: "Sort-merge join",
      color: PURPLE,
      cards: [
        { name: "Predicate", operator: "equality only", predicate: "sortable keys", trigger: "broadcast unavailable or disabled", shuffle: "both sides", memory: "streaming merge", status: PURPLE },
        { name: "Plan shape", operator: "Exchange + Sort + SortMergeJoin", predicate: "verified", trigger: "MERGE hint or natural fallback", shuffle: "yes", memory: "lower than full hash build", status: GRAY },
      ],
      plan: "Exchange hashpartitioning(...)\nSort [k ASC NULLS FIRST]\nSortMergeJoin [k], [k], Inner",
      note: "Bucketing can remove Exchange nodes, but Spark may still keep Sort nodes.",
    },
    srnl: {
      title: "Shuffle-replicate NL / cartesian path",
      color: RED,
      cards: [
        { name: "Hint", operator: "SHUFFLE_REPLICATE_NL", predicate: "planner hint", trigger: "inner-like cartesian path", shuffle: "replicate/cross behavior", memory: "output can explode", status: RED },
        { name: "Plan node", operator: "CartesianProduct", predicate: "verified physical label", trigger: "broadcast disabled or not used", shuffle: "not BNLJ", memory: "N x M risk", status: AMBER },
      ],
      plan: "CartesianProduct\nJoin type: Inner\nJoin condition: (k = k) or None",
      note: "Spark 4.2 exposed CartesianProduct, not a distinct SRNLJ operator name, in the verified examples.",
    },
    ssmj: {
      title: "SSMJ terminology",
      color: PURPLE,
      cards: [
        { name: "Informal term", operator: "shuffle sort-merge join", predicate: "describes whole pipeline", trigger: "Exchange + Sort + SMJ", shuffle: "yes", memory: "streaming merge", status: PURPLE },
        { name: "Actual plan node", operator: "SortMergeJoin", predicate: "verified", trigger: "Spark 4.2 label", shuffle: "same operator as smj.md", memory: "same", status: HIT_COLOR },
      ],
      plan: "Exchange ...\nSort ...\nSortMergeJoin ...",
      note: "SSMJ is terminology overlap, not a separate strategy in Spark 4.2.",
    },
  };

  function renderStrategyViz(el, initialKey) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const options = [
      ["overview", "Overview"],
      ["bhj", "BHJ"],
      ["bnlj", "BNLJ"],
      ["hashFamily", "Hash family"],
      ["shj", "SHJ"],
      ["smj", "SMJ"],
      ["srnl", "SRNL"],
      ["ssmj", "SSMJ"],
    ];
    const group = btnGroup(container, options, render);
    let current = initialKey;
    if (group.get() !== initialKey) {
      container.selectAll("button").each(function () {
        if (d3.select(this).attr("data-v") === initialKey) {
          this.click();
        }
      });
      current = initialKey;
    }

    function render(key) {
      current = key;
      stage.selectAll("*").remove();
      const data = STRATEGY_DATA[key];
      stage.append("div").style("font-size", "0.9rem").style("font-weight", "700").style("color", data.color).text(data.title);

      const cards = stage.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(220px, 1fr))").style("gap", "10px").style("margin", "10px 0");
      data.cards.forEach((card) => {
        const p = panel(cards);
        chip(p, card.name, { bg: "rgba(124,77,255,0.08)", fg: card.status, border: card.status });
        metricRow(p, "operator", card.operator, card.status);
        metricRow(p, "predicate", card.predicate);
        metricRow(p, "trigger", card.trigger);
        metricRow(p, "shuffle", card.shuffle);
        metricRow(p, "memory", card.memory);
      });

      const detail = stage.append("div").style("display", "grid").style("gap", "10px");
      box(detail, data.plan, { bg: "#f8f7ff", border: data.color, fg: FG });
      detail.append("div").style("font-size", "0.74rem").style("color", GRAY).text(data.note);
    }

    if (current === initialKey) {
      render(initialKey);
    }
  }

  const TOOLKIT_DATA = {
    overview: {
      title: "Optimization toolkit",
      color: PURPLE,
      summary: "Use the cheapest fix that changes the real bottleneck.",
      chips: [
        ["Broadcast", HIT_COLOR], ["AQE skew", TEAL], ["Bucketing", PURPLE], ["Caching", GRAY], ["Separation", AMBER], ["Salting", RED], ["Iterative", RED],
      ],
      steps: ["Change operator when possible", "Reduce shuffle volume", "Target hot keys only when needed"],
      plan: "Best first checks:\n- Did the operator change?\n- Did Exchange disappear?\n- Did repeated scans disappear?",
    },
    skew: {
      title: "Skew handling options",
      color: RED,
      summary: "AQE helps after shuffle; manual rewrites help before or alongside it.",
      chips: [["AQE", TEAL], ["Broadcast", HIT_COLOR], ["Salting", RED], ["Split hot keys", AMBER]],
      steps: ["Measure hot keys", "Try broadcast or AQE first", "Escalate to salting/separation only if needed"],
      plan: "AQE configs:\n- adaptive.skewJoin.enabled\n- skewedPartitionFactor\n- skewedPartitionThresholdInBytes",
    },
    broadcast: {
      title: "Broadcast as a skew fix",
      color: HIT_COLOR,
      summary: "Avoid the skewed shuffle entirely when one side is truly small.",
      chips: [["changes operator", HIT_COLOR], ["avoids shuffle", HIT_COLOR], ["memory-sensitive", AMBER]],
      steps: ["Small side broadcast", "Large skewed side stays local", "Verify BroadcastHashJoin in plan"],
      plan: "BroadcastExchange\nBroadcastHashJoin ...\n(no Exchange on large side)",
    },
    bucketing: {
      title: "Bucketing for repeated joins",
      color: PURPLE,
      summary: "Bucketing is mainly about avoiding Exchange, not curing a single hot key.",
      chips: [["no Exchange verified", PURPLE], ["Sort still remained", AMBER], ["layout-dependent", GRAY]],
      steps: ["Write matching buckets", "Join on bucket column", "Check for bucketed scans and missing Exchange"],
      plan: "Scan ... Bucketed: true\nScan ... Bucketed: true\nSortMergeJoin",
    },
    caching: {
      title: "Cache only where reuse exists",
      color: GRAY,
      summary: "Caching saves repeated work; it does not redistribute skewed keys.",
      chips: [["reused filtered set", GRAY], ["reused salted set", PURPLE], ["not a skew fix", RED]],
      steps: ["Filter or transform first", "Cache reusable intermediate", "Join from the cached result"],
      plan: "CACHE TABLE recent_orders\n...\nJOIN dim_customer ...",
    },
    iterative: {
      title: "Iterative broadcast",
      color: RED,
      summary: "Lower peak broadcast size, but pay with repeated scans and unions.",
      chips: [["manual", RED], ["multiple passes", AMBER], ["correctness verified", HIT_COLOR]],
      steps: ["Chunk the small side", "Broadcast one chunk per pass", "UNION ALL the outputs"],
      plan: "pass 0 -> join\npass 1 -> join\npass 2 -> join\nUNION ALL",
    },
    separation: {
      title: "Separate heavy keys",
      color: AMBER,
      summary: "Give the hot keys their own path instead of salting every row.",
      chips: [["targeted", AMBER], ["needs hot-key list", GRAY], ["union paths", PURPLE]],
      steps: ["Identify hot keys", "Join hot and normal subsets differently", "UNION ALL results"],
      plan: "hot_fact -> specialized join\nnormal_fact -> regular join\nUNION ALL",
    },
    salting: {
      title: "Salt the skewed side",
      color: RED,
      summary: "Transform one hot key into several physical keys by adding a second join component.",
      chips: [["skewed side gets one salt", RED], ["other side expands to all salts", TEAL], ["join on key + salt", PURPLE]],
      steps: ["Compute salt on skewed side", "explode(sequence(...)) on the other side", "join on both columns"],
      plan: "fact(customer_id, salt)\ndim(customer_id, salt via explode(sequence))\njoin on (customer_id, salt)",
    },
  };

  function renderToolkitViz(el, initialKey) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const options = [
      ["overview", "Overview"],
      ["skew", "AQE skew"],
      ["broadcast", "Broadcast"],
      ["bucketing", "Bucketing"],
      ["caching", "Caching"],
      ["iterative", "Iterative"],
      ["separation", "Separation"],
      ["salting", "Salting"],
    ];
    const group = btnGroup(container, options, render);
    if (group.get() !== initialKey) {
      container.selectAll("button").each(function () {
        if (d3.select(this).attr("data-v") === initialKey) {
          this.click();
        }
      });
    }

    function render(key) {
      stage.selectAll("*").remove();
      const data = TOOLKIT_DATA[key];
      stage.append("div").style("font-size", "0.9rem").style("font-weight", "700").style("color", data.color).text(data.title);
      stage.append("div").style("font-size", "0.76rem").style("color", FG).style("margin", "6px 0 10px").text(data.summary);

      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "10px");
      data.chips.forEach(([text, color]) => chip(row, text, { bg: `${color}20`, fg: color, border: color }));

      const grid = stage.append("div").style("display", "grid").style("grid-template-columns", "1.2fr 1fr").style("gap", "10px");
      const left = panel(grid);
      data.steps.forEach((step, idx) => metricRow(left, `step ${idx + 1}`, step));
      const right = grid.append("div").style("display", "grid").style("gap", "10px");
      box(right, data.plan, { bg: "#f8f7ff", border: data.color, fg: FG });
    }

    render(initialKey);
  }

  function renderSaltingViz(el, saltedSmj) {
    const container = d3.select(el);
    container.selectAll("*").remove();

    const root = container.append("div").style("display", "grid").style("gap", "10px");
    const head = root.append("div");
    head.append("div").style("font-size", "0.9rem").style("font-weight", "700").style("color", saltedSmj ? PURPLE : RED)
      .text(saltedSmj ? "Salted keys flowing into SortMergeJoin" : "Correct salting pattern");
    head.append("div").style("font-size", "0.76rem").style("color", FG)
      .text(saltedSmj
        ? "Salting changes the join key shape; Spark 4.2 still reports SortMergeJoin as the operator when broadcast is disabled."
        : "The skewed side gets one salt per row; the other side expands to every salt value before the join.");

    const flow = root.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").style("gap", "10px");
    const left = panel(flow);
    chip(left, "Skewed side", { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED });
    metricRow(left, "key", "customer_id");
    metricRow(left, "extra column", saltedSmj ? "salt for SMJ key" : "salt");
    metricRow(left, "rule", "one salt value per row", RED);

    const middle = panel(flow);
    chip(middle, "Other side", { bg: "rgba(38,166,154,0.12)", fg: TEAL, border: TEAL });
    metricRow(middle, "expand with", "explode(sequence(0, n-1))", TEAL);
    metricRow(middle, "rule", "duplicate across all salts");
    metricRow(middle, "reason", "preserve correctness");

    const right = panel(flow);
    chip(right, saltedSmj ? "Join output" : "Join condition", { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
    metricRow(right, "join on", "customer_id + salt", PURPLE);
    metricRow(right, "operator", saltedSmj ? "SortMergeJoin (verified)" : "any equi-join operator");
    metricRow(right, "afterward", saltedSmj ? "optional re-aggregate" : "optional re-aggregate");

    box(root, saltedSmj
      ? "SortMergeJoin [customer_id, salt], [customer_id, salt], Inner"
      : "salted_fact(customer_id, txn, salt)\nexpanded_dim(customer_id, segment, salt)\nJOIN ON customer_id AND salt", { bg: "#f8f7ff", border: saltedSmj ? PURPLE : RED, fg: FG });
  }

  function init() {
    const specs = [
      ["viz-joins-strategy-overview", (el) => renderStrategyViz(el, "overview")],
      ["viz-joins-strategy-bhj", (el) => renderStrategyViz(el, "bhj")],
      ["viz-joins-strategy-bnlj", (el) => renderStrategyViz(el, "bnlj")],
      ["viz-joins-strategy-hash-family", (el) => renderStrategyViz(el, "hashFamily")],
      ["viz-joins-strategy-shj", (el) => renderStrategyViz(el, "shj")],
      ["viz-joins-strategy-smj", (el) => renderStrategyViz(el, "smj")],
      ["viz-joins-strategy-srnl", (el) => renderStrategyViz(el, "srnl")],
      ["viz-joins-strategy-ssmj", (el) => renderStrategyViz(el, "ssmj")],
      ["viz-joins-optimization-overview", (el) => renderToolkitViz(el, "overview")],
      ["viz-joins-skew-overview", (el) => renderToolkitViz(el, "skew")],
      ["viz-joins-skew-broadcast", (el) => renderToolkitViz(el, "broadcast")],
      ["viz-joins-skew-bucketing", (el) => renderToolkitViz(el, "bucketing")],
      ["viz-joins-skew-caching", (el) => renderToolkitViz(el, "caching")],
      ["viz-joins-skew-iterative-broadcast", (el) => renderToolkitViz(el, "iterative")],
      ["viz-joins-skew-separation", (el) => renderToolkitViz(el, "separation")],
      ["viz-joins-skew-salting", (el) => renderSaltingViz(el, false)],
      ["viz-joins-skew-salted-smj", (el) => renderSaltingViz(el, true)],
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
