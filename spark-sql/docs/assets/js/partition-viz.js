/**
 * partition-viz.js
 * D3 v7 interactive visualizations for the partition optimization pages
 * (docs/optimization/partition/{index,coalesce,rebalance/rebalance,repartition/repartition-hint}.md).
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

  function renderPartitionStrategySelector(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const strategies = {
      coalesce: {
        label: "COALESCE(n)",
        chips: [["shuffle: no", HIT_COLOR], ["increase: no", RED], ["aqe required: no", GRAY]],
        note: "Verified plan node: Coalesce(2) with no Exchange.",
        plan: "Coalesce (2)\n+- Range (1)",
      },
      repartition: {
        label: "REPARTITION(n)",
        chips: [["shuffle: yes", AMBER], ["increase: yes", HIT_COLOR], ["mode: round-robin", PURPLE]],
        note: "Verified plan: Exchange RoundRobinPartitioning(n).",
        plan: "AdaptiveSparkPlan\n+- Exchange\n   Arguments: RoundRobinPartitioning(4), REPARTITION_BY_NUM",
      },
      bykey: {
        label: "REPARTITION(n, key)",
        chips: [["shuffle: yes", AMBER], ["increase: yes", HIT_COLOR], ["mode: hash", PURPLE]],
        note: "Verified plan: Exchange hashpartitioning(key, n).",
        plan: "AdaptiveSparkPlan\n+- Exchange\n   Arguments: hashpartitioning(id, 5), REPARTITION_BY_NUM",
      },
      rebalance: {
        label: "REBALANCE(...)",
        chips: [["shuffle: AQE only", AMBER], ["aqe required: yes", RED], ["final count: adaptive", PURPLE]],
        note: "Verified behavior: ignored when AQE is off; adaptive shuffle when AQE is on.",
        plan: "AdaptiveSparkPlan\n+- Exchange\n   Arguments: RoundRobinPartitioning(4), REBALANCE_PARTITIONS_BY_NONE",
      },
    };
    const group = btnGroup(container, [
      ["coalesce", "COALESCE"],
      ["repartition", "REPARTITION(n)"],
      ["bykey", "REPARTITION(n,key)"],
      ["rebalance", "REBALANCE"],
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const s = strategies[key];
      stage.append("div").style("font-size", "0.76rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "6px").text(s.label);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      s.chips.forEach(([label, color]) => {
        chip(row, label, { bg: `${color}1a`, fg: color, border: color });
      });
      box(stage, s.plan, { bg: "#fafafa", border: "#ddd", fg: FG });
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px").text(s.note);
    }
    render(group.get());
  }

  function renderCoalesceMerging(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let target = 3;
    const source = [18, 6, 14, 5, 11, 8];

    const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "10px");
    ctrl.append("span").style("font-size", "0.74rem").text("Target partitions:");
    const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    ctrl.append("input").attr("type", "range").attr("min", 1).attr("max", source.length).attr("value", target).style("width", "180px")
      .on("input", function () { target = +this.value; render(); });

    const stage = container.append("div");

    function render() {
      label.text(target);
      stage.selectAll("*").remove();
      const groups = d3.range(target).map(() => []);
      source.forEach((value, idx) => groups[idx % target].push(value));

      const top = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      source.forEach((size, idx) => chip(top, `P${idx + 1} ${size} MB`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE }));

      stage.append("div").style("text-align", "center").style("font-size", "0.85rem").style("color", GRAY).style("margin", "6px 0").text("↓ narrow dependency, no Exchange ↓");

      const bottom = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      groups.forEach((group, idx) => {
        const total = group.reduce((a, b) => a + b, 0);
        chip(bottom, `C${idx + 1} ${group.join("+")} = ${total} MB`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      });

      stage.append("div").style("font-size", "0.7rem").style("color", GRAY)
        .text(target > source.length
          ? "Coalesce cannot create more partitions than already exist."
          : "Spark is collapsing existing partitions into fewer readers instead of redistributing every row.");
    }
    render();
  }

  function renderRebalanceAqe(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      on: {
        title: "AQE on",
        before: [92, 10, 8, 6],
        after: [34, 28, 27, 27],
        plan: "AdaptiveSparkPlan\n+- AQEShuffleRead coalesced and skewed\n   +- Exchange RoundRobinPartitioning(4), REBALANCE_PARTITIONS_BY_NONE",
        chipText: "REBALANCE active",
        chipColor: HIT_COLOR,
      },
      off: {
        title: "AQE off",
        before: [92, 10, 8, 6],
        after: [92, 10, 8, 6],
        plan: "Range\n(no rebalance exchange inserted)",
        chipText: "hint ignored",
        chipColor: RED,
      },
    };
    const group = btnGroup(container, [["on", "AQE on"], ["off", "AQE off"]], render);

    function histogram(parent, values, color, label) {
      const wrap = parent.append("div").style("flex", "1");
      wrap.append("div").style("font-size", "0.7rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "6px").text(label);
      const row = wrap.append("div").style("display", "flex").style("align-items", "flex-end").style("gap", "6px").style("height", "100px");
      values.forEach((v, idx) => {
        const col = row.append("div").style("display", "flex").style("flex-direction", "column").style("align-items", "center").style("gap", "4px");
        col.append("div").style("width", "36px").style("height", `${Math.max(12, v)}px`).style("background", `${color}1a`).style("border", `1px solid ${color}`).style("border-radius", "4px 4px 0 0");
        col.append("span").style("font-size", "0.64rem").style("color", GRAY).text(`P${idx + 1}`);
      });
    }

    function render(mode) {
      stage.selectAll("*").remove();
      const m = modes[mode];
      const head = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(head, m.title, { bg: `${m.chipColor}1a`, fg: m.chipColor, border: m.chipColor });
      chip(head, m.chipText, { bg: `${m.chipColor}1a`, fg: m.chipColor, border: m.chipColor });

      const row = stage.append("div").style("display", "flex").style("gap", "12px").style("margin-bottom", "8px");
      histogram(row, m.before, AMBER, "Before");
      histogram(row, m.after, mode === "on" ? HIT_COLOR : RED, "After");
      box(stage, m.plan, { bg: "#fafafa", border: "#ddd", fg: FG });
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px")
        .text(mode === "on"
          ? "With AQE enabled, the rebalance exchange gives Spark a chance to smooth out skewed output partitions."
          : "With AQE disabled, Spark 4.2 leaves the query unchanged; there is no rebalance fallback shuffle.");
    }
    render(group.get());
  }

  function renderRepartitionShuffle(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let partitions = 4;
    const stage = container.append("div");
    const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "10px");
    const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    ctrl.append("input").attr("type", "range").attr("min", 2).attr("max", 8).attr("value", partitions).style("width", "180px")
      .on("input", function () { partitions = +this.value; render(group.get()); });
    const group = btnGroup(container, [
      ["round", "REPARTITION(n)"],
      ["hash", "REPARTITION(n,key)"],
      ["range", "REPARTITION_BY_RANGE(n,key)"],
    ], render);

    function planFor(mode) {
      if (mode === "round") {
        return {
          title: "Round-robin",
          arrows: ["A→1", "B→2", "C→3", "D→4"],
          plan: `AdaptiveSparkPlan\n+- Exchange\n   Arguments: RoundRobinPartitioning(${partitions}), REPARTITION_BY_NUM`,
          note: "No key supplied: Spark evens rows out by round-robin.",
        };
      }
      if (mode === "hash") {
        return {
          title: "Hash",
          arrows: ["US→hash", "EU→hash", "US→same bucket", "APAC→hash"],
          plan: `AdaptiveSparkPlan\n+- Exchange\n   Arguments: hashpartitioning(region, ${partitions}), REPARTITION_BY_NUM`,
          note: "Rows with the same key hash to the same partition.",
        };
      }
      return {
        title: "Range",
        arrows: ["1-25", "26-50", "51-75", "76-100"],
        plan: `AdaptiveSparkPlan\n+- Exchange\n   Arguments: rangepartitioning(id ASC NULLS FIRST, ${partitions}), REPARTITION_BY_NUM`,
        note: "Spark builds ordered key ranges instead of hash buckets.",
      };
    }

    function render(mode) {
      label.text(`n = ${partitions}`);
      stage.selectAll("*").remove();
      const p = planFor(mode);
      stage.append("div").style("font-size", "0.76rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "6px").text(p.title);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      p.arrows.forEach((text, idx) => {
        chip(row, text, { bg: `${[PURPLE, TEAL, AMBER, RED][idx % 4]}1a`, fg: [PURPLE, TEAL, AMBER, RED][idx % 4], border: [PURPLE, TEAL, AMBER, RED][idx % 4] });
      });
      box(stage, p.plan, { bg: "#fafafa", border: "#ddd", fg: FG });
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px").text(p.note);
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-partition-strategy-selector", renderPartitionStrategySelector],
      ["viz-coalesce-merging", renderCoalesceMerging],
      ["viz-rebalance-aqe", renderRebalanceAqe],
      ["viz-repartition-shuffle", renderRepartitionShuffle],
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
