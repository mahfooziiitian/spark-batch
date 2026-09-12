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

      function scaleBars(parent, values, activeIndex, color) {
        const wrap = parent.append("div")
          .style("display", "flex")
          .style("align-items", "flex-end")
          .style("gap", "6px")
          .style("height", "110px")
          .style("margin-bottom", "8px");
        values.forEach((v, i) => {
          wrap.append("div")
            .style("width", "24px")
            .style("height", `${Math.max(10, v)}px`)
            .style("background", i === activeIndex ? color : "#e0e0e0")
            .style("border-radius", "4px 4px 0 0")
            .attr("title", String(v));
        });
      }

      function renderAqeOverview(el) {
        const container = d3.select(el);
        container.selectAll("*").remove();
        const stage = container.append("div");
        const options = {
          coalesce: {
            chips: [["stage complete", PURPLE], ["tiny reducers", AMBER], ["AQEShuffleRead coalesced", HIT_COLOR]],
            text: "Shuffle stats show many small reducers, so AQE merges adjacent reads into fewer tasks.",
          },
          bhj: {
            chips: [["stage complete", PURPLE], ["build side < 10 MiB", HIT_COLOR], ["BroadcastHashJoin", TEAL]],
            text: "A join that started as sort-merge can switch to broadcast once runtime size is actually small.",
          },
          skew: {
            chips: [["stage complete", PURPLE], ["hot reducer", RED], ["AQEShuffleRead skewed", AMBER]],
            text: "A skewed reducer can be split only after Spark has real shuffle-size data for that stage.",
          },
        };
        btnGroup(container, [["coalesce", "Coalesce"], ["bhj", "Broadcast rewrite"], ["skew", "Skew split"]], render);
        function render(key) {
          stage.selectAll("*").remove();
          const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
          options[key].chips.forEach(([label, color]) => chip(row, label, { bg: `${color}1a`, fg: color, border: color }));
          stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(options[key].text);
        }
        render("coalesce");
      }

      function renderAqePlanToggle(el) {
        const container = d3.select(el);
        container.selectAll("*").remove();
        const stage = container.append("div");
        const plans = {
          initial: `AdaptiveSparkPlan isFinalPlan=false
+- SortMergeJoin
   :- Sort
   :  +- Exchange hashpartitioning(k, 8)
   +- Sort
      +- Exchange hashpartitioning(k, 8)`,
          final: `AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   +- BroadcastHashJoin
      :- AQEShuffleRead local
      +- BroadcastQueryStage
         +- BroadcastExchange
+- == Initial Plan ==
   +- SortMergeJoin`,
        };
        btnGroup(container, [["initial", "Before execution"], ["final", "After execution"]], render);
        function render(key) {
          stage.selectAll("*").remove();
          box(stage, plans[key], {
            bg: key === "final" ? "rgba(38,166,154,0.08)" : "rgba(124,77,255,0.08)",
            border: key === "final" ? HIT_COLOR : PURPLE,
          });
        }
        render("initial");
      }

      function renderCoalesce(el) {
        const container = d3.select(el);
        container.selectAll("*").remove();
        const stage = container.append("div");
        btnGroup(container, [["before", "Before AQE"], ["after", "After AQE"]], render);
        function render(mode) {
          stage.selectAll("*").remove();
          const values = mode === "before" ? [10, 14, 9, 11, 13, 12, 15, 10, 14, 12, 11, 13] : [46, 52, 56];
          scaleBars(stage, values, -1, PURPLE);
          stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
            .text(mode === "before"
              ? "Many tiny reducers each cost scheduler overhead."
              : "AQE reads adjacent tiny reducers together as fewer, larger tasks.");
        }
        render("before");
      }

      function renderBhj(el) {
        const container = d3.select(el);
        container.selectAll("*").remove();
        let size = 12;
        const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "8px");
        ctrl.append("span").style("font-size", "0.74rem").text("Runtime build side (MiB):");
        const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
        ctrl.append("input").attr("type", "range").attr("min", 1).attr("max", 20).attr("value", size).style("width", "180px")
          .on("input", function () { size = +this.value; update(); });
        const stage = container.append("div");
        function update() {
          stage.selectAll("*").remove();
          label.text(`${size} MiB`);
          const finalJoin = size <= 10 ? "BroadcastHashJoin" : "SortMergeJoin";
          const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
          chip(row, "Initial: SortMergeJoin", { bg: "rgba(124,77,255,0.08)", fg: PURPLE, border: PURPLE });
          chip(row, `Final: ${finalJoin}`, { bg: `${(size <= 10 ? HIT_COLOR : RED)}1a`, fg: size <= 10 ? HIT_COLOR : RED, border: size <= 10 ? HIT_COLOR : RED });
          stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
            .text(size <= 10
              ? "Below the adaptive broadcast threshold, AQE can replace the merge join after the shuffle stage completes."
              : "Above the runtime threshold, AQE keeps the sort-merge join.");
        }
        update();
      }

      function renderShj(el) {
        const container = d3.select(el);
        container.selectAll("*").remove();
        let partSize = 2.4;
        const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "8px");
        ctrl.append("span").style("font-size", "0.74rem").text("Per-partition build side (MiB):");
        const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
        ctrl.append("input").attr("type", "range").attr("min", 0).attr("max", 40).attr("value", partSize * 10).style("width", "180px")
          .on("input", function () { partSize = +this.value / 10; update(); });
        const stage = container.append("div");
        function update() {
          stage.selectAll("*").remove();
          label.text(`${partSize.toFixed(1)} MiB`);
          const finalJoin = partSize <= 1.0 ? "ShuffledHashJoin" : "SortMergeJoin";
          const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
          chip(row, "Broadcast disabled", { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER });
          chip(row, `Final: ${finalJoin}`, { bg: `${(partSize <= 1.0 ? TEAL : RED)}1a`, fg: partSize <= 1.0 ? TEAL : RED, border: partSize <= 1.0 ? TEAL : RED });
          stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
            .text(partSize <= 1.0
              ? "At or below the local-map threshold, AQE can keep the shuffle but drop the merge-sort work."
              : "Above the threshold, Spark keeps the sort-merge join.");
        }
        update();
      }

      function renderSkewJoin(el) {
        const container = d3.select(el);
        container.selectAll("*").remove();
        let hot = 260;
        const median = 80;
        const threshold = 128;
        const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "8px");
        ctrl.append("span").style("font-size", "0.74rem").text("Hot partition size (MiB):");
        const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
        ctrl.append("input").attr("type", "range").attr("min", 40).attr("max", 320).attr("value", hot).style("width", "180px")
          .on("input", function () { hot = +this.value; update(); });
        const stage = container.append("div");
        function update() {
          stage.selectAll("*").remove();
          label.text(`${hot} MiB`);
          scaleBars(stage, [80, 76, 82, hot, 78], 3, hot > Math.max(threshold, median * 2) ? RED : GRAY);
          const skewed = hot > threshold && hot > median * 2;
          const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "6px");
          chip(row, `median=${median} MiB`, { bg: "rgba(124,77,255,0.08)", fg: PURPLE, border: PURPLE });
          chip(row, `threshold=${threshold} MiB`, { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER });
          chip(row, skewed ? "Result: skewed" : "Result: not skewed", { bg: `${(skewed ? RED : HIT_COLOR)}1a`, fg: skewed ? RED : HIT_COLOR, border: skewed ? RED : HIT_COLOR });
        }
        update();
      }

      function renderSkewSplit(el) {
        const container = d3.select(el);
        container.selectAll("*").remove();
        const stage = container.append("div");
        btnGroup(container, [["before", "Before split"], ["after", "After split"]], render);
        function render(mode) {
          stage.selectAll("*").remove();
          if (mode === "before") {
            scaleBars(stage, [22, 24, 20, 100, 23], 3, RED);
            stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
              .text("One rebalance partition is too large relative to its neighbors.");
          } else {
            scaleBars(stage, [22, 24, 20, 34, 33, 33, 23], 3, TEAL);
            stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
              .text("AQE can split the oversized rebalance read while still coalescing tiny neighbors.");
          }
        }
        render("before");
      }

      function init() {
        const specs = [
          ["viz-aqe-overview", renderAqeOverview],
          ["viz-aqe-final-plan-toggle", renderAqePlanToggle],
          ["viz-aqe-coalesce", renderCoalesce],
          ["viz-aqe-smj-bhj", renderBhj],
          ["viz-aqe-smj-shj", renderShj],
          ["viz-aqe-skew-join", renderSkewJoin],
          ["viz-aqe-skew-split", renderSkewSplit],
        ];
        specs.forEach(([id, fn]) => {
          const elm = document.getElementById(id);
          if (elm && !elm.dataset.rendered) { fn(elm); elm.dataset.rendered = "1"; }
        });
      }
      if (typeof document$ !== "undefined") { document$.subscribe(() => requestAnimationFrame(init)); }
      else { document.addEventListener("DOMContentLoaded", init); }
    })();
