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

  function stageCard(parent, label, lines, color) {
    const card = parent.append("div")
      .style("flex", "1 1 120px")
      .style("min-width", "120px")
      .style("border", `1.5px solid ${color}`)
      .style("border-radius", "8px")
      .style("padding", "10px")
      .style("background", "rgba(255,255,255,0.9)");
    card.append("div")
      .style("font-size", "0.78rem")
      .style("font-weight", "700")
      .style("color", color)
      .style("margin-bottom", "6px")
      .text(label);
    lines.forEach((line) => {
      card.append("div")
        .style("font-size", "0.72rem")
        .style("color", FG)
        .style("font-family", "monospace")
        .text(line);
    });
    return card;
  }

  function arrow(parent) {
    parent.append("div")
      .style("align-self", "center")
      .style("color", GRAY)
      .style("font-size", "1rem")
      .style("padding", "0 2px")
      .text("→");
  }

  function renderHavingOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const data = {
      where: {
        stages: [
          ["Scan", ["6 rows read"], GRAY],
          ["WHERE", ["amount >= 20", "3 rows kept"], TEAL],
          ["Aggregate", ["2 regions built"], PURPLE],
          ["Project", ["2 result rows"], HIT_COLOR],
        ],
        note: "The row predicate runs before grouping, so Spark aggregates only the surviving rows."
      },
      having: {
        stages: [
          ["Scan", ["6 rows read"], GRAY],
          ["Aggregate", ["3 regions built"], PURPLE],
          ["HAVING", ["SUM(amount) >= 20", "2 groups kept"], AMBER],
          ["Project", ["2 result rows"], HIT_COLOR],
        ],
        note: "The group predicate runs after aggregation, so Spark first computes all groups, then filters them."
      }
    };
    const group = btnGroup(container, [["where", "Predicate in WHERE"], ["having", "Predicate in HAVING"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("align-items", "stretch").style("margin-bottom", "8px");
      data[mode].stages.forEach((spec, idx) => {
        stageCard(row, spec[0], spec[1], spec[2]);
        if (idx < data[mode].stages.length - 1) arrow(row);
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(data[mode].note);
    }
    render(group.get());
  }

  function renderWhereVsHaving(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const plans = {
      where: {
        chips: [
          ["Rows after filter: 5", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }],
          ["Groups built: 2", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
        ],
        plan: "Aggregate [_groupingexpression#4L], [_groupingexpression#4L AS bucket#0L, count(1) AS cnt#1L]\n+- Project [(id#2L % 2) AS _groupingexpression#4L]\n   +- Filter (id#2L >= 5)\n      +- Range (0, 10, step=1)"
      },
      having: {
        chips: [
          ["Rows entering aggregate: 10", { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY }],
          ["Groups built: 2", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
          ["Groups after HAVING: 2", { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }],
        ],
        plan: "Filter (cnt#8L >= 5)\n+- Aggregate [_groupingexpression#12L], [_groupingexpression#12L AS bucket#7L, count(1) AS cnt#8L]\n   +- Project [(id#9L % 2) AS _groupingexpression#12L]\n      +- Range (0, 10, step=1)"
      }
    };
    const group = btnGroup(container, [["where", "WHERE plan"], ["having", "HAVING plan"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const chipRow = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      plans[mode].chips.forEach(([text, opts]) => chip(chipRow, text, opts));
      box(stage, plans[mode].plan, { bg: "#faf8ff", border: mode === "having" ? AMBER : TEAL });
    }
    render(group.get());
  }

  function renderHavingFilter(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const aggregates = {
      shipped: {
        headline: "SUM(amount) FILTER (WHERE status = 'shipped')",
        values: [["APAC", "120"], ["EMEA", "40"], ["LATAM", "0"]],
        note: "Only rows whose status is shipped contribute to this aggregate."
      },
      returned: {
        headline: "SUM(amount) FILTER (WHERE status = 'returned')",
        values: [["APAC", "15"], ["EMEA", "25"], ["LATAM", "10"]],
        note: "A different FILTER sees the same grouped rows but keeps a different subset."
      },
      total: {
        headline: "SUM(amount)",
        values: [["APAC", "135"], ["EMEA", "65"], ["LATAM", "10"]],
        note: "The unfiltered aggregate reads every row in the group."
      }
    };
    const group = btnGroup(container, [["shipped", "Shipped only"], ["returned", "Returned only"], ["total", "All rows"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, aggregates[mode].headline, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE })
        .style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      aggregates[mode].values.forEach(([groupKey, value]) => {
        stageCard(row, groupKey, [`aggregate = ${value}`], mode === "total" ? TEAL : AMBER);
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(aggregates[mode].note);
    }
    render(group.get());
  }

  function renderHavingAdvanced(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      group: {
        title: "Regular GROUP BY",
        cards: [
          ["Groups produced", ["region=A", "region=B", "region=C"], PURPLE],
          ["HAVING", ["keep SUM(amount) > 100"], AMBER],
        ],
        footer: "Ordinary HAVING works on the per-group rows produced by GROUP BY."
      },
      rollup: {
        title: "ROLLUP(region, product)",
        cards: [
          ["Detail rows", ["(region, product)"], PURPLE],
          ["Subtotals", ["(region)", "()"], TEAL],
          ["HAVING", ["drop small subtotals too"], AMBER],
        ],
        footer: "Subtotal and grand-total rows are still ordinary grouped rows from HAVING's perspective."
      },
      single: {
        title: "No GROUP BY",
        cards: [
          ["Implicit group", ["all rows -> 1 aggregate row"], PURPLE],
          ["Allowed select list", ["COUNT(*)", "AVG(amount)"], TEAL],
          ["Rejected raw cols", ["amount -> MISSING_GROUP_BY"], RED],
        ],
        footer: "Spark 4.2 permits single-group HAVING only when the projection is aggregate-safe."
      }
    };
    const group = btnGroup(container, [["group", "GROUP BY"], ["rollup", "ROLLUP"], ["single", "Single group"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, modes[mode].title, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE })
        .style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      modes[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].footer);
    }
    render(group.get());
  }

  function renderHavingPatterns(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let threshold = 30;
    const totals = [
      { region: "LATAM", total: 40 },
      { region: "APAC", total: 30 },
      { region: "EMEA", total: 5 },
    ];
    const avg = 25;

    const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "8px").style("flex-wrap", "wrap");
    ctrl.append("span").style("font-size", "0.74rem").text("HAVING SUM(amount) >");
    const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    ctrl.append("input").attr("type", "range").attr("min", 5).attr("max", 45).attr("step", 5).attr("value", threshold).style("width", "180px")
      .on("input", function () { threshold = +this.value; update(); });

    const stage = container.append("div");

    function update() {
      label.text(threshold);
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      totals.forEach((item) => {
        const keep = item.total > threshold;
        stageCard(row, item.region, [`SUM(amount) = ${item.total}`, keep ? "kept by HAVING" : "filtered out"], keep ? HIT_COLOR : RED);
      });
      const chips = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(chips, `Literal threshold = ${threshold}`, { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      chip(chips, `Scalar subquery avg = ${avg}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      box(stage, "HAVING SUM(amount) > (\n  SELECT AVG(region_total)\n  FROM (\n    SELECT region, SUM(amount) AS region_total\n    FROM orders\n    GROUP BY region\n  ) t\n)", { bg: "#faf8ff", border: PURPLE });
    }
    update();
  }

  function init() {
    const specs = [
      ["viz-having-overview", renderHavingOverview],
      ["viz-having-vs-where", renderWhereVsHaving],
      ["viz-having-filter", renderHavingFilter],
      ["viz-having-advanced", renderHavingAdvanced],
      ["viz-having-patterns", renderHavingPatterns],
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
