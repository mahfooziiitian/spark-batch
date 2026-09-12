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

  function renderCteOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const info = {
      single: {
        scope: "One statement",
        refs: ["WITH totals AS (...)", "SELECT * FROM totals"],
        note: "One reference is the cheapest and clearest CTE shape.",
        color: TEAL,
      },
      multi: {
        scope: "One statement",
        refs: ["WITH totals AS (...)", "SELECT * FROM totals t1", "JOIN totals t2 ON ..."],
        note: "Spark 4.2 did not guarantee compute-once reuse in the verified self-join plan.",
        color: AMBER,
      },
      recursive: {
        scope: "One statement",
        refs: ["WITH RECURSIVE nums AS (...)", "SELECT * FROM nums"],
        note: "Recursive CTEs remain statement-local and add recursion limits on top.",
        color: PURPLE,
      },
    };
    const group = btnGroup(container, [["single", "Single ref"], ["multi", "Multi ref"], ["recursive", "Recursive"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      info[mode].refs.forEach((ref, i) => {
        chip(row, ref, {
          bg: i === 0 ? "rgba(124,77,255,0.10)" : "rgba(38,166,154,0.10)",
          fg: i === 0 ? PURPLE : FG,
          border: i === 0 ? PURPLE : info[mode].color,
        });
      });
      chip(stage.append("div").style("margin-bottom", "6px"), `Scope: ${info[mode].scope}`, { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY });
      box(stage, info[mode].note, { bg: "#fff", border: info[mode].color });
    }
    render(group.get());
  }

  function renderChainedFlow(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const steps = [
      { name: "step1", text: "Read and filter source rows", color: TEAL },
      { name: "step2", text: "Aggregate results from step1", color: PURPLE },
      { name: "step3", text: "Rank or join prior steps", color: AMBER },
      { name: "final", text: "SELECT or DML consumes the last step", color: HIT_COLOR },
    ];
    const tabs = stage.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "8px").style("flex-wrap", "wrap");
    const detail = stage.append("div");
    let idx = 0;

    function render() {
      tabs.selectAll("*").remove();
      steps.forEach((s, i) => {
        chip(tabs, `${i + 1}. ${s.name}`, i === idx
          ? { bg: s.color, fg: "#fff", border: s.color }
          : { bg: "#f5f5f7", fg: FG, border: "#ccc" })
          .style("cursor", "pointer")
          .on("click", () => { idx = i; render(); });
      });
      detail.selectAll("*").remove();
      const flow = detail.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("align-items", "center").style("margin-bottom", "8px");
      steps.forEach((s, i) => {
        chip(flow, s.name, { bg: i <= idx ? `${s.color}22` : "#f5f5f7", fg: i <= idx ? s.color : FG, border: i <= idx ? s.color : "#ccc" });
        if (i < steps.length - 1) {
          flow.append("div").style("color", GRAY).style("font-family", "monospace").text("→");
        }
      });
      box(detail, `${steps[idx].name}: ${steps[idx].text}\n\nSpark resolves earlier CTEs first; forward references are not available.`, { bg: "#fff", border: steps[idx].color });
    }
    render();
  }

  function renderDmlSupport(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const data = {
      insert: {
        status: "Verified OK on managed Parquet table",
        flow: ["WITH prepared", "INSERT INTO target", "Rows written"],
        color: TEAL,
      },
      overwrite: {
        status: "Verified OK with INSERT OVERWRITE",
        flow: ["WITH prepared", "INSERT OVERWRITE target", "Target replaced"],
        color: PURPLE,
      },
      merge: {
        status: "CTE syntax accepted; execution needs row-level table support",
        flow: ["WITH deduped_source", "MERGE INTO target", "Provider decides support"],
        color: AMBER,
      },
      update: {
        status: "CTE syntax accepted; execution needs row-level table support",
        flow: ["WITH corrections", "UPDATE target", "Provider decides support"],
        color: RED,
      },
      delete: {
        status: "CTE syntax accepted; execution needs row-level table support",
        flow: ["WITH doomed", "DELETE FROM target", "Provider decides support"],
        color: GRAY,
      },
    };
    const group = btnGroup(container, [["insert", "INSERT"], ["overwrite", "OVERWRITE"], ["merge", "MERGE"], ["update", "UPDATE"], ["delete", "DELETE"]], render);

    function render(mode) {
      const d = data[mode];
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("align-items", "center").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      d.flow.forEach((text, i) => {
        chip(row, text, { bg: `${d.color}22`, fg: d.color === GRAY ? FG : d.color, border: d.color });
        if (i < d.flow.length - 1) row.append("div").style("color", GRAY).text("→");
      });
      box(stage, d.status, { bg: "#fff", border: d.color });
    }
    render(group.get());
  }

  function renderCteVsView(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      cte: {
        chips: ["WITH active_customers AS (...)", "SELECT ..."],
        note: "Scope ends with the statement. Verified plan stayed inline.",
        color: PURPLE,
      },
      view: {
        chips: ["CREATE TEMP VIEW active_customers AS ...", "SELECT #1", "SELECT #2"],
        note: "Session-scoped reuse, but still not materialized by default.",
        color: AMBER,
      },
      cache: {
        chips: ["CREATE TEMP VIEW ...", "CACHE TABLE active_customers", "InMemoryRelation"],
        note: "After cache population, Spark 4.2 EXPLAIN showed in-memory reuse.",
        color: TEAL,
      },
    };
    const group = btnGroup(container, [["cte", "CTE"], ["view", "Temp View"], ["cache", "Cached View"]], render);

    function render(mode) {
      const d = modes[mode];
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      d.chips.forEach((text, i) => chip(row, text, {
        bg: i === d.chips.length - 1 && mode === "cache" ? "rgba(38,166,154,0.12)" : "#f5f5f7",
        fg: i === d.chips.length - 1 && mode === "cache" ? HIT_COLOR : FG,
        border: i === 0 ? PURPLE : d.color,
      }));
      box(stage, d.note, { bg: "#fff", border: d.color });
    }
    render(group.get());
  }

  function renderDedupSteps(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      where: {
        title: "CTE + WHERE rn = 1",
        output: ["A | ts=2024-01-03 | rn=1", "B | ts=2024-01-01 | rn=1"],
        color: PURPLE,
      },
      qualify: {
        title: "QUALIFY ROW_NUMBER() = 1",
        output: ["A | ts=2024-01-03", "B | ts=2024-01-01"],
        color: TEAL,
      },
    };
    const group = btnGroup(container, [["where", "WHERE rn = 1"], ["qualify", "QUALIFY"]], render);

    function render(mode) {
      const d = modes[mode];
      stage.selectAll("*").remove();
      const ranked = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      ["A | ts=2024-01-03 | rn=1", "A | ts=2024-01-02 | rn=2", "B | ts=2024-01-01 | rn=1"].forEach((text, i) => chip(ranked, text, {
        bg: i !== 1 ? `${d.color}22` : "#f5f5f7",
        fg: i !== 1 ? d.color : FG,
        border: i !== 1 ? d.color : "#ccc",
      }));
      const filtered = stage.append("div").style("margin-bottom", "8px");
      chip(filtered, d.title, { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY });
      const out = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      d.output.forEach((text) => chip(out, text, { bg: `${d.color}22`, fg: d.color, border: d.color }));
      box(stage, "Use a CTE when you want to inspect or reuse ranked rows; use QUALIFY for the shortest inline form.", { bg: "#fff", border: d.color });
    }
    render(group.get());
  }

  function renderPivotReshape(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      pivot: {
        left: ["EU | 2024-01 | 10", "EU | 2024-02 | 20", "US | 2024-01 | 30"],
        right: ["EU | jan=10 | feb=20", "US | jan=30 | feb=NULL"],
        color: PURPLE,
        note: "Pivot fixes output columns up front.",
      },
      unpivot: {
        left: ["Sales | jan=50k | feb=48k | mar=52k"],
        right: ["Sales | January | 50k", "Sales | February | 48k", "Sales | March | 52k"],
        color: TEAL,
        note: "Unpivot expands columns back into rows.",
      },
    };
    const group = btnGroup(container, [["pivot", "PIVOT"], ["unpivot", "UNPIVOT"]], render);

    function render(mode) {
      const d = modes[mode];
      stage.selectAll("*").remove();
      const layout = stage.append("div").style("display", "grid").style("grid-template-columns", "1fr auto 1fr").style("gap", "10px").style("align-items", "start").style("margin-bottom", "8px");
      const left = layout.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px");
      d.left.forEach((text) => chip(left, text, { bg: "#f5f5f7", fg: FG, border: "#ccc" }));
      layout.append("div").style("align-self", "center").style("color", GRAY).style("font-family", "monospace").text("→");
      const right = layout.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px");
      d.right.forEach((text) => chip(right, text, { bg: `${d.color}22`, fg: d.color, border: d.color }));
      box(stage, d.note, { bg: "#fff", border: d.color });
    }
    render(group.get());
  }

  function renderRecursiveSteps(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const frames = [
      { label: "Anchor", rows: ["n = 1"], note: "The anchor member runs once.", color: PURPLE },
      { label: "Iteration 1", rows: ["n = 1", "n = 2"], note: "Recursive member consumes prior output.", color: TEAL },
      { label: "Iteration 2", rows: ["n = 1", "n = 2", "n = 3"], note: "More rows appear while the predicate is still true.", color: AMBER },
      { label: "Stop", rows: ["n = 1", "n = 2", "n = 3", "n = 4", "n = 5"], note: "Recursion stops when no new rows are produced or a limit is reached.", color: HIT_COLOR },
    ];
    const tabs = stage.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "8px").style("flex-wrap", "wrap");
    const detail = stage.append("div");
    let idx = 0;

    function render() {
      tabs.selectAll("*").remove();
      frames.forEach((f, i) => {
        chip(tabs, f.label, i === idx
          ? { bg: f.color, fg: "#fff", border: f.color }
          : { bg: "#f5f5f7", fg: FG, border: "#ccc" })
          .style("cursor", "pointer")
          .on("click", () => { idx = i; render(); });
      });
      detail.selectAll("*").remove();
      const rows = detail.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      frames[idx].rows.forEach((text) => chip(rows, text, { bg: `${frames[idx].color}22`, fg: frames[idx].color, border: frames[idx].color }));
      chip(detail.append("div").style("margin-bottom", "8px"), "Defaults: level limit = 100, row limit = 1000000", { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY });
      box(detail, `${frames[idx].note}\n\nSpark 4.2 also accepts MAX RECURSION LEVEL n per query.`, { bg: "#fff", border: frames[idx].color });
    }
    render();
  }

  function init() {
    const specs = [
      ["viz-cte-overview", renderCteOverview],
      ["viz-cte-chained-flow", renderChainedFlow],
      ["viz-cte-dml-support", renderDmlSupport],
      ["viz-cte-vs-view", renderCteVsView],
      ["viz-cte-dedup-steps", renderDedupSteps],
      ["viz-cte-pivot-reshape", renderPivotReshape],
      ["viz-cte-recursive-steps", renderRecursiveSteps],
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
