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

  function renderFilterIndex(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const flows = {
      where: { title: "WHERE", text: "FROM -> WHERE -> GROUP BY -> HAVING -> SELECT -> WINDOW -> QUALIFY -> ORDER BY -> LIMIT", hit: 1 },
      having: { title: "HAVING", text: "FROM -> WHERE -> GROUP BY -> HAVING -> SELECT -> WINDOW -> QUALIFY -> ORDER BY -> LIMIT", hit: 3 },
      filter: { title: "FILTER", text: "FROM -> WHERE -> GROUP BY -> HAVING -> SELECT[FILTER scopes one aggregate] -> WINDOW -> QUALIFY", hit: 4 },
      qualify: { title: "QUALIFY", text: "FROM -> WHERE -> GROUP BY -> HAVING -> SELECT -> WINDOW -> QUALIFY -> ORDER BY -> LIMIT", hit: 6 },
    };
    const steps = ["FROM", "WHERE", "GROUP BY", "HAVING", "SELECT", "WINDOW", "QUALIFY", "ORDER BY", "LIMIT"];
    const group = btnGroup(container, [["where", "WHERE"], ["having", "HAVING"], ["filter", "FILTER"], ["qualify", "QUALIFY"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      steps.forEach((s, i) => chip(row, s, i === flows[mode].hit
        ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: i > flows[mode].hit ? "rgba(124,77,255,0.08)" : "#f5f5f7", fg: FG, border: "#ddd" }));
      box(stage, flows[mode].text);
    }
    render(group.get());
  }

  function renderCaseWhen(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const regionCtrl = container.append("div");
    const amountCtrl = container.append("div");
    let region = "EU";
    let amount = 800;
    const regionGroup = btnGroup(regionCtrl, [["US", "region = 'US'"], ["EU", "region = 'EU'"], ["APAC", "region = 'APAC'"], ["NULL", "region = NULL-ish else"]], (v) => { region = v; render(); });
    const amountGroup = btnGroup(amountCtrl, [["450", "amount 450"], ["800", "amount 800"], ["950", "amount 950"], ["1500", "amount 1500"]], (v) => { amount = +v; render(); });

    function render() {
      stage.selectAll("*").remove();
      region = regionGroup.get();
      amount = +amountGroup.get();
      const pass = region === "US" ? amount > 1000 : region === "EU" ? amount > 700 : amount > 500;
      const branch = region === "US" ? "WHEN region = 'US' THEN amount > 1000" : region === "EU" ? "WHEN region = 'EU' THEN amount > 700" : "ELSE amount > 500";
      chip(stage, pass ? "row kept" : "row filtered out", pass
        ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      stage.append("div").style("margin", "8px 0");
      box(stage, `${branch}\n=> ${pass ? "TRUE" : "FALSE"}`);
    }
    render();
  }

  function renderNullFilter(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const rows = [
      { id: 1, name: "Alice", region: "US" },
      { id: 2, name: "Bob", region: null },
      { id: 3, name: "Carol", region: "EU" },
      { id: 4, name: "Dave", region: null },
    ];
    const group = btnGroup(container, [["eq", "region = NULL"], ["nseq", "region <=> NULL"], ["isnull", "region IS NULL"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px");
      rows.forEach((r) => {
        let status = "UNKNOWN";
        let color = AMBER;
        if (mode === "eq") {
          status = "UNKNOWN";
        } else if (mode === "nseq") {
          status = r.region === null ? "TRUE" : "FALSE";
          color = r.region === null ? HIT_COLOR : RED;
        } else {
          status = r.region === null ? "TRUE" : "FALSE";
          color = r.region === null ? HIT_COLOR : RED;
        }
        const line = row.append("div").style("display", "flex").style("gap", "8px").style("align-items", "center");
        chip(line, `${r.name}: ${r.region === null ? "NULL" : r.region}`);
        chip(line, status, status === "TRUE"
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : status === "FALSE"
            ? { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED }
            : { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      });
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "4px")
        .text(mode === "eq" ? "WHERE keeps only TRUE, so UNKNOWN rows are discarded too." : "NULL-safe operators convert the ambiguous case into TRUE or FALSE.");
    }
    render(group.get());
  }

  function renderPushdown(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const cases = {
      bare: { sql: "region = 'US' AND amount > 500", pushed: ["EqualTo(region,US)", "GreaterThan(amount,500.0)"], note: "Both comparisons push into the Parquet scan." },
      expr: { sql: "amount * 1.1 > 1000", pushed: ["IsNotNull(amount)"], note: "The arithmetic comparison stays in Spark; only the NULL check pushes." },
      udf: { sql: "is_us(region)", pushed: [], note: "Opaque UDF logic blocks predicate pushdown." },
      struct: { sql: "profile.tier = 'gold'", pushed: ["EqualTo(profile.tier,gold)"], note: "Nested struct-field predicates can still push." },
    };
    const group = btnGroup(container, [["bare", "Bare columns"], ["expr", "Wrapped expression"], ["udf", "UDF"], ["struct", "Struct field"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      box(stage, cases[key].sql);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0");
      if (cases[key].pushed.length) {
        cases[key].pushed.forEach((p) => chip(row, p, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      } else {
        chip(row, "PushedFilters: []", { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      }
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).text(cases[key].note);
    }
    render(group.get());
  }

  function renderQualify(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const data = [
      { customer: 101, order: 6, amount: 1500, rank: 1 },
      { customer: 101, order: 1, amount: 1200, rank: 2 },
      { customer: 101, order: 3, amount: 450, rank: 3 },
      { customer: 102, order: 8, amount: 1100, rank: 1 },
      { customer: 102, order: 5, amount: 600, rank: 2 },
      { customer: 102, order: 2, amount: 800, rank: 3 },
    ];
    const group = btnGroup(container, [["1", "QUALIFY rank <= 1"], ["2", "QUALIFY rank <= 2"]], render);

    function render(limit) {
      stage.selectAll("*").remove();
      const lim = +limit;
      box(stage, `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn\nQUALIFY rn <= ${lim}`);
      const rows = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px").style("margin-top", "8px");
      data.forEach((d) => {
        const keep = d.rank <= lim;
        const line = rows.append("div").style("display", "flex").style("gap", "8px").style("align-items", "center");
        chip(line, `cust ${d.customer} / order ${d.order} / rn ${d.rank}`);
        chip(line, keep ? "kept by QUALIFY" : "filtered after WINDOW", keep
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      });
    }
    render(group.get());
  }

  function renderSubQuery(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const datasets = {
      in: { sql: "customer_id IN (SELECT id FROM customers WHERE country = 'US')", ids: [101, 103, 104, 108], note: "Spark 4.2 planned this as a left semi-join." },
      exists: { sql: "EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)", ids: [101, 102, 103, 104, 105, 106, 108], note: "Simple EXISTS also became a left semi-join." },
      notexists: { sql: "NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)", ids: [5], note: "NOT EXISTS became a left anti-join and avoids the NULL trap." },
    };
    const group = btnGroup(container, [["in", "IN"], ["exists", "EXISTS"], ["notexists", "NOT EXISTS"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, datasets[mode].sql);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0");
      datasets[mode].ids.forEach((id) => chip(row, String(id), { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).text(datasets[mode].note);
    }
    render(group.get());
  }

  function renderTableSample(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const samples = {
      percent: { chips: ["Bernoulli", "approximate row count", "changes run-to-run"], colors: [AMBER, AMBER, RED], sql: "TABLESAMPLE (50 PERCENT)" },
      seeded: { chips: ["Bernoulli", "approximate row count", "repeatable with seed"], colors: [TEAL, AMBER, HIT_COLOR], sql: "TABLESAMPLE (50 PERCENT) REPEATABLE (42)" },
      rows: { chips: ["exact row count", "limit-like plan", "not random by itself"], colors: [HIT_COLOR, PURPLE, AMBER], sql: "TABLESAMPLE (3 ROWS)" },
      bucket: { chips: ["fraction m/n", "Bernoulli in Spark 4.2", "ON col unsupported"], colors: [AMBER, RED, RED], sql: "TABLESAMPLE (BUCKET 1 OUT OF 2)" },
    };
    const group = btnGroup(container, [["percent", "PERCENT"], ["seeded", "PERCENT + seed"], ["rows", "ROWS"], ["bucket", "BUCKET"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, samples[mode].sql);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0");
      samples[mode].chips.forEach((text, i) => chip(row, text, {
        bg: `${samples[mode].colors[i]}1a`, fg: samples[mode].colors[i], border: samples[mode].colors[i],
      }));
    }
    render(group.get());
  }

  function renderAggFilter(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const views = {
      shipped: { sql: "SUM(amount) FILTER (WHERE status = 'shipped')", vals: [["US", "1650"], ["EU", "600"], ["APAC", "300"]] },
      pending: { sql: "COUNT(*) FILTER (WHERE status = 'pending')", vals: [["US", "1"], ["EU", "1"], ["APAC", "0"]] },
      gt1000: { sql: "COUNT(*) FILTER (WHERE amount > 1000)", vals: [["US", "2"], ["EU", "1"], ["APAC", "0"]] },
    };
    const group = btnGroup(container, [["shipped", "Shipped sum"], ["pending", "Pending count"], ["gt1000", "Count > 1000"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, views[mode].sql);
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-top", "8px");
      views[mode].vals.forEach(([region, val]) => chip(row, `${region}: ${val}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px")
        .text("Each aggregate can carry its own WHERE clause inside the same grouped SELECT.");
    }
    render(group.get());
  }

  function renderArray(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      contains: [
        [1, "TRUE"], [2, "FALSE"], [3, "TRUE"], [4, "FALSE"], [5, "NULL"], [6, "TRUE"], [7, "FALSE"],
      ],
      exists: [
        [1, "TRUE"], [2, "TRUE"], [3, "FALSE"], [4, "FALSE"], [5, "NULL"], [6, "FALSE"], [7, "FALSE"],
      ],
      filter: [
        [1, "[priority, alert]"], [2, "[]"], [3, "[priority]"], [4, "[alert]"], [5, "NULL"], [6, "[priority]"], [7, "[]"],
      ],
      forall: [
        [1, "TRUE"], [2, "FALSE"], [3, "TRUE"], [4, "FALSE"], [5, "TRUE"], [6, "NULL"], [7, "TRUE"],
      ],
    };
    const group = btnGroup(container, [["contains", "array_contains"], ["exists", "exists"], ["filter", "filter + size"], ["forall", "forall"]], render);

    function styleFor(value) {
      if (value === "TRUE" || (typeof value === "string" && value.startsWith("[") && value !== "[]")) return { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR };
      if (value === "FALSE" || value === "[]") return { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED };
      return { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER };
    }

    function render(mode) {
      stage.selectAll("*").remove();
      const rows = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "6px");
      modes[mode].forEach(([id, value]) => {
        const line = rows.append("div").style("display", "flex").style("gap", "8px").style("align-items", "center");
        chip(line, `event ${id}`);
        chip(line, value, styleFor(value));
      });
    }
    render(group.get());
  }

  function renderLateralView(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const details = {
      explode: { rows: 8, kept: [1, 2, 3, 4], dropped: [5, 6], note: "NULL and empty arrays emit zero rows." },
      outer: { rows: 10, kept: [1, 2, 3, 4, 5, 6], dropped: [], note: "NULL and empty arrays each emit one NULL row." },
    };
    const group = btnGroup(container, [["explode", "explode"], ["outer", "explode_outer"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, `${details[mode].rows} output rows`, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      const keepRow = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0 4px");
      details[mode].kept.forEach((id) => chip(keepRow, `event ${id}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      if (details[mode].dropped.length) {
        const dropRow = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap");
        details[mode].dropped.forEach((id) => chip(dropRow, `event ${id} dropped`, { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED }));
      }
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px").text(details[mode].note);
    }
    render(group.get());
  }

  function renderMap(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const views = {
      element: { sql: "element_at(attributes, 'priority') = 'high'", ids: [1, 5], note: "Missing keys yield NULL, so the predicate becomes UNKNOWN." },
      haskey: { sql: "map_contains_key(attributes, 'promo')", ids: [1, 3, 6], note: "A NULL map returns NULL rather than FALSE." },
      filter: { sql: "size(map_filter(attributes, (k, v) -> k = 'priority' AND v = 'high')) > 0", ids: [1, 5], note: "map_filter returns a map; size(...) turns it into a row predicate." },
    };
    const group = btnGroup(container, [["element", "element_at"], ["haskey", "map_contains_key"], ["filter", "map_filter"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, views[mode].sql);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0");
      views[mode].ids.forEach((id) => chip(row, `order ${id}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).text(views[mode].note);
    }
    render(group.get());
  }

  function renderStruct(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      tier: { sql: "profile.tier = 'gold'", ids: [1, 4], note: "Named struct fields are addressable with dot notation." },
      region: { sql: "profile.region = 'US' AND profile.score > 50", ids: [1], note: "Multiple nested-field predicates combine like ordinary scalar filters." },
      score: { sql: "profile IS NOT NULL AND profile.score < 50", ids: [3], note: "Guarding the struct avoids NULL propagation surprises." },
    };
    const group = btnGroup(container, [["tier", "Tier"], ["region", "Region + score"], ["score", "NULL guard"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, modes[mode].sql);
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0");
      modes[mode].ids.forEach((id) => chip(row, `id ${id}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
      chip(row, "id 5 -> NULL struct", { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-filter-index", renderFilterIndex],
      ["viz-filter-case-when", renderCaseWhen],
      ["viz-filter-null-filter", renderNullFilter],
      ["viz-filter-pp", renderPushdown],
      ["viz-filter-qualify", renderQualify],
      ["viz-filter-sub-query", renderSubQuery],
      ["viz-filter-tablesample", renderTableSample],
      ["viz-filter-agg-filter", renderAggFilter],
      ["viz-filter-array", renderArray],
      ["viz-filter-lateral-view", renderLateralView],
      ["viz-filter-map", renderMap],
      ["viz-filter-struct", renderStruct],
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
