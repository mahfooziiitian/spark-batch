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

  function renderConfigOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const states = {
      cluster: {
        command: `SET spark.sql.shuffle.partitions;
-- effective value = 200`,
        cards: [
          ["Cluster", ["shuffle.partitions = 200"], PURPLE],
          ["Session A", ["inherits cluster", "effective = 200"], TEAL],
          ["Session B", ["inherits cluster", "effective = 200"], GRAY],
        ],
        note: "With no session override, every session sees the cluster or built-in value."
      },
      session: {
        command: `SET spark.sql.shuffle.partitions = 50;
SET spark.sql.shuffle.partitions;
-- Session A sees 50`,
        cards: [
          ["Cluster", ["shuffle.partitions = 200"], PURPLE],
          ["Session A", ["SET override = 50", "effective = 50"], HIT_COLOR],
          ["Session B", ["still inherits 200"], GRAY],
        ],
        note: "A SQL SET changes only the current SparkSession; sibling sessions are untouched."
      },
      reset: {
        command: `RESET spark.sql.shuffle.partitions;
SET spark.sql.shuffle.partitions;
-- Session A back to 200`,
        cards: [
          ["Cluster", ["shuffle.partitions = 200"], PURPLE],
          ["Session A", ["override removed", "effective = 200"], AMBER],
          ["Session B", ["still 200"], GRAY],
        ],
        note: "RESET removes the session layer and exposes the lower-precedence value again."
      }
    };
    const group = btnGroup(container, [["cluster", "Cluster default"], ["session", "Session SET override"], ["reset", "RESET key"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      states[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      box(stage, states[mode].command, { bg: "#faf8ff", border: mode === "session" ? HIT_COLOR : PURPLE })
        .style("margin-bottom", "8px");
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(states[mode].note);
    }
    render(group.get());
  }

  function renderAnsi(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      on: {
        title: "ANSI mode = true",
        cards: [
          ["2147483647 + 1", ["ARITHMETIC_OVERFLOW", "query fails"], RED],
          ["CAST('abc' AS INT)", ["Number format error", "query fails"], RED],
          ["try_cast('abc' AS INT)", ["NULL", "query continues"], HIT_COLOR],
        ],
        sql: `SET spark.sql.ansi.enabled = true;
SELECT 2147483647 + 1;
SELECT CAST('abc' AS INT);`,
        note: "Spark 4 defaults to ANSI mode, so overflow and malformed casts raise explicit errors."
      },
      off: {
        title: "ANSI mode = false",
        cards: [
          ["2147483647 + 1", ["wrapped integer", "legacy permissive behavior"], AMBER],
          ["CAST('abc' AS INT)", ["NULL", "query continues"], AMBER],
          ["try_cast('abc' AS INT)", ["NULL", "same result here"], HIT_COLOR],
        ],
        sql: `SET spark.sql.ansi.enabled = false;
SELECT 2147483647 + 1;
SELECT CAST('abc' AS INT);`,
        note: "Disabling ANSI restores the older NULL-or-wrap behavior for many invalid operations."
      },
      safe: {
        title: "ANSI mode on + try_*",
        cards: [
          ["try_add(2147483647, 1)", ["NULL", "local opt-out from overflow error"], HIT_COLOR],
          ["try_cast('abc' AS INT)", ["NULL", "bad row isolated"], HIT_COLOR],
          ["session default", ["ANSI stays true", "other errors still surface"], PURPLE],
        ],
        sql: `SET spark.sql.ansi.enabled = true;
SELECT try_add(2147483647, 1);
SELECT try_cast('abc' AS INT);`,
        note: "Use try_* when only a specific expression should become NULL-on-error."
      }
    };
    const group = btnGroup(container, [["on", "ANSI on"], ["off", "ANSI off"], ["safe", "Use try_* safely"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, modes[mode].title, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      modes[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      box(stage, modes[mode].sql, { bg: "#faf8ff", border: mode === "off" ? AMBER : PURPLE }).style("margin-bottom", "8px");
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function renderAqe(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      off: {
        chips: [
          ["AQE disabled", { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED }],
          ["shuffle partitions fixed = 200", { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY }],
        ],
        plan: `SortMergeJoin
+- Exchange hashpartitioning(customer_id, 200)
+- Exchange hashpartitioning(customer_id, 200)`,
        note: "Without AQE, Spark keeps the compile-time shuffle count and join strategy even if runtime sizes differ."
      },
      coalesce: {
        chips: [
          ["initial partitions = 200", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
          ["runtime output = 32 partitions", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }],
        ],
        plan: `AdaptiveSparkPlan
+- CustomShuffleReader (coalesced)
   +- Exchange hashpartitioning(region, 200)`,
        note: "AQE merges many tiny shuffle outputs into fewer tasks closer to the advisory partition size."
      },
      broadcast: {
        chips: [
          ["initial plan = SortMergeJoin", { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }],
          ["post-filter build side = 7 MB", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }],
          ["threshold = 10 MB", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
        ],
        plan: `AdaptiveSparkPlan
+- BroadcastHashJoin
   :- Exchange hashpartitioning(order_id, 200)
   +- BroadcastExchange HashedRelationBroadcastMode(...)`,
        note: "After filters shrink a side below the broadcast threshold, AQE can replace the merge join with a broadcast join."
      },
      skew: {
        chips: [
          ["median partition = 64 MB", { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY }],
          ["partition 17 = 640 MB", { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED }],
          ["AQE splits hot partition", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }],
        ],
        plan: `AdaptiveSparkPlan
+- SortMergeJoin (isSkewJoin = true)
   +- AQEShuffleRead skewed partition split into subpartitions`,
        note: "A partition must exceed both the skew factor and the absolute skew threshold before AQE splits it."
      },
      dpp: {
        chips: [
          ["dim_date filter = FY2024 / Q1", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
          ["fact partitions scanned = 14 / 365", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }],
        ],
        plan: `AdaptiveSparkPlan
+- FileScan fact_sales ... PartitionFilters: [dynamicpruningexpression(sale_date#...)]
+- BroadcastExchange(dim_date filter result)`,
        note: "Dynamic partition pruning turns the selective dimension filter into a runtime partition filter on the fact scan."
      }
    };
    const group = btnGroup(container, [["off", "AQE off"], ["coalesce", "Coalesce"], ["broadcast", "Broadcast switch"], ["skew", "Skew split"], ["dpp", "DPP"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const chipRow = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      modes[mode].chips.forEach(([text, opts]) => chip(chipRow, text, opts));
      box(stage, modes[mode].plan, { bg: "#faf8ff", border: mode === "off" ? RED : PURPLE }).style("margin-bottom", "8px");
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function renderIo(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      snappy: {
        title: "Parquet codec = snappy",
        cards: [
          ["Write speed", ["fast", "good default for interactive data"], HIT_COLOR],
          ["Compressed size", ["medium", "balanced CPU vs storage"], PURPLE],
          ["Best fit", ["frequent reads", "general-purpose tables"], TEAL],
        ],
        sql: "SET spark.sql.parquet.compression.codec = snappy;",
        note: "Snappy stays the safest default when the same dataset is written and queried often."
      },
      zstd: {
        title: "Parquet codec = zstd",
        cards: [
          ["Write speed", ["moderate", "more CPU than snappy"], AMBER],
          ["Compressed size", ["smaller files", "better for colder data"], HIT_COLOR],
          ["Best fit", ["archive or lower-cost storage", "still practical to read"], TEAL],
        ],
        sql: "SET spark.sql.parquet.compression.codec = zstd;",
        note: "Zstandard trades extra CPU for noticeably smaller files, which is often worth it off the hot path."
      },
      gzip: {
        title: "Parquet codec = gzip",
        cards: [
          ["Write speed", ["slowest", "highest CPU cost here"], RED],
          ["Compressed size", ["smallest files", "good when write speed matters least"], AMBER],
          ["Best fit", ["rarely rewritten archival data", "not default interactive storage"], TEAL],
        ],
        sql: "SET spark.sql.parquet.compression.codec = gzip;",
        note: "Gzip usually wins on ratio but loses on write latency, so reserve it for slower-moving data."
      },
      merge: {
        title: "spark.sql.parquet.mergeSchema = true",
        cards: [
          ["Scan startup", ["reads schema metadata from many files", "extra planning overhead"], RED],
          ["Result schema", ["merged across file versions", "helpful for debugging evolution"], HIT_COLOR],
          ["Best fit", ["temporary investigation", "not a default steady-state read path"], TEAL],
        ],
        sql: "SET spark.sql.parquet.mergeSchema = true;",
        note: "Schema merging solves a metadata problem, not a performance one, so keep it off except when needed."
      }
    };
    const group = btnGroup(container, [["snappy", "snappy"], ["zstd", "zstd"], ["gzip", "gzip"], ["merge", "mergeSchema=true"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, modes[mode].title, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      modes[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      box(stage, modes[mode].sql, { bg: "#faf8ff", border: mode === "merge" ? RED : PURPLE }).style("margin-bottom", "8px");
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function joinStrategy(sizeMb, mode) {
    if (mode === "raised") {
      return sizeMb <= 200 ? "BroadcastHashJoin" : "SortMergeJoin";
    }
    if (mode === "disabled") {
      return "SortMergeJoin";
    }
    if (mode === "shuffled") {
      if (sizeMb <= 10) return "BroadcastHashJoin";
      return sizeMb <= 600 ? "ShuffledHashJoin" : "SortMergeJoin";
    }
    return sizeMb <= 10 ? "BroadcastHashJoin" : "SortMergeJoin";
  }

  function renderJoin(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const factSize = 2048;
    let sizeMb = 40;
    let mode = "default";

    const controls = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "8px").style("margin-bottom", "8px");
    const group = btnGroup(controls, [["default", "threshold 10 MB"], ["raised", "threshold 200 MB"], ["disabled", "threshold -1"], ["shuffled", "prefer SMJ = false"]], (v) => { mode = v; update(); });
    const sliderRow = controls.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("flex-wrap", "wrap");
    sliderRow.append("span").style("font-size", "0.74rem").text("Filtered dimension size (MB)");
    const label = sliderRow.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    sliderRow.append("input").attr("type", "range").attr("min", 2).attr("max", 300).attr("step", 2).attr("value", sizeMb).style("width", "220px")
      .on("input", function () { sizeMb = +this.value; update(); });

    const stage = container.append("div");

    function update() {
      const strategy = joinStrategy(sizeMb, mode);
      const threshold = mode === "raised" ? "200 MB" : mode === "disabled" ? "disabled" : "10 MB";
      label.text(sizeMb);
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      stageCard(row, "fact_sales", [`probe side ≈ ${factSize} MB`, "shuffle-heavy if not broadcast"], PURPLE);
      stageCard(row, "dim_product", [`build side ≈ ${sizeMb} MB`, `threshold = ${threshold}`], sizeMb <= 10 ? HIT_COLOR : AMBER);
      stageCard(row, "Planner choice", [strategy, mode === "shuffled" && strategy === "ShuffledHashJoin" ? "build side stays much smaller" : ""].filter(Boolean), strategy === "BroadcastHashJoin" ? HIT_COLOR : strategy === "ShuffledHashJoin" ? TEAL : AMBER);
      const chips = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(chips, `autoBroadcastJoinThreshold = ${threshold}`, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE });
      chip(chips, `preferSortMergeJoin = ${mode === "shuffled" ? "false" : "true"}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      box(stage, `EXPLAIN
SELECT f.sale_id, d.category
FROM fact_sales AS f
JOIN dim_product AS d ON f.product_id = d.product_id;
-- expected physical operator: ${strategy}`, { bg: "#faf8ff", border: strategy === "BroadcastHashJoin" ? HIT_COLOR : PURPLE });
    }
    update();
    mode = group.get();
  }

  function renderMemory(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const heapGb = 32;
    const sparkPoolGb = +(heapGb * 0.6).toFixed(1);
    const userGb = +(heapGb - sparkPoolGb).toFixed(1);
    const modes = {
      balanced: {
        title: "Balanced workload",
        cards: [
          ["User / JVM reserve", [`${userGb} GB`, "outside Spark pool"], GRAY],
          ["Storage in use", ["6 GB cached", "below soft reserve"], PURPLE],
          ["Execution in use", ["8 GB shuffle / agg", "no spill"], HIT_COLOR],
        ],
        note: `On a ${heapGb} GB executor, Spark's unified pool is ${sparkPoolGb} GB. This workload fits comfortably.`
      },
      cache: {
        title: "Cache-heavy workload",
        cards: [
          ["User / JVM reserve", [`${userGb} GB`, "unchanged"], GRAY],
          ["Storage in use", ["11 GB cached", "borrows from execution headroom"], PURPLE],
          ["Execution in use", ["4 GB active", "less room for future shuffles"], AMBER],
        ],
        note: "Storage can grow when execution is quiet, but that borrowed space becomes reclaimable if a shuffle needs it."
      },
      spill: {
        title: "Execution spike",
        cards: [
          ["User / JVM reserve", [`${userGb} GB`, "outside Spark pool"], GRAY],
          ["Storage after eviction", ["2 GB remains cached", "most reclaimable cache removed"], AMBER],
          ["Execution demand", ["22 GB requested", `pool limit = ${sparkPoolGb} GB`], RED],
          ["Disk spill", ["~2.8 GB spills", "query continues, slower"], HIT_COLOR],
        ],
        note: "Once the working set exceeds the unified pool even after eviction, Spark spills partitions to disk."
      }
    };
    const group = btnGroup(container, [["balanced", "Balanced"], ["cache", "Cache-heavy"], ["spill", "Execution spike"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, modes[mode].title, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      modes[mode].cards.forEach((card, idx) => {
        stageCard(row, card[0], card[1], card[2]);
        if (mode === "spill" && idx < modes[mode].cards.length - 1) arrow(row);
      });
      const chips = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(chips, `spark.memory.fraction = 0.6 -> ${sparkPoolGb} GB pool`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      chip(chips, "spark.memory.storageFraction = 0.5", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function renderSession(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const states = {
      set: {
        command: "SET spark.sql.shuffle.partitions = 50;",
        cards: [
          ["Cluster default", ["shuffle.partitions = 200"], PURPLE],
          ["Session A", ["override = 50", "effective = 50"], HIT_COLOR],
          ["Session B", ["effective = 200"], GRAY],
        ],
        note: "SET mutates only the current session's effective value."
      },
      inspect: {
        command: `SET -v;
-- shows spark.sql.*, spark.hadoop.*, and custom keys`,
        cards: [
          ["Cluster default", ["unchanged"], PURPLE],
          ["Session A", ["override still visible", "inspection only"], HIT_COLOR],
          ["Session B", ["unchanged"], GRAY],
        ],
        note: "SET -v is read-only: it surfaces current values and descriptions but changes nothing."
      },
      reset_key: {
        command: "RESET spark.sql.shuffle.partitions;",
        cards: [
          ["Cluster default", ["shuffle.partitions = 200"], PURPLE],
          ["Session A", ["override removed", "effective = 200"], AMBER],
          ["Session B", ["effective = 200"], GRAY],
        ],
        note: "RESET key removes only that one override; other session settings can stay in place."
      },
      reset_all: {
        command: "RESET;",
        cards: [
          ["Cluster default", ["shuffle.partitions = 200", "broadcast threshold = 10 MB"], PURPLE],
          ["Session A", ["all overrides cleared", "back to cluster values"], AMBER],
          ["Session B", ["never changed"], GRAY],
        ],
        note: "RESET with no key clears the whole session-level overlay at once."
      }
    };
    const group = btnGroup(container, [["set", "SET"], ["inspect", "SET -v"], ["reset_key", "RESET key"], ["reset_all", "RESET"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      states[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      box(stage, states[mode].command, { bg: "#faf8ff", border: mode === "inspect" ? TEAL : PURPLE }).style("margin-bottom", "8px");
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(states[mode].note);
    }
    render(group.get());
  }

  function renderShuffle(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const dataMb = 25600;
    let partitions = 200;
    let mode = "off";

    const controls = container.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "8px").style("margin-bottom", "8px");
    const group = btnGroup(controls, [["off", "AQE off"], ["on", "AQE on"], ["overwrite", "Overwrite mode"]], (v) => { mode = v; update(); });
    const sliderRow = controls.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("flex-wrap", "wrap");
    sliderRow.append("span").style("font-size", "0.74rem").text("spark.sql.shuffle.partitions");
    const label = sliderRow.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    sliderRow.append("input").attr("type", "range").attr("min", 20).attr("max", 1000).attr("step", 20).attr("value", partitions).style("width", "220px")
      .on("input", function () { partitions = +this.value; update(); });

    const stage = container.append("div");

    function update() {
      label.text(partitions);
      stage.selectAll("*").remove();
      if (mode === "overwrite") {
        const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
        stageCard(row, "STATIC", ["overwrite target resolved up front", "easy to replace the full table"], RED);
        stageCard(row, "DYNAMIC", ["only touched partitions replaced", "safer for daily reloads"], HIT_COLOR);
        box(stage, `SET spark.sql.sources.partitionOverwriteMode = DYNAMIC;
INSERT OVERWRITE TABLE sales
SELECT ... WHERE order_date = CURRENT_DATE();`, { bg: "#faf8ff", border: PURPLE }).style("margin-bottom", "8px");
        stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text("The shuffle partition count tunes task parallelism; overwrite mode separately controls which table partitions a write replaces.");
        return;
      }

      const finalPartitions = mode === "on" ? Math.min(partitions, 200) : partitions;
      const avgBefore = +(dataMb / partitions).toFixed(1);
      const avgAfter = +(dataMb / finalPartitions).toFixed(1);
      const health = mode === "on"
        ? (finalPartitions < partitions ? HIT_COLOR : avgAfter > 256 ? RED : AMBER)
        : (avgBefore < 64 ? AMBER : avgBefore > 256 ? RED : HIT_COLOR);
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      stageCard(row, "Shuffle data", ["25 GB", "example from the guide"], PURPLE);
      stageCard(row, "Configured partitions", [String(partitions), `${avgBefore} MB each before AQE`], mode === "off" ? AMBER : TEAL);
      stageCard(row, "Final tasks", [String(finalPartitions), `${avgAfter} MB each after AQE`], health);
      const chips = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(chips, mode === "on" ? "AQE can merge tiny partitions" : "No runtime coalescing", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE });
      chip(chips, avgBefore < 64 ? "many tiny tasks likely" : avgBefore > 256 ? "heavier tasks, more spill risk" : "near the 128 MB rule of thumb", { bg: "rgba(38,166,154,0.12)", fg: health === RED ? RED : FG, border: health });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
        .text(mode === "on"
          ? "AQE can only shrink the partition count when partitions are too small; it cannot invent more parallelism than the starting value allows."
          : "Without AQE, the configured shuffle partition count directly determines the downstream task count.");
    }
    update();
    mode = group.get();
  }

  function init() {
    const specs = [
      ["viz-config-overview", renderConfigOverview],
      ["viz-config-ansi", renderAnsi],
      ["viz-config-aqe", renderAqe],
      ["viz-config-io", renderIo],
      ["viz-config-join", renderJoin],
      ["viz-config-memory", renderMemory],
      ["viz-config-session", renderSession],
      ["viz-config-shuffle", renderShuffle],
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
