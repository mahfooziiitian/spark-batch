/**
 * caching-viz.js
 * D3 v7 interactive visualizations for docs/optimization/caching/{index,cache,config,manager}.md.
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

  function box(parent, title, color) {
    const wrap = parent.append("div")
      .style("border", `1.5px solid ${color}`)
      .style("border-radius", "8px")
      .style("padding", "10px")
      .style("background", `${color}12`)
      .style("min-width", "160px");
    wrap.append("div")
      .style("font-size", "0.68rem")
      .style("font-weight", "700")
      .style("letter-spacing", "0.02em")
      .style("color", color)
      .style("margin-bottom", "6px")
      .text(title);
    return wrap;
  }

  function renderCachingOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const states = {
      first: {
        title: "First run",
        plan: "SELECT id, id % 2 AS grp FROM RANGE(0, 10)",
        cache: "CACHE TABLE cached_range AS ...",
        result: "No reusable cache yet → fill cache and materialize batches",
        color: AMBER,
      },
      equivalent: {
        title: "Equivalent query",
        plan: "SELECT id, id % 2 AS grp FROM RANGE(0, 10)",
        cache: "Match found in CacheManager",
        result: "Rewrite to InMemoryRelation → Scan In-memory table",
        color: HIT_COLOR,
      },
      different: {
        title: "Different logical plan",
        plan: "SELECT id, (id + 1) % 2 AS grp FROM RANGE(0, 10)",
        cache: "No equivalent cached sub-plan",
        result: "Miss → recompute from original source",
        color: RED,
      },
    };
    const group = btnGroup(container, [["first", "First run"], ["equivalent", "Equivalent plan"], ["different", "Different plan"]], render);

    function render(mode) {
      const s = states[mode];
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "10px").style("flex-wrap", "wrap").style("align-items", "stretch");
      const incoming = box(row, "Incoming query", GRAY);
      chip(incoming, s.plan, { bg: "#fff", border: GRAY });
      const lookup = box(row, "Cache lookup", PURPLE);
      chip(lookup, s.cache, { bg: "rgba(124,77,255,0.08)", fg: PURPLE, border: PURPLE });
      const outcome = box(row, "Outcome", s.color);
      chip(outcome, s.result, { bg: "#fff", fg: s.color, border: s.color });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "8px")
        .text(mode === "equivalent"
          ? "Spark 4.2 reused the cache even without the cached table name when the logical plan matched."
          : mode === "first"
            ? "Eager caching pays the fill cost up front so later queries can read from the in-memory relation."
            : "A new expression shape means no cache hit, even if the query looks similar at a glance.");
    }
    render(group.get());
  }

  function renderCacheLifecycle(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const states = {
      cache: {
        state: "registered + materialized",
        chips: [["CACHE TABLE orders", PURPLE], ["isCached = true", HIT_COLOR], ["next SELECT uses cache", TEAL]],
        note: "`CACHE TABLE` ran eagerly in Spark 4.2 for an existing relation.",
      },
      lazy: {
        state: "registered, not filled yet",
        chips: [["CACHE LAZY TABLE orders", PURPLE], ["isCached = true", HIT_COLOR], ["first SELECT fills cache", AMBER]],
        note: "Lazy mode registers immediately but waits for the first read to populate cached batches.",
      },
      refresh: {
        state: "still cached",
        chips: [["REFRESH TABLE orders", PURPLE], ["cache entry kept", TEAL], ["use UNCACHE to drop data", RED]],
        note: "For an unchanged cached temp view, `REFRESH TABLE` did not remove the cache entry in local Spark 4.2 checks.",
      },
      uncache: {
        state: "uncached",
        chips: [["UNCACHE TABLE orders", RED], ["isCached = false", GRAY], ["table/view still queryable", TEAL]],
        note: "`UNCACHE TABLE` drops cached data but does not drop the relation name itself.",
      },
      clear: {
        state: "all session caches removed",
        chips: [["CLEAR CACHE", RED], ["every entry removed", GRAY], ["later queries recompute", AMBER]],
        note: "`CLEAR CACHE` clears the current SparkSession's cache registry.",
      },
    };
    const group = btnGroup(container, [["cache", "CACHE TABLE"], ["lazy", "CACHE LAZY TABLE"], ["refresh", "REFRESH TABLE"], ["uncache", "UNCACHE TABLE"], ["clear", "CLEAR CACHE"]], render);

    function colorFor(c) {
      return c === PURPLE ? { bg: "rgba(124,77,255,0.08)", fg: PURPLE, border: PURPLE }
        : c === HIT_COLOR || c === TEAL ? { bg: "rgba(38,166,154,0.10)", fg: HIT_COLOR, border: HIT_COLOR }
          : c === AMBER ? { bg: "rgba(255,167,38,0.14)", fg: AMBER, border: AMBER }
            : c === RED ? { bg: "rgba(239,83,80,0.10)", fg: RED, border: RED }
              : { bg: "#f5f5f7", fg: FG, border: GRAY };
    }

    function render(mode) {
      const s = states[mode];
      stage.selectAll("*").remove();
      stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      const chipRow = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      s.chips.forEach(([text, color]) => chip(chipRow, text, colorFor(color)));
      chip(stage, `state: ${s.state}`, { bg: "#fff", border: PURPLE, fg: PURPLE });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "8px").text(s.note);
    }
    render(group.get());
  }

  function renderCacheConfigTradeoffs(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const levels = {
      memory: {
        label: "MEMORY_ONLY",
        tradeoffs: [["fast reads", HIT_COLOR], ["no disk fallback", RED], ["best for small hot results", PURPLE]],
        note: "Use when the cached result fits comfortably in executor memory.",
      },
      memdisk: {
        label: "MEMORY_AND_DISK",
        tradeoffs: [["default verified shape", PURPLE], ["disk fallback available", TEAL], ["balanced risk", AMBER]],
        note: "Spark 4.2 showed the default SQL/DataFrame cache level as disk + memory + deserialized.",
      },
      disk: {
        label: "DISK_ONLY",
        tradeoffs: [["low memory pressure", TEAL], ["higher IO cost", RED], ["use when recomputation is worse", PURPLE]],
        note: "Useful when the result is too large for memory but still worth materializing once.",
      },
      join: {
        label: "Broadcast threshold",
        tradeoffs: [["threshold = -1 → no broadcast", RED], ["threshold = 10 MiB → broadcast hit", HIT_COLOR], ["cache changes source, not join rule", PURPLE]],
        note: "Cached tables can still participate in `BroadcastHashJoin` if `spark.sql.autoBroadcastJoinThreshold` allows it.",
      },
    };
    const group = btnGroup(container, [["memory", "MEMORY_ONLY"], ["memdisk", "MEMORY_AND_DISK"], ["disk", "DISK_ONLY"], ["join", "Join threshold"]], render);

    function styleFromColor(color) {
      if (color === HIT_COLOR || color === TEAL) return { bg: "rgba(38,166,154,0.10)", fg: HIT_COLOR, border: HIT_COLOR };
      if (color === RED) return { bg: "rgba(239,83,80,0.10)", fg: RED, border: RED };
      if (color === AMBER) return { bg: "rgba(255,167,38,0.14)", fg: AMBER, border: AMBER };
      return { bg: "rgba(124,77,255,0.08)", fg: PURPLE, border: PURPLE };
    }

    function render(mode) {
      const s = levels[mode];
      stage.selectAll("*").remove();
      chip(stage, s.label, { bg: "#fff", fg: PURPLE, border: PURPLE });
      const row = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin", "8px 0");
      s.tradeoffs.forEach(([text, color]) => chip(row, text, styleFromColor(color)));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(s.note);
    }
    render(group.get());
  }

  function renderCacheManagerMatch(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const cases = {
      same: {
        query: "SELECT * FROM cached_range",
        verdict: "hit",
        reason: "same named relation",
      },
      logical: {
        query: "SELECT id, id % 2 AS grp FROM RANGE(0, 10)",
        verdict: "hit",
        reason: "same logical computation without table name",
      },
      filtered: {
        query: "SELECT * FROM cached_range WHERE grp = 1",
        verdict: "hit",
        reason: "extra filter can run on top of cached superset",
      },
      subsetMiss: {
        query: "SELECT * FROM RANGE(0, 10) WHERE id = 1",
        verdict: "miss",
        reason: "a cached filtered subset is not a generic replacement",
      },
      different: {
        query: "SELECT id, (id + 1) % 2 AS grp FROM RANGE(0, 10)",
        verdict: "miss",
        reason: "different expression tree",
      },
    };
    const group = btnGroup(container, [["same", "Same relation"], ["logical", "Same logic"], ["filtered", "Filter on cached"], ["subsetMiss", "Subset miss"], ["different", "Different expression"]], render);

    function render(mode) {
      const c = cases[mode];
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "10px").style("flex-wrap", "wrap");
      const left = box(row, "Incoming plan", GRAY);
      chip(left, c.query, { bg: "#fff", border: GRAY });
      const right = box(row, "CacheManager verdict", c.verdict === "hit" ? HIT_COLOR : RED);
      chip(right, c.verdict === "hit" ? "rewrite to InMemoryRelation" : "keep original logical plan", c.verdict === "hit"
        ? { bg: "rgba(38,166,154,0.10)", fg: HIT_COLOR, border: HIT_COLOR }
        : { bg: "rgba(239,83,80,0.10)", fg: RED, border: RED });
      right.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px").text(c.reason);
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-caching-overview", renderCachingOverview],
      ["viz-cache-lifecycle", renderCacheLifecycle],
      ["viz-cache-config-tradeoffs", renderCacheConfigTradeoffs],
      ["viz-cache-manager-match", renderCacheManagerMatch],
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
