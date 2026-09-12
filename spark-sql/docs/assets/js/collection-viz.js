/**
 * collection-viz.js
 * D3 v7 interactive visualizations for the Collection Functions pages
 * (docs/functions/collection/{index,array,list,map,set,struct}.md).
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-collection-capabilities — capability matrix (ARRAY/MAP/STRUCT) for
 *                                  equality, ordering, set-ops.
 *   #viz-array-sort-nulls        — ARRAY_SORT vs SORT_ARRAY NULL placement.
 *   #viz-collect-list-order      — bare aggregate vs window-ordered COLLECT_LIST.
 *   #viz-map-limits              — unsupported MAP operations + workaround.
 *   #viz-find-in-set             — interactive FIND_IN_SET position finder.
 *   #viz-struct-equality         — field-by-field NULL-safe struct equality.
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

  /* ══════════════════════════════════════════════════════════════════
   * 1. Collection capability matrix
   * ══════════════════════════════════════════════════════════════════ */
  function renderCollectionCapabilities(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");

    const support = {
      equality: { ARRAY: true, MAP: false, STRUCT: true },
      ordering: { ARRAY: true, MAP: false, STRUCT: false },
      distinct_groupby: { ARRAY: true, MAP: false, STRUCT: true },
      indexing: { ARRAY: true, MAP: false, STRUCT: false },
    };
    const labels = {
      equality: "= equality operator",
      ordering: "ORDER BY / sorting",
      distinct_groupby: "DISTINCT / GROUP BY",
      indexing: "positional indexing",
    };

    const group = btnGroup(container, Object.keys(support).map((k) => [k, labels[k]]), render);

    function render(cap) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "1rem");
      ["ARRAY", "MAP", "STRUCT"].forEach((t) => {
        const ok = support[cap][t];
        const col = row.append("div").style("flex", "1").style("text-align", "center");
        col.append("div").style("font-size", "0.74rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "6px").text(t);
        chip(col, ok ? "✓ supported" : "✕ not supported", ok
          ? { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      });
      stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "8px")
        .text(cap === "ordering" && !support.ordering.STRUCT
          ? "STRUCT supports = but not < / > / ORDER BY directly (only via ARRAY_SORT(MAP_ENTRIES(...)) style workarounds for maps)."
          : "");
    }
    render(group.get());
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. ARRAY_SORT vs SORT_ARRAY NULL placement
   * ══════════════════════════════════════════════════════════════════ */
  function renderArraySortNulls(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const arr = [3, null, 1, null, 2];

    function renderRow(label, sorted, color) {
      const row = stage.append("div").style("margin-bottom", "10px");
      row.append("div").style("font-size", "0.7rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px").text(label);
      const chips = row.append("div").style("display", "flex").style("gap", "4px");
      sorted.forEach((v) => chip(chips, v === null ? "NULL" : String(v), v === null
        ? { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }
        : { bg: `${color}1a`, fg: color, border: color }));
    }

    stage.append("div").style("font-family", "monospace").style("font-size", "0.72rem").style("color", GRAY).style("margin-bottom", "10px")
      .text("input: [3, NULL, 1, NULL, 2]");

    renderRow("SORT_ARRAY(arr, true)  — ascending", [null, null, 1, 2, 3], HIT_COLOR);
    renderRow("SORT_ARRAY(arr, false) — descending", [3, 2, 1, null, null], PURPLE);
    renderRow("ARRAY_SORT(arr) — default comparator", [1, 2, 3, null, null], TEAL);

    stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px")
      .text("Note: ARRAY_SORT keeps NULLs last even though the values are in ascending order — its default comparator never flips NULL position based on direction.");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. COLLECT_LIST order determinism
   * ══════════════════════════════════════════════════════════════════ */
  function renderCollectListOrder(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");

    const group = btnGroup(container, [["bare", "Bare COLLECT_LIST"], ["window", "COLLECT_LIST(...) OVER (ORDER BY seq)"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      if (mode === "bare") {
        stage.append("div").style("font-size", "0.72rem").style("color", FG).style("margin-bottom", "6px").text("Two separate runs of the same query, same input rows:");
        [["login", "purchase", "logout"], ["purchase", "login", "logout"]].forEach((seq, i) => {
          const row = stage.append("div").style("display", "flex").style("gap", "4px").style("margin-bottom", "4px");
          row.append("span").style("font-size", "0.68rem").style("color", GRAY).style("width", "50px").text(`run ${i + 1}:`);
          seq.forEach((s) => chip(row, s, { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER }));
        });
        stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px")
          .text("✕ Order can differ run-to-run — task scheduling and shuffle order aren't guaranteed.");
      } else {
        stage.append("div").style("font-size", "0.72rem").style("color", FG).style("margin-bottom", "6px").text("Every run, same input rows, explicit ORDER BY seq:");
        [1, 2].forEach((i) => {
          const row = stage.append("div").style("display", "flex").style("gap", "4px").style("margin-bottom", "4px");
          row.append("span").style("font-size", "0.68rem").style("color", GRAY).style("width", "50px").text(`run ${i}:`);
          ["login", "purchase", "logout"].forEach((s) => chip(row, s, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }));
        });
        stage.append("div").style("font-size", "0.7rem").style("color", GRAY).style("margin-top", "6px")
          .text("✓ Identical every run — the window frame's ORDER BY seq fixes the collection order.");
      }
    }
    render(group.get());
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. MAP unsupported operations
   * ══════════════════════════════════════════════════════════════════ */
  function renderMapLimits(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");

    const ops = {
      equality: { sql: "MAP(1,'a') = MAP(1,'a')", ok: false, err: "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE]" },
      distinct: { sql: "SELECT DISTINCT map_col FROM t", ok: false, err: "[UNSUPPORTED_FEATURE.SET_OPERATION_ON_MAP_TYPE]" },
      keys: { sql: "MAP_KEYS(map_col)", ok: true, err: null },
      entries_sorted: { sql: "ARRAY_SORT(MAP_ENTRIES(map_col))", ok: true, err: null },
    };
    const labels = { equality: "Direct = comparison", distinct: "DISTINCT / GROUP BY", keys: "MAP_KEYS() extraction", entries_sorted: "Workaround: sorted entries" };

    const group = btnGroup(container, Object.keys(ops).map((k) => [k, labels[k]]), render);

    function render(key) {
      stage.selectAll("*").remove();
      const o = ops[key];
      stage.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("margin-bottom", "8px").text(o.sql);
      if (o.ok) {
        chip(stage, "✓ works", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      } else {
        chip(stage, `✕ ${o.err}`, { bg: "rgba(239,83,80,0.1)", fg: RED, border: RED });
      }
    }
    render(group.get());
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. FIND_IN_SET position finder
   * ══════════════════════════════════════════════════════════════════ */
  function renderFindInSet(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const items = ["admin", "editor", "viewer", ""];

    const inputRow = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "0.7rem");
    inputRow.append("span").style("font-size", "0.76rem").style("color", FG).text("FIND_IN_SET(");
    const input = inputRow.append("input").attr("type", "text").attr("value", "editor")
      .style("width", "90px").style("font-family", "monospace").style("font-size", "0.76rem").style("padding", "3px 6px");
    inputRow.append("span").style("font-size", "0.76rem").style("color", FG).text(", 'admin,editor,viewer,')");

    const itemsRow = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "0.5rem");
    const resultRow = container.append("div").style("font-family", "monospace").style("font-size", "0.8rem");

    function update() {
      const needle = input.property("value");
      itemsRow.selectAll("*").remove();
      let pos = 0;
      items.forEach((it, i) => {
        const hit = it === needle;
        if (hit && pos === 0) pos = i + 1;
        chip(itemsRow, it === "" ? "(empty)" : it, hit
          ? { bg: "rgba(38,166,154,0.15)", fg: HIT_COLOR, border: HIT_COLOR }
          : { bg: "#f5f5f7", fg: GRAY, border: "#ddd" });
      });
      resultRow.html(pos > 0
        ? `→ <span style="color:${HIT_COLOR}">${pos}</span> (1-based position, case-sensitive exact match)`
        : `→ <span style="color:${RED}">0</span> (not found)`);
    }
    input.on("input", update);
    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 6. Struct field-by-field equality
   * ══════════════════════════════════════════════════════════════════ */
  function renderStructEquality(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");

    const left = { a: 1, b: null };
    const right = { a: 1, b: null };

    const row = stage.append("div").style("display", "flex").style("gap", "1.5rem").style("align-items", "flex-start");
    [["struct 1", left], ["struct 2", right]].forEach(([label, s]) => {
      const col = row.append("div");
      col.append("div").style("font-size", "0.7rem").style("font-weight", "700").style("color", FG).style("margin-bottom", "4px").text(label);
      Object.entries(s).forEach(([k, v]) => {
        chip(col.append("div").style("margin-bottom", "3px"), `${k}: ${v === null ? "NULL" : v}`,
          v === null ? { bg: "rgba(255,167,38,0.12)", fg: AMBER, border: AMBER } : { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      });
    });

    stage.append("div").style("margin-top", "10px").style("font-family", "monospace").style("font-size", "0.8rem")
      .html(`struct1 = struct2 → <span style="color:${HIT_COLOR}">true</span>  <span style="color:${GRAY}">(field 'a': 1=1 ✓, field 'b': NULL≡NULL ✓ under struct's NULL-safe comparison)</span>`);
    stage.append("div").style("margin-top", "6px").style("font-family", "monospace").style("font-size", "0.78rem").style("color", GRAY)
      .html(`compare with scalar: NULL = NULL → <span style="color:${RED}">NULL</span>  (three-valued logic, not true)`);
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-collection-capabilities", renderCollectionCapabilities],
      ["viz-array-sort-nulls", renderArraySortNulls],
      ["viz-collect-list-order", renderCollectListOrder],
      ["viz-map-limits", renderMapLimits],
      ["viz-find-in-set", renderFindInSet],
      ["viz-struct-equality", renderStructEquality],
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
