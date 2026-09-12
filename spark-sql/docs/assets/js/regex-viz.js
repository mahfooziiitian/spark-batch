/**
 * regex-viz.js
 * D3 v7 interactive visualizations for the regex function pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-regex-overview         — same regex family, different no-match outputs.
 *   #viz-regex-count-scan       — non-overlapping count vs overlap lookahead.
 *   #viz-regex-extract-groups   — group 0 / 1 / 2 plus optional-group-empty behavior.
 *   #viz-regex-instr-position   — 1-based match position and no-match zero.
 *   #viz-regex-like-anchor      — LIKE-style wildcard expectation vs real regex.
 *   #viz-regexp-operator-alias  — REGEXP vs RLIKE alias and NOT REGEXP.
 *   #viz-regexp-replace-backref — backreferences and position-start scanning.
 *   #viz-regexp-substr-first-match — first full match vs NULL on no match.
 *   #viz-rlike-ilike-case       — case-sensitive regex vs (?i) vs ILIKE wildcard.
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
      .style("display", "inline-flex")
      .style("align-items", "center")
      .style("padding", "5px 12px")
      .style("border-radius", "5px")
      .style("font-size", "0.74rem")
      .style("font-family", "monospace")
      .style("cursor", opts.clickable ? "pointer" : "default")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .style("opacity", opts.opacity != null ? opts.opacity : 1)
      .text(text);
  }

  function box(parent, text, opts) {
    opts = opts || {};
    return parent.append("div")
      .style("padding", opts.padding || "10px 12px")
      .style("border-radius", "8px")
      .style("border", `1px solid ${opts.border || "#e0e0e0"}`)
      .style("background", opts.bg || "#fff")
      .style("color", opts.fg || FG)
      .style("font-size", opts.size || "0.75rem")
      .style("font-family", opts.mono ? "monospace" : "inherit")
      .style("line-height", "1.45")
      .text(text);
  }

  function makeTabs(container, items, getLabel, onPick) {
    const row = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.6rem");
    items.forEach((item, i) => {
      row.append("button")
        .attr("type", "button")
        .attr("data-i", i)
        .style("padding", "4px 10px")
        .style("border-radius", "5px")
        .style("border", "1px solid #ccc")
        .style("background", i === 0 ? PURPLE : "#fff")
        .style("color", i === 0 ? "#fff" : FG)
        .style("cursor", "pointer")
        .style("font-size", "0.72rem")
        .style("font-family", "monospace")
        .text(getLabel(item))
        .on("click", function () { onPick(item, i, row); });
    });
    return row;
  }

  function highlightString(row, text, start, len) {
    row.selectAll("*").remove();
    for (let i = 0; i < text.length; i++) {
      const hit = start >= 0 && i >= start && i < start + len;
      chip(row, text[i] === " " ? "␠" : text[i], {
        bg: hit ? "rgba(38,166,154,0.12)" : "#f5f5f7",
        fg: hit ? HIT_COLOR : FG,
        border: hit ? HIT_COLOR : "#ddd",
      });
    }
  }

  function renderRegexOverview(el) {
    const cases = [
      {
        label: "matching",
        sample: "order-123",
        regex: "(\\d+)",
        outputs: [
          ["REGEXP_LIKE", "true", TEAL],
          ["REGEXP_EXTRACT(..., 1)", "123", PURPLE],
          ["REGEXP_SUBSTR", "123", PURPLE],
          ["REGEXP_INSTR", "7", AMBER],
          ["REGEXP_COUNT", "1", TEAL],
        ],
      },
      {
        label: "no match",
        sample: "plain text",
        regex: "(\\d+)",
        outputs: [
          ["REGEXP_LIKE", "false", RED],
          ["REGEXP_EXTRACT(..., 1)", "''", AMBER],
          ["REGEXP_SUBSTR", "NULL", RED],
          ["REGEXP_INSTR", "0", AMBER],
          ["REGEXP_COUNT", "0", AMBER],
        ],
      },
    ];
    let current = cases[0];

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabRow = makeTabs(container, cases, (d) => d.label, (item, idx, row) => {
      current = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const sampleLine = container.append("div")
      .style("font-size", "0.76rem")
      .style("color", FG)
      .style("margin-bottom", "0.55rem");
    const outputsWrap = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(150px, 1fr))")
      .style("gap", "8px");

    function update() {
      sampleLine.html(`<code>${current.sample}</code> with <code>${current.regex}</code>`);
      outputsWrap.selectAll("*").remove();
      current.outputs.forEach(([label, value, color]) => {
        const card = outputsWrap.append("div")
          .style("padding", "8px 10px")
          .style("border", "1px solid #e0e0e0")
          .style("border-radius", "8px")
          .style("background", "#fff");
        card.append("div").style("font-size", "0.67rem").style("color", GRAY).style("margin-bottom", "4px").text(label);
        card.append("div").style("font-size", "0.88rem").style("font-family", "monospace").style("font-weight", "700").style("color", color).text(value);
      });
    }
    update();
  }

  function renderRegexCountScan(el) {
    const modes = [
      {
        label: "default",
        title: "REGEXP_COUNT('ababa', 'aba')",
        starts: [0, 2],
        active: [true, false],
        count: "1",
        note: "Non-overlapping scan: the second start sits inside the first match, so it is skipped.",
      },
      {
        label: "lookahead",
        title: "REGEXP_COUNT('ababa', '(?=(aba))')",
        starts: [0, 2],
        active: [true, true],
        count: "2",
        note: "Zero-width lookahead counts both starting positions without consuming characters.",
      },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const title = container.append("div")
      .style("font-family", "monospace")
      .style("font-size", "0.78rem")
      .style("margin-bottom", "0.45rem")
      .style("color", FG);
    const row = container.append("div")
      .style("display", "flex")
      .style("gap", "6px")
      .style("margin-bottom", "0.55rem");
    const result = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("align-items", "center")
      .style("margin-bottom", "0.45rem");
    const note = container.append("div")
      .style("font-size", "0.73rem")
      .style("color", FG);

    function update() {
      title.text(mode.title);
      row.selectAll("*").remove();
      const text = "ababa";
      for (let i = 0; i < text.length; i++) {
        const idx = mode.starts.indexOf(i);
        const isStart = idx !== -1;
        const active = isStart && mode.active[idx];
        chip(row, text[i], {
          bg: active ? "rgba(38,166,154,0.12)" : isStart ? "rgba(255,167,38,0.12)" : "#f5f5f7",
          fg: active ? HIT_COLOR : isStart ? AMBER : FG,
          border: active ? HIT_COLOR : isStart ? AMBER : "#ddd",
        });
      }
      result.selectAll("*").remove();
      chip(result, `count = ${mode.count}`, { bg: "rgba(124,77,255,0.12)", fg: PURPLE, border: PURPLE });
      note.text(mode.note);
    }
    update();
  }

  function renderRegexExtractGroups(el) {
    const modes = [
      { label: "group 0", query: "abc-123", value: "abc-123", note: "Group 0 is the whole regex match." },
      { label: "group 1", query: "abc-123", value: "abc", note: "Group 1 is the first parenthesized capture." },
      { label: "group 2", query: "abc-123", value: "123", note: "Group 2 is the second parenthesized capture." },
      { label: "optional", query: "color=blue", value: "''", note: "The regex matched, but optional group 1 did not participate, so Spark returns an empty string." },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const code = container.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("margin-bottom", "0.5rem").style("color", FG);
    const out = container.append("div").style("margin-bottom", "0.45rem");
    const note = container.append("div").style("font-size", "0.73rem").style("color", FG);

    function update() {
      if (mode.label === "optional") {
        code.html("<code>REGEXP_EXTRACT('color=blue', 'color=(red)?(blue)', 1)</code>");
      } else {
        code.html(`<code>REGEXP_EXTRACT('abc-123', '([a-z]+)-(\\d+)', ${mode.label.split(" ")[1]})</code>`);
      }
      out.selectAll("*").remove();
      chip(out, `result = ${mode.value}`, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      note.text(mode.note);
    }
    update();
  }

  function renderRegexInstrPosition(el) {
    const modes = [
      { label: "match", text: "hello123", start: 5, len: 3, value: "6", note: "Spark counts from 1, so the first digit sits at position 6." },
      { label: "no match", text: "helloworld", start: -1, len: 0, value: "0", note: "No regex match returns 0, not NULL." },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const posRow = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "4px");
    const charRow = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "0.5rem");
    const result = container.append("div").style("margin-bottom", "0.45rem");
    const note = container.append("div").style("font-size", "0.73rem").style("color", FG);

    function update() {
      posRow.selectAll("*").remove();
      charRow.selectAll("*").remove();
      for (let i = 0; i < mode.text.length; i++) {
        chip(posRow, String(i + 1), { bg: "#fff", fg: GRAY, border: "#eee" });
      }
      highlightString(charRow, mode.text, mode.start, mode.len);
      result.selectAll("*").remove();
      chip(result, `REGEXP_INSTR(...) = ${mode.value}`, {
        bg: mode.value === "0" ? "rgba(255,167,38,0.12)" : "rgba(38,166,154,0.12)",
        fg: mode.value === "0" ? AMBER : HIT_COLOR,
        border: mode.value === "0" ? AMBER : HIT_COLOR,
      });
      note.text(mode.note);
    }
    update();
  }

  function renderRegexLikeAnchor(el) {
    const modes = [
      { label: "LIKE habit", expr: "REGEXP_LIKE('file-12.csv', 'file-%.csv')", result: "false", note: "% is literal in regex, so nothing matches here.", color: RED },
      { label: "true regex", expr: "REGEXP_LIKE('file-12.csv', '^file-.*\\.csv$')", result: "true", note: "Use .* for any-length text and \\. for a literal dot.", color: HIT_COLOR },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const expr = container.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("margin-bottom", "0.5rem").style("color", FG);
    const result = container.append("div").style("margin-bottom", "0.45rem");
    const note = container.append("div").style("font-size", "0.73rem").style("color", FG);

    function update() {
      expr.text(mode.expr);
      result.selectAll("*").remove();
      chip(result, mode.result, {
        bg: mode.result === "true" ? "rgba(38,166,154,0.12)" : "rgba(239,83,80,0.1)",
        fg: mode.color,
        border: mode.color,
      });
      note.text(mode.note);
    }
    update();
  }

  function renderRegexpOperatorAlias(el) {
    const modes = [
      { label: "RLIKE", expr: "'abc123' RLIKE '\\d+'", result: "true", color: HIT_COLOR },
      { label: "REGEXP", expr: "'abc123' REGEXP '\\d+'", result: "true", color: HIT_COLOR },
      { label: "NOT REGEXP", expr: "'abc123' NOT REGEXP '\\d+'", result: "false", color: RED },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const expr = container.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("margin-bottom", "0.5rem").style("color", FG);
    const result = container.append("div").style("margin-bottom", "0.45rem");
    const note = container.append("div").style("font-size", "0.73rem").style("color", FG);

    function update() {
      expr.text(mode.expr);
      result.selectAll("*").remove();
      chip(result, mode.result, {
        bg: mode.result === "true" ? "rgba(38,166,154,0.12)" : "rgba(239,83,80,0.1)",
        fg: mode.color,
        border: mode.color,
      });
      note.text(mode.label === "REGEXP" ? "Same matcher and same result as RLIKE." : mode.label === "RLIKE" ? "Operator form of a boolean regex test." : "NOT just negates the alias result.");
    }
    update();
  }

  function renderRegexpReplaceBackref(el) {
    const modes = [
      {
        label: "backref",
        source: "2024-01-15",
        pattern: "(\\d{4})-(\\d{2})-(\\d{2})",
        repl: "$2/$3/$1",
        result: "01/15/2024",
        note: "Capture groups let the replacement string reorder the matched pieces.",
      },
      {
        label: "position",
        source: "cat bat rat",
        pattern: "[a-z]at",
        repl: "X",
        result: "cat X X",
        note: "Scanning starts at character 5, so the first token stays untouched but later matches still change.",
      },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const grid = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(120px, 1fr))")
      .style("gap", "8px")
      .style("margin-bottom", "0.5rem");
    const note = container.append("div").style("font-size", "0.73rem").style("color", FG);

    function update() {
      grid.selectAll("*").remove();
      box(grid, `source\n${mode.source}`, { mono: true, bg: "#fff" });
      box(grid, `pattern\n${mode.pattern}`, { mono: true, bg: "#fff" });
      box(grid, `replace\n${mode.repl}`, { mono: true, bg: "#fff" });
      box(grid, `result\n${mode.result}`, { mono: true, bg: "rgba(38,166,154,0.08)", border: HIT_COLOR, fg: HIT_COLOR });
      note.text(mode.note);
    }
    update();
  }

  function renderRegexpSubstrFirstMatch(el) {
    const modes = [
      { label: "match", text: "error-404 then 500", start: 6, len: 3, result: "404", note: "REGEXP_SUBSTR returns the first full match only." },
      { label: "no match", text: "plain text", start: -1, len: 0, result: "NULL", note: "Unlike REGEXP_EXTRACT, no match here returns NULL." },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const row = container.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "0.5rem");
    const result = container.append("div").style("margin-bottom", "0.45rem");
    const note = container.append("div").style("font-size", "0.73rem").style("color", FG);

    function update() {
      highlightString(row, mode.text, mode.start, mode.len);
      result.selectAll("*").remove();
      chip(result, `result = ${mode.result}`, {
        bg: mode.result === "NULL" ? "rgba(239,83,80,0.1)" : "rgba(38,166,154,0.12)",
        fg: mode.result === "NULL" ? RED : HIT_COLOR,
        border: mode.result === "NULL" ? RED : HIT_COLOR,
      });
      note.text(mode.note);
    }
    update();
  }

  function renderRlikeIlikeCase(el) {
    const modes = [
      { label: "RLIKE", expr: "'Spark' RLIKE 'spark'", result: "false", note: "Plain RLIKE is case-sensitive.", color: RED },
      { label: "RLIKE (?i)", expr: "'Spark' RLIKE '(?i)spark'", result: "true", note: "Inline flag (?i) makes the regex case-insensitive.", color: HIT_COLOR },
      { label: "ILIKE", expr: "'Spark' ILIKE 'sp%rk'", result: "true", note: "ILIKE ignores case but uses SQL wildcards, not regex tokens.", color: HIT_COLOR },
    ];
    let mode = modes[0];

    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabRow = makeTabs(container, modes, (d) => d.label, (item, idx, row) => {
      mode = item;
      row.selectAll("button").each(function (_, i) {
        d3.select(this)
          .style("background", i === idx ? PURPLE : "#fff")
          .style("color", i === idx ? "#fff" : FG)
          .style("border", `1px solid ${i === idx ? PURPLE : "#ccc"}`);
      });
      update();
    });
    tabRow.selectAll("button").style("border", function (_, i) { return `1px solid ${i === 0 ? PURPLE : "#ccc"}`; });

    const expr = container.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("margin-bottom", "0.5rem").style("color", FG);
    const result = container.append("div").style("margin-bottom", "0.45rem");
    const note = container.append("div").style("font-size", "0.73rem").style("color", FG);

    function update() {
      expr.text(mode.expr);
      result.selectAll("*").remove();
      chip(result, mode.result, {
        bg: mode.result === "true" ? "rgba(38,166,154,0.12)" : "rgba(239,83,80,0.1)",
        fg: mode.color,
        border: mode.color,
      });
      note.text(mode.note);
    }
    update();
  }

  function init() {
    const specs = [
      ["viz-regex-overview", renderRegexOverview],
      ["viz-regex-count-scan", renderRegexCountScan],
      ["viz-regex-extract-groups", renderRegexExtractGroups],
      ["viz-regex-instr-position", renderRegexInstrPosition],
      ["viz-regex-like-anchor", renderRegexLikeAnchor],
      ["viz-regexp-operator-alias", renderRegexpOperatorAlias],
      ["viz-regexp-replace-backref", renderRegexpReplaceBackref],
      ["viz-regexp-substr-first-match", renderRegexpSubstrFirstMatch],
      ["viz-rlike-ilike-case", renderRlikeIlikeCase],
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

