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

  function box(parent, title, lines, opts) {
    opts = opts || {};
    const wrap = parent.append("div")
      .style("border", `1px solid ${opts.border || "#e0e0e0"}`)
      .style("border-radius", "8px")
      .style("padding", "10px 12px")
      .style("background", opts.bg || "#fff")
      .style("min-width", opts.minWidth || "160px");
    wrap.append("div")
      .style("font-size", "0.7rem")
      .style("font-weight", "700")
      .style("color", opts.titleColor || FG)
      .style("margin-bottom", "6px")
      .text(title);
    lines.forEach((line) => {
      wrap.append("div")
        .style("font-size", "0.72rem")
        .style("color", line.color || FG)
        .style("font-family", line.code ? "monospace" : "inherit")
        .style("margin-top", "2px")
        .text(line.text);
    });
    return wrap;
  }

  function renderDatetimeOverview(el) {
    const types = [
      {
        name: "DATE",
        color: TEAL,
        lines: [
          "Stores a calendar day only",
          "Drops hour/minute/second",
          "Best for partitions and daily reports",
        ],
      },
      {
        name: "TIMESTAMP",
        color: PURPLE,
        lines: [
          "Represents an instant in time",
          "Displayed in the session timezone",
          "Best for event ordering and ingestion",
        ],
      },
      {
        name: "TIMESTAMP_NTZ",
        color: AMBER,
        lines: [
          "Keeps a wall-clock date+time",
          "No timezone conversion by itself",
          "Best for local schedules before zoning",
        ],
      },
      {
        name: "INTERVAL",
        color: RED,
        lines: [
          "Represents a duration, not a point",
          "Add/subtract it from dates or timestamps",
          "Best for offsets like +7 days or +1 month",
        ],
      },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.7rem");
    const panel = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))")
      .style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      types.forEach((t, i) => {
        chip(tabs, t.name, {
          clickable: true,
          bg: i === active ? t.color : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? t.color : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const current = types[active];
      box(panel, current.name, current.lines.map((text) => ({ text })), {
        border: current.color,
        bg: "rgba(255,255,255,0.98)",
        titleColor: current.color,
      });
      box(panel, "Sample literal", [{ text: current.name === "INTERVAL" ? "INTERVAL 2 DAYS" : `${current.name} '2024-07-19 14:05:00'`, code: true }], {
        border: "#ddd",
      });
    }
    update();
  }

  function renderIntervalMonthLogic(el) {
    const cases = [
      { label: "2024-01-31 + 1 MONTH", result: "2024-02-29", note: "Leap-year February clamps to the 29th." },
      { label: "2023-01-31 + 1 MONTH", result: "2023-02-28", note: "Non-leap February clamps to the 28th." },
      { label: "2024-01-31 + 30 DAYS", result: "2024-03-01", note: "Elapsed-day arithmetic is different from calendar months." },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const panel = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      cases.forEach((item, i) => {
        chip(tabs, item.label, {
          clickable: true,
          bg: i === active ? PURPLE : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? PURPLE : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const item = cases[active];
      box(panel, "Input", [{ text: item.label, code: true }], { border: "#ddd" });
      box(panel, "Spark result", [{ text: item.result, code: true }, { text: item.note, color: GRAY }], { border: TEAL, titleColor: TEAL, bg: "rgba(38,166,154,0.06)" });
    }
    update();
  }

  function renderTimestampParseFlow(el) {
    const cases = [
      {
        name: "Valid format",
        sql: "TO_TIMESTAMP('2024-07-19 14:05', 'yyyy-MM-dd HH:mm')",
        result: "2024-07-19 14:05:00",
        color: TEAL,
      },
      {
        name: "Invalid format",
        sql: "TO_TIMESTAMP('07/19/2024', 'yyyy-MM-dd')",
        result: "ANSI parse error",
        color: RED,
      },
      {
        name: "Safe parse",
        sql: "TRY_TO_TIMESTAMP('07/19/2024', 'yyyy-MM-dd')",
        result: "NULL",
        color: AMBER,
      },
      {
        name: "Offset in input",
        sql: "TO_TIMESTAMP('2024-07-19T12:00:00-07:00')",
        result: "Offset is applied during parsing",
        color: PURPLE,
      },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const panel = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(200px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      cases.forEach((item, i) => {
        chip(tabs, item.name, {
          clickable: true,
          bg: i === active ? item.color : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? item.color : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const item = cases[active];
      box(panel, "Expression", [{ text: item.sql, code: true }], { border: "#ddd" });
      box(panel, "Outcome", [{ text: item.result }, { text: item.name === "Safe parse" ? "Useful when bad source rows should not fail the whole query." : "", color: GRAY }].filter((x) => x.text), {
        border: item.color,
        titleColor: item.color,
        bg: "rgba(255,255,255,0.98)",
      });
    }
    update();
  }

  function renderTimezoneDst(el) {
    const cases = [
      { label: "Spring 09:30 UTC", utc: "2024-03-10 09:30", local: "2024-03-10 01:30", note: "Before the DST jump" },
      { label: "Spring 10:30 UTC", utc: "2024-03-10 10:30", local: "2024-03-10 03:30", note: "02:xx local time is skipped" },
      { label: "Fall 08:30 UTC", utc: "2024-11-03 08:30", local: "2024-11-03 01:30", note: "First 01:30" },
      { label: "Fall 09:30 UTC", utc: "2024-11-03 09:30", local: "2024-11-03 01:30", note: "Second 01:30" },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const row = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      row.selectAll("*").remove();
      cases.forEach((item, i) => {
        chip(tabs, item.label, {
          clickable: true,
          bg: i === active ? PURPLE : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? PURPLE : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const item = cases[active];
      box(row, "UTC input", [{ text: item.utc, code: true }], { border: "#ddd" });
      box(row, "Los Angeles local", [{ text: item.local, code: true }, { text: item.note, color: GRAY }], {
        border: item.note.includes("skipped") ? RED : HIT_COLOR,
        titleColor: item.note.includes("skipped") ? RED : HIT_COLOR,
        bg: item.note.includes("skipped") ? "rgba(239,83,80,0.06)" : "rgba(38,166,154,0.06)",
      });
    }
    update();
  }

  function renderDateCast(el) {
    const cases = [
      { input: "TIMESTAMP '2024-07-19 23:15:00'", output: "2024-07-19", note: "Time-of-day is dropped." },
      { input: "DATE '2024-07-19'", output: "2024-07-19", note: "Already a date." },
      { input: "CURRENT_DATE()", output: "today", note: "Stable within a query." },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const panel = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      cases.forEach((item, i) => {
        chip(tabs, `DATE(${i === 2 ? "..." : item.input.split(" ")[0]})`, {
          clickable: true,
          bg: i === active ? TEAL : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? TEAL : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const item = cases[active];
      box(panel, "Input", [{ text: item.input, code: true }], { border: "#ddd" });
      box(panel, "DATE(...) result", [{ text: item.output, code: true }, { text: item.note, color: GRAY }], { border: TEAL, titleColor: TEAL, bg: "rgba(38,166,154,0.06)" });
    }
    update();
  }

  function renderDateAddSubtract(el) {
    const cases = [
      { label: "+ 1 day", input: "DATE '2016-07-30'", fn: "DATE_ADD", output: "2016-07-31", color: TEAL },
      { label: "- 1 day", input: "DATE '2016-07-30'", fn: "DATE_SUB", output: "2016-07-29", color: PURPLE },
      { label: "+ 1 month", input: "DATE '2024-01-31'", fn: "ADD_MONTHS", output: "2024-02-29", color: AMBER },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const panel = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      cases.forEach((item, i) => {
        chip(tabs, item.label, {
          clickable: true,
          bg: i === active ? item.color : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? item.color : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const item = cases[active];
      box(panel, item.fn, [{ text: item.input, code: true }], { border: "#ddd" });
      box(panel, "Result", [{ text: item.output, code: true }], { border: item.color, titleColor: item.color, bg: "rgba(255,255,255,0.98)" });
    }
    update();
  }

  function renderDateDifference(el) {
    const cases = [
      {
        label: "Cross midnight",
        lines: [
          "DATEDIFF = 1 calendar day",
          "TIMESTAMPDIFF(DAY) = 0 full days",
          "TIMESTAMPDIFF(HOUR) = 2 hours",
        ],
      },
      {
        label: "Month-end pair",
        lines: [
          "MONTHS_BETWEEN = 1.0",
          "Both dates are month-end values",
          "Spark treats them as a whole month apart",
        ],
      },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const panel = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(220px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      cases.forEach((item, i) => {
        chip(tabs, item.label, {
          clickable: true,
          bg: i === active ? PURPLE : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? PURPLE : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      box(panel, cases[active].label, cases[active].lines.map((text) => ({ text })), { border: PURPLE, titleColor: PURPLE, bg: "rgba(124,77,255,0.06)" });
    }
    update();
  }

  function renderDateParts(el) {
    const parts = [
      { label: "DAY", value: "19", note: "Day of month" },
      { label: "DAYOFWEEK", value: "6", note: "Friday when Sunday = 1" },
      { label: "DAYOFYEAR", value: "201", note: "Ordinal day in the year" },
      { label: "SECONDS", value: "1.000001", note: "Fractional seconds survive" },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const panel = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      parts.forEach((item, i) => {
        chip(tabs, item.label, {
          clickable: true,
          bg: i === active ? TEAL : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? TEAL : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const item = parts[active];
      box(panel, item.label, [{ text: item.value, code: true }, { text: item.note, color: GRAY }], { border: TEAL, titleColor: TEAL, bg: "rgba(38,166,154,0.06)" });
    }
    update();
  }

  function renderDateTrunc(el) {
    const sample = "2024-07-17 14:35:10.123456";
    const units = [
      { label: "MONTH", value: "2024-07-01 00:00:00" },
      { label: "WEEK", value: "2024-07-15 00:00:00" },
      { label: "DAY", value: "2024-07-17 00:00:00" },
      { label: "MILLISECOND", value: "2024-07-17 14:35:10.123" },
    ];
    let active = 0;
    const container = d3.select(el);
    container.selectAll("*").remove();
    container.append("div")
      .style("font-size", "0.72rem")
      .style("color", GRAY)
      .style("font-family", "monospace")
      .style("margin-bottom", "0.5rem")
      .text(`Input: ${sample}`);
    const tabs = container.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.6rem");
    const panel = container.append("div").style("display", "grid").style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))").style("gap", "10px");

    function update() {
      tabs.selectAll("*").remove();
      panel.selectAll("*").remove();
      units.forEach((item, i) => {
        chip(tabs, item.label, {
          clickable: true,
          bg: i === active ? AMBER : "#f5f5f7",
          fg: i === active ? "#fff" : FG,
          border: i === active ? AMBER : "#ddd",
        }).on("click", () => {
          active = i;
          update();
        });
      });
      const item = units[active];
      box(panel, `DATE_TRUNC('${item.label}', value)`, [{ text: item.value, code: true }], { border: AMBER, titleColor: AMBER, bg: "rgba(255,167,38,0.08)" });
    }
    update();
  }

  function init() {
    const specs = [
      ["viz-datetime-overview", renderDatetimeOverview],
      ["viz-interval-month-logic", renderIntervalMonthLogic],
      ["viz-timestamp-parse-flow", renderTimestampParseFlow],
      ["viz-timezone-dst", renderTimezoneDst],
      ["viz-date-cast", renderDateCast],
      ["viz-date-add-subtract", renderDateAddSubtract],
      ["viz-date-difference", renderDateDifference],
      ["viz-date-parts", renderDateParts],
      ["viz-date-trunc", renderDateTrunc],
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
