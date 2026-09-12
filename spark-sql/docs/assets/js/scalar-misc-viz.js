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
      .style("justify-content", "center")
      .style("padding", "5px 12px")
      .style("border-radius", "5px")
      .style("font-size", "0.74rem")
      .style("font-family", "monospace")
      .style("cursor", opts.clickable ? "pointer" : "default")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .style("opacity", opts.opacity != null ? opts.opacity : 1)
      .style("min-width", opts.minWidth || null)
      .text(text);
  }

  function box(parent, title, opts) {
    opts = opts || {};
    const wrap = parent.append("div")
      .style("border", `1px solid ${opts.border || "#e3e7ea"}`)
      .style("background", opts.bg || "#fff")
      .style("border-radius", "8px")
      .style("padding", "10px 12px")
      .style("margin-bottom", opts.marginBottom || "0.6rem");
    wrap.append("div")
      .style("font-size", "0.7rem")
      .style("font-weight", "700")
      .style("color", opts.titleColor || FG)
      .style("margin-bottom", "0.35rem")
      .text(title);
    return wrap;
  }

  function renderCountModes(el) {
    const rows = [1, 1, null, 2, null];
    let mode = "COUNT(*)";
    const modes = ["COUNT(*)", "COUNT(v)", "COUNT(DISTINCT v)", "count_if(v IS NULL)"];

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.7rem");

    const sample = box(container, "Sample rows", { bg: "#fafafa" });
    const sampleRow = sample.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap");
    const resultBox = box(container, "Included rows and result", { border: PURPLE });
    const includedRow = resultBox.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.45rem");
    const resultLine = resultBox.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("font-weight", "700");

    modes.forEach((m) => {
      ctrl.append("button")
        .attr("data-mode", m)
        .style("padding", "4px 10px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-size", "0.72rem")
        .style("font-family", "monospace")
        .on("click", () => { mode = m; update(); })
        .text(m);
    });

    rows.forEach((v, i) => {
      chip(sampleRow, `row${i + 1}:${v === null ? "NULL" : v}`, { minWidth: "86px" });
    });

    function evaluateRow(v) {
      if (mode === "COUNT(*)") return true;
      if (mode === "COUNT(v)") return v !== null;
      if (mode === "COUNT(DISTINCT v)") return v !== null;
      return v === null;
    }

    function update() {
      ctrl.selectAll("button").each(function () {
        const active = d3.select(this).attr("data-mode") === mode;
        d3.select(this)
          .style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });

      includedRow.selectAll("*").remove();
      const included = [];
      const distinct = [];
      rows.forEach((v, i) => {
        const include = evaluateRow(v);
        const key = v === null ? "NULL" : String(v);
        if (mode === "COUNT(DISTINCT v)" && include && !distinct.includes(key)) {
          distinct.push(key);
        }
        if (include) included.push({ i, v });
        chip(includedRow, `row${i + 1}:${key}`, {
          bg: include ? "rgba(38,166,154,0.12)" : "rgba(239,83,80,0.08)",
          fg: include ? HIT_COLOR : RED,
          border: include ? HIT_COLOR : RED,
          opacity: include ? 1 : 0.55,
          minWidth: "86px",
        });
      });

      let result = included.length;
      if (mode === "COUNT(DISTINCT v)") result = distinct.length;
      resultLine
        .style("color", mode === "count_if(v IS NULL)" ? AMBER : PURPLE)
        .text(`${mode} = ${result}` + (mode === "COUNT(DISTINCT v)" ? `  (distinct values: ${distinct.join(", ") || "—"})` : ""));
    }

    update();
  }

  function renderPredicateLogic(el) {
    let lhs = 2;
    let includeNull = true;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const top = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(220px, 1fr))")
      .style("gap", "12px");

    const inBox = box(top, "IN / NOT IN", { border: PURPLE });
    const nanBox = box(top, "ISNULL vs ISNAN", { border: TEAL });

    const lhsRow = inBox.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.45rem");
    [1, 2, null].forEach((v) => {
      chip(lhsRow, `lhs=${v === null ? "NULL" : v}`, { clickable: true, minWidth: "78px" })
        .on("click", () => { lhs = v; update(); });
    });

    const toggleRow = inBox.append("div").style("display", "flex").style("gap", "8px").style("align-items", "center").style("margin-bottom", "0.5rem");
    toggleRow.append("span").style("font-size", "0.74rem").style("color", FG).text("include NULL in list");
    toggleRow.append("button")
      .style("padding", "4px 10px")
      .style("border-radius", "5px")
      .style("cursor", "pointer")
      .style("font-size", "0.72rem")
      .style("font-family", "monospace")
      .on("click", () => { includeNull = !includeNull; update(); })
      .text("toggle");

    const listRow = inBox.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.45rem");
    const inLine = inBox.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("margin-bottom", "0.25rem");
    const notInLine = inBox.append("div").style("font-family", "monospace").style("font-size", "0.78rem");

    const samples = [
      { label: "NaN", isNull: false, isNan: true },
      { label: "NULL", isNull: true, isNan: false },
      { label: "42.0", isNull: false, isNan: false },
    ];
    const sampleRow = nanBox.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.5rem");
    const nanLines = nanBox.append("div").style("font-family", "monospace").style("font-size", "0.78rem").style("line-height", "1.7");

    function sqlIn(value, list) {
      if (value === null) return null;
      if (list.includes(value)) return true;
      return list.includes(null) ? null : false;
    }

    function sqlNotIn(value, list) {
      const res = sqlIn(value, list);
      return res === null ? null : !res;
    }

    function tone(line, value) {
      if (value === true) return line.style("color", HIT_COLOR);
      if (value === false) return line.style("color", RED);
      return line.style("color", AMBER);
    }

    function update() {
      lhsRow.selectAll("div").each(function (d, i) {
        const value = [1, 2, null][i];
        const active = lhs === value;
        d3.select(this)
          .style("background", active ? PURPLE : "#f5f5f7")
          .style("color", active ? "#fff" : FG)
          .style("border", `1.5px solid ${active ? PURPLE : "#ddd"}`);
      });

      listRow.selectAll("*").remove();
      const list = includeNull ? [1, null, 3] : [1, 3];
      list.forEach((v) => {
        chip(listRow, v === null ? "NULL" : String(v), {
          bg: v === null ? "rgba(255,167,38,0.12)" : "#f5f5f7",
          fg: v === null ? AMBER : FG,
          border: v === null ? AMBER : "#ddd",
        });
      });

      const inRes = sqlIn(lhs, list);
      const notInRes = sqlNotIn(lhs, list);
      tone(inLine, inRes).text(`${lhs === null ? "NULL" : lhs} IN (${list.map((v) => v === null ? "NULL" : v).join(", ")}) = ${inRes === null ? "NULL" : inRes}`);
      tone(notInLine, notInRes).text(`${lhs === null ? "NULL" : lhs} NOT IN (${list.map((v) => v === null ? "NULL" : v).join(", ")}) = ${notInRes === null ? "NULL" : notInRes}`);

      sampleRow.selectAll("*").remove();
      samples.forEach((s) => {
        chip(sampleRow, s.label, {
          bg: s.label === "NaN" ? "rgba(38,166,154,0.12)" : s.label === "NULL" ? "rgba(255,167,38,0.12)" : "#f5f5f7",
          fg: s.label === "NaN" ? HIT_COLOR : s.label === "NULL" ? AMBER : FG,
          border: s.label === "NaN" ? HIT_COLOR : s.label === "NULL" ? AMBER : "#ddd",
        });
      });
      nanLines.html(
        "ISNAN(NaN) = <span style='color:" + HIT_COLOR + "'>true</span><br>" +
        "ISNULL(NaN) = <span style='color:" + RED + "'>false</span><br>" +
        "ISNULL(NULL) = <span style='color:" + HIT_COLOR + "'>true</span><br>" +
        "ISNAN(NULL) = <span style='color:" + RED + "'>false</span>"
      );
    }

    update();
  }

  function renderBitwiseShift(el) {
    const base = -8;
    let shift = 1;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div")
      .style("display", "flex")
      .style("align-items", "center")
      .style("gap", "10px")
      .style("margin-bottom", "0.6rem");
    ctrl.append("span").style("font-size", "0.76rem").style("color", FG).style("font-weight", "700").text("shift amount");
    const label = ctrl.append("code").style("padding", "2px 8px").style("border-radius", "4px").style("background", "#eee").text(shift);
    ctrl.append("input")
      .attr("type", "range")
      .attr("min", 0)
      .attr("max", 6)
      .attr("value", shift)
      .style("width", "160px")
      .on("input", function () { shift = +this.value; update(); });

    const bitsBox = box(container, "32-bit view of -8", { bg: "#fafafa" });
    const baseBits = bitsBox.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-bottom", "0.35rem");
    const signedLine = bitsBox.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-bottom", "0.25rem");
    const unsignedLine = bitsBox.append("div").style("font-family", "monospace").style("font-size", "0.8rem");

    function bits(n) {
      const raw = (n >>> 0).toString(2).padStart(32, "0");
      return raw.replace(/(.{8})/g, "$1 ").trim();
    }

    function update() {
      label.text(shift);
      const signed = base >> shift;
      const unsigned = base >>> shift;
      baseBits.style("color", FG).text(`base                 ${bits(base)}   (${base})`);
      signedLine.style("color", PURPLE).text(`SHIFTRIGHT           ${bits(signed)}   (${signed})`);
      unsignedLine.style("color", TEAL).text(`SHIFTRIGHTUNSIGNED   ${bits(unsigned)}   (${unsigned})`);
    }

    update();
  }

  function renderAclContext(el) {
    let mode = "interactive";
    const contexts = {
      interactive: {
        label: "Interactive analyst",
        user: "alice@example.com",
        groups: ["finance-analysts", "bi-readers"],
      },
      job: {
        label: "Automated job",
        user: "svc-finance-etl",
        groups: ["etl-runners"],
      },
    };

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem");
    ["interactive", "job"].forEach((key) => {
      ctrl.append("button")
        .attr("data-mode", key)
        .style("padding", "4px 10px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-size", "0.72rem")
        .on("click", () => { mode = key; update(); })
        .text(contexts[key].label);
    });

    const panel = box(container, "Session context", { border: PURPLE });
    const who = panel.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-bottom", "0.35rem");
    const groupRow = panel.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "0.45rem");
    const checkLine = panel.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("margin-bottom", "0.2rem");
    const accessLine = panel.append("div").style("font-family", "monospace").style("font-size", "0.8rem").style("font-weight", "700");

    function update() {
      ctrl.selectAll("button").each(function () {
        const active = d3.select(this).attr("data-mode") === mode;
        d3.select(this)
          .style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });

      const ctx = contexts[mode];
      const allowed = ctx.groups.includes("finance-analysts");
      who.style("color", FG).text(`CURRENT_USER() = ${ctx.user}`);
      groupRow.selectAll("*").remove();
      ctx.groups.forEach((g) => {
        chip(groupRow, g, {
          bg: g === "finance-analysts" ? "rgba(38,166,154,0.12)" : "#f5f5f7",
          fg: g === "finance-analysts" ? HIT_COLOR : FG,
          border: g === "finance-analysts" ? HIT_COLOR : "#ddd",
        });
      });
      checkLine.style("color", allowed ? HIT_COLOR : RED).text(`IS_MEMBER('finance-analysts') = ${allowed}`);
      accessLine.style("color", allowed ? HIT_COLOR : RED).text(allowed ? "Row filter keeps finance rows" : "Row filter hides finance rows");
    }

    update();
  }

  function renderWebUrlParts(el) {
    const samples = {
      valid: "https://example.com:8443/api/v1?q=Spark+SQL&lang=en#intro",
      invalid: "not a url",
    };
    let mode = "valid";

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem");
    ["valid", "invalid"].forEach((key) => {
      ctrl.append("button")
        .attr("data-mode", key)
        .style("padding", "4px 10px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-size", "0.72rem")
        .on("click", () => { mode = key; update(); })
        .text(key === "valid" ? "Valid URL" : "Malformed URL");
    });

    const top = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(240px, 1fr))")
      .style("gap", "12px");

    const parseBox = box(top, "PARSE_URL / TRY_PARSE_URL", { border: PURPLE });
    const encodeBox = box(top, "URL_ENCODE / URL_DECODE", { border: TEAL });

    const source = parseBox.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("margin-bottom", "0.45rem").style("word-break", "break-all");
    const partLines = parseBox.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("line-height", "1.7");

    const phrase = "Spark SQL + D3.js";
    const encoded = encodeURIComponent(phrase).replace(/%20/g, "+");
    const decodeForm = (s) => decodeURIComponent(s.replace(/\+/g, "%20"));
    encodeBox.append("div").style("font-family", "monospace").style("font-size", "0.76rem").style("line-height", "1.8")
      .html(
        `URL_ENCODE('${phrase}') = <span style="color:${PURPLE}">${encoded}</span><br>` +
        `URL_DECODE('${encoded}') = <span style="color:${TEAL}">${decodeForm(encoded)}</span>`
      );

    function update() {
      ctrl.selectAll("button").each(function () {
        const active = d3.select(this).attr("data-mode") === mode;
        d3.select(this)
          .style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });

      const url = samples[mode];
      source.style("color", FG).text(url);
      if (mode === "valid") {
        partLines.html(
          `HOST = <span style="color:${HIT_COLOR}">example.com</span><br>` +
          `PATH = <span style="color:${PURPLE}">/api/v1</span><br>` +
          `QUERY:q = <span style="color:${AMBER}">Spark+SQL</span><br>` +
          `PARSE_URL(valid, 'HOST') = <span style="color:${HIT_COLOR}">example.com</span><br>` +
          `TRY_PARSE_URL(valid, 'HOST') = <span style="color:${HIT_COLOR}">example.com</span>`
        );
      } else {
        partLines.html(
          `PARSE_URL(invalid, 'HOST') = <span style="color:${RED}">error</span><br>` +
          `TRY_PARSE_URL(invalid, 'HOST') = <span style="color:${AMBER}">NULL</span>`
        );
      }
    }

    update();
  }

  function init() {
    const specs = [
      ["viz-count-modes", renderCountModes],
      ["viz-predicate-logic", renderPredicateLogic],
      ["viz-bitwise-shift", renderBitwiseShift],
      ["viz-acl-context", renderAclContext],
      ["viz-web-url-parts", renderWebUrlParts],
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
