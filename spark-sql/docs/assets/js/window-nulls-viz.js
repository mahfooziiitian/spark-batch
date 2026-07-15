/**
 * window-nulls-viz.js
 * D3 v7 interactive visualizations for NULL Options in Window Functions page.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-null-scan     — interactive NULL scan showing IGNORE vs RESPECT NULLS
 *   #viz-null-ordering — NULLS FIRST vs NULLS LAST sort comparison
 */
(function () {
  "use strict";

  const PURPLE = "#7c4dff";
  const AMBER = "#ffa726";
  const TEAL = "#26a69a";
  const RED = "#ef5350";
  const GRAY = "#90a4ae";
  const FG = "#546e7a";
  const NULL_BG = "rgba(239, 83, 80, 0.08)";
  const NULL_STRIPE = "rgba(239, 83, 80, 0.15)";
  const SCAN_COLOR = "#7c4dff";
  const SKIP_COLOR = "#ef5350";
  const HIT_COLOR = "#26a69a";

  function makeTooltip(parent) {
    const div = document.createElement("div");
    div.className = "ts-tooltip";
    div.style.cssText = "opacity:0;position:absolute;pointer-events:none;";
    parent.style.position = "relative";
    parent.appendChild(div);
    return {
      show(html, x, y) { div.innerHTML = html; div.style.opacity = 1; div.style.left = x + "px"; div.style.top = y + "px"; },
      hide() { div.style.opacity = 0; },
    };
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. NULL SCAN — Interactive IGNORE vs RESPECT NULLS
   *    Click a row to set CURRENT ROW. Toggle between functions
   *    (FIRST_VALUE, LAST_VALUE, LAG, LEAD). Shows scan direction
   *    with animated arrows, highlighting skipped NULLs.
   * ══════════════════════════════════════════════════════════════════ */
  function renderNullScan(el) {
    const W = Math.min(el.clientWidth || 720, 720);
    const colW = 72;
    const rowH = 34;
    const rowGap = 3;
    const headerH = 28;

    const data = [
      { date: "Jan 01", amount: 100 },
      { date: "Jan 02", amount: null },
      { date: "Jan 03", amount: 200 },
      { date: "Jan 04", amount: null },
      { date: "Jan 05", amount: 300 },
      { date: "Jan 06", amount: null },
      { date: "Jan 07", amount: 150 },
    ];

    let currentIdx = 4;
    let func = "LAG";
    let ignoreNulls = true;

    const container = d3.select(el);
    container.selectAll("*").remove();

    // Controls
    const ctrl = container.append("div")
      .style("display", "flex").style("align-items", "center")
      .style("gap", "0.6rem").style("margin-bottom", "0.6rem")
      .style("flex-wrap", "wrap").style("font-size", "0.8rem");

    // Function selector
    ctrl.append("span").style("color", FG).style("font-weight", "600").text("Function:");
    const funcs = ["LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE"];
    const funcBtns = funcs.map(f => {
      return ctrl.append("button")
        .style("padding", "4px 10px").style("border-radius", "4px")
        .style("border", "1px solid #ccc").style("font-size", "0.72rem")
        .style("cursor", "pointer").style("font-family", "monospace")
        .text(f)
        .on("click", () => { func = f; update(); });
    });

    ctrl.append("span").style("width", "8px");

    // Toggle
    const toggleWrap = ctrl.append("label")
      .style("display", "inline-flex").style("align-items", "center")
      .style("gap", "6px").style("cursor", "pointer").style("font-size", "0.78rem");
    const checkbox = toggleWrap.append("input").attr("type", "checkbox")
      .property("checked", ignoreNulls)
      .style("accent-color", TEAL)
      .on("change", function () { ignoreNulls = this.checked; update(); });
    toggleWrap.append("span").text("IGNORE NULLS");

    // Result display
    const resultDisp = ctrl.append("code")
      .style("padding", "4px 10px").style("border-radius", "4px")
      .style("font-size", "0.78rem").style("font-weight", "700");

    const tip = makeTooltip(el);

    // Table dimensions
    const cols = ["Row", "Date", "Amount", "Scan", "Result"];
    const tableW = colW * cols.length + 20;
    const tableH = headerH + data.length * (rowH + rowGap) + 30;

    const svg = container.append("svg")
      .attr("width", "100%").attr("height", tableH)
      .attr("viewBox", `0 0 ${Math.max(tableW, W)} ${tableH}`)
      .style("overflow", "visible");

    const g = svg.append("g").attr("transform", "translate(10, 0)");

    // Header
    cols.forEach((col, ci) => {
      g.append("text")
        .attr("x", ci * colW + colW / 2).attr("y", headerH / 2 + 4)
        .attr("text-anchor", "middle").attr("font-size", "0.68rem")
        .attr("font-weight", "700").attr("fill", FG)
        .text(col);
    });
    g.append("line")
      .attr("x1", 0).attr("y1", headerH).attr("x2", colW * cols.length).attr("y2", headerH)
      .attr("stroke", "#ccc");

    function computeResult() {
      const cur = currentIdx;
      if (func === "LAG") {
        for (let i = cur - 1; i >= 0; i--) {
          if (!ignoreNulls || data[i].amount !== null) return { value: data[i].amount, idx: i, scanned: range(i, cur) };
        }
        return { value: null, idx: -1, scanned: range(0, cur) };
      }
      if (func === "LEAD") {
        for (let i = cur + 1; i < data.length; i++) {
          if (!ignoreNulls || data[i].amount !== null) return { value: data[i].amount, idx: i, scanned: range(cur + 1, i + 1) };
        }
        return { value: null, idx: -1, scanned: range(cur + 1, data.length) };
      }
      if (func === "FIRST_VALUE") {
        for (let i = 0; i <= cur; i++) {
          if (!ignoreNulls || data[i].amount !== null) return { value: data[i].amount, idx: i, scanned: range(0, i + 1) };
        }
        return { value: null, idx: -1, scanned: range(0, cur + 1) };
      }
      if (func === "LAST_VALUE") {
        for (let i = data.length - 1; i >= 0; i--) {
          if (!ignoreNulls || data[i].amount !== null) return { value: data[i].amount, idx: i, scanned: range(i, data.length) };
        }
        return { value: null, idx: -1, scanned: range(0, data.length) };
      }
      return { value: null, idx: -1, scanned: [] };
    }

    function range(a, b) { const r = []; for (let i = a; i < b; i++) r.push(i); return r; }

    function update() {
      // Update button styles
      funcBtns.forEach((btn, i) => {
        const active = funcs[i] === func;
        btn.style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border-color", active ? PURPLE : "#ccc");
      });

      const result = computeResult();
      const resultVal = result.value !== null ? result.value : "NULL";
      resultDisp
        .style("background", result.value !== null ? "#e0f2f1" : "#fce4ec")
        .style("color", result.value !== null ? "#004d40" : "#b71c1c")
        .text(`→ ${resultVal}`);

      // Clear rows
      g.selectAll(".data-row").remove();

      data.forEach((d, i) => {
        const y = headerH + i * (rowH + rowGap) + 4;
        const rowG = g.append("g").attr("class", "data-row").style("cursor", "pointer");

        // NULL stripe background
        if (d.amount === null) {
          rowG.append("rect")
            .attr("x", 0).attr("y", y).attr("width", colW * cols.length).attr("height", rowH)
            .attr("fill", NULL_STRIPE).attr("rx", 3);
        }

        // Current row highlight
        if (i === currentIdx) {
          rowG.append("rect")
            .attr("x", 0).attr("y", y).attr("width", colW * cols.length).attr("height", rowH)
            .attr("fill", "rgba(255, 167, 38, 0.15)").attr("stroke", AMBER)
            .attr("stroke-width", 1.5).attr("rx", 3);
        }

        // Row index
        rowG.append("text")
          .attr("x", colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.68rem").attr("fill", GRAY)
          .text(i);

        // Date
        rowG.append("text")
          .attr("x", colW + colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.7rem").attr("fill", FG)
          .text(d.date);

        // Amount
        rowG.append("text")
          .attr("x", 2 * colW + colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.7rem")
          .attr("fill", d.amount === null ? RED : FG)
          .attr("font-style", d.amount === null ? "italic" : "normal")
          .text(d.amount === null ? "NULL" : d.amount);

        // Scan indicator
        const isScanned = result.scanned.includes(i);
        const isHit = result.idx === i;
        const isSkipped = isScanned && d.amount === null && ignoreNulls;
        const isCurrent = i === currentIdx;

        let scanText = "";
        let scanColor = GRAY;
        if (isCurrent) {
          scanText = "◉ current";
          scanColor = AMBER;
        } else if (isHit) {
          scanText = "✓ hit";
          scanColor = HIT_COLOR;
        } else if (isSkipped) {
          scanText = "⊘ skip";
          scanColor = SKIP_COLOR;
        } else if (isScanned) {
          scanText = "→ scan";
          scanColor = SCAN_COLOR;
        }

        rowG.append("text")
          .attr("x", 3 * colW + colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.65rem")
          .attr("fill", scanColor).attr("font-weight", isHit ? "700" : "400")
          .text(scanText);

        // Result column (only on current row)
        if (isCurrent) {
          rowG.append("text")
            .attr("x", 4 * colW + colW / 2).attr("y", y + rowH / 2 + 4)
            .attr("text-anchor", "middle").attr("font-size", "0.7rem")
            .attr("fill", result.value !== null ? HIT_COLOR : RED)
            .attr("font-weight", "700")
            .text(result.value !== null ? result.value : "NULL");
        }

        // Click to set current
        rowG.on("click", () => { currentIdx = i; update(); });

        rowG.on("mouseenter", function (ev) {
          let html = `<strong>${d.date}</strong> · Amount: <strong>${d.amount === null ? "NULL" : d.amount}</strong>`;
          if (isCurrent) html += `<br><span style="color:${AMBER}">◉ Current row</span>`;
          else if (isHit) html += `<br><span style="color:${HIT_COLOR}">✓ Found — returned by ${func}</span>`;
          else if (isSkipped) html += `<br><span style="color:${SKIP_COLOR}">⊘ Skipped (NULL, IGNORE NULLS)</span>`;
          else if (isScanned) html += `<br><span style="color:${SCAN_COLOR}">→ Scanned</span>`;
          tip.show(html, ev.offsetX + 14, ev.offsetY - 30);
        }).on("mouseleave", () => tip.hide());
      });

      // Scan direction arrow
      g.selectAll(".scan-arrow").remove();
      const arrowY = headerH + data.length * (rowH + rowGap) + 16;
      const dir = (func === "LAG" || func === "FIRST_VALUE") ? "← scans backward" :
                  (func === "LEAD" || func === "LAST_VALUE") ? "→ scans forward" : "";
      g.append("text").attr("class", "scan-arrow")
        .attr("x", colW * cols.length / 2).attr("y", arrowY)
        .attr("text-anchor", "middle").attr("font-size", "0.68rem").attr("fill", GRAY)
        .text(`${func}  ${dir}  |  ${ignoreNulls ? "IGNORE" : "RESPECT"} NULLS  |  Click a row to change CURRENT`);
    }

    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. NULL ORDERING — Side-by-side NULLS FIRST vs NULLS LAST
   *    Shows how NULL placement changes ROW_NUMBER / RANK results.
   * ══════════════════════════════════════════════════════════════════ */
  function renderNullOrdering(el) {
    const colW = 60;
    const rowH = 30;
    const rowGap = 2;
    const headerH = 26;

    const data = [
      { name: "Alice", score: 100 },
      { name: "Bob",   score: null },
      { name: "Carol", score: 200 },
      { name: "Dave",  score: null },
      { name: "Eve",   score: 300 },
      { name: "Frank", score: 150 },
    ];

    const container = d3.select(el);
    container.selectAll("*").remove();

    const wrapper = container.append("div")
      .style("display", "flex").style("gap", "1.5rem").style("flex-wrap", "wrap");

    function renderPanel(parent, title, nullsPos, color) {
      const panel = parent.append("div")
        .style("flex", "1").style("min-width", "280px");

      panel.append("div")
        .style("font-weight", "700").style("font-size", "0.82rem")
        .style("color", color).style("margin-bottom", "0.4rem")
        .style("text-align", "center")
        .text(title);

      // Sort data
      const sorted = data.slice().sort((a, b) => {
        const aNull = a.score === null;
        const bNull = b.score === null;
        if (aNull && bNull) return 0;
        if (aNull) return nullsPos === "FIRST" ? -1 : 1;
        if (bNull) return nullsPos === "FIRST" ? 1 : -1;
        return a.score - b.score;
      });

      const cols = ["Row#", "Name", "Score", "ROW_NUMBER"];
      const tableW = colW * cols.length;
      const tableH = headerH + sorted.length * (rowH + rowGap) + 10;

      const svg = panel.append("svg")
        .attr("width", "100%").attr("height", tableH)
        .attr("viewBox", `0 0 ${tableW + 10} ${tableH}`)
        .style("overflow", "visible");
      const g = svg.append("g").attr("transform", "translate(5, 0)");

      // Header
      cols.forEach((col, ci) => {
        g.append("text")
          .attr("x", ci * colW + colW / 2).attr("y", headerH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.64rem")
          .attr("font-weight", "700").attr("fill", FG)
          .text(col);
      });
      g.append("line")
        .attr("x1", 0).attr("y1", headerH).attr("x2", tableW).attr("y2", headerH)
        .attr("stroke", "#ddd");

      sorted.forEach((d, i) => {
        const y = headerH + i * (rowH + rowGap) + 4;
        const isNull = d.score === null;

        // NULL row background
        if (isNull) {
          g.append("rect")
            .attr("x", 0).attr("y", y).attr("width", tableW).attr("height", rowH)
            .attr("fill", NULL_STRIPE).attr("rx", 2);
        }

        // Position
        g.append("text")
          .attr("x", colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.66rem").attr("fill", GRAY)
          .text(i);

        // Name
        g.append("text")
          .attr("x", colW + colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.68rem").attr("fill", FG)
          .text(d.name);

        // Score
        g.append("text")
          .attr("x", 2 * colW + colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.68rem")
          .attr("fill", isNull ? RED : FG)
          .attr("font-style", isNull ? "italic" : "normal")
          .text(isNull ? "NULL" : d.score);

        // ROW_NUMBER
        g.append("text")
          .attr("x", 3 * colW + colW / 2).attr("y", y + rowH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.72rem")
          .attr("fill", isNull ? color : FG).attr("font-weight", isNull ? "700" : "400")
          .text(i + 1);
      });
    }

    renderPanel(wrapper, "ORDER BY score ASC NULLS FIRST", "FIRST", PURPLE);
    renderPanel(wrapper, "ORDER BY score ASC NULLS LAST", "LAST", TEAL);

    container.append("div")
      .style("text-align", "center").style("font-size", "0.68rem")
      .style("color", GRAY).style("margin-top", "0.4rem")
      .html("NULL placement changes physical row positions → different <strong>ROW_NUMBER</strong> assignments. " +
        "<span style='background:" + NULL_STRIPE + ";padding:2px 8px;border-radius:3px'>shaded</span> = NULL rows");
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const s = document.getElementById("viz-null-scan");
    if (s && !s.dataset.rendered) { renderNullScan(s); s.dataset.rendered = "1"; }
    const o = document.getElementById("viz-null-ordering");
    if (o && !o.dataset.rendered) { renderNullOrdering(o); o.dataset.rendered = "1"; }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
