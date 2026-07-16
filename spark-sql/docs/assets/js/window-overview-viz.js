/**
 * window-overview-viz.js
 * D3 v7 interactive visualization for Window Functions overview page.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-window-concept  — animated row-by-row window function computation
 *   #viz-partition-demo  — interactive PARTITION BY visualizer
 */
(function () {
  "use strict";

  const COLORS = {
    partition1: "#7c4dff",
    partition2: "#26a69a",
    partition3: "#ffa726",
    current: "#ef5350",
    frame: "rgba(124, 77, 255, 0.12)",
    frameBorder: "rgba(124, 77, 255, 0.45)",
    fg: "#546e7a",
    fgLight: "#90a4ae",
    highlight: "#7c4dff",
  };

  const PARTITIONS = ["North", "South"];
  const PARTITION_COLORS = { North: COLORS.partition1, South: COLORS.partition2 };

  const DATA = [
    { region: "North", rep: "Alice", date: "Jan 01", amount: 100 },
    { region: "North", rep: "Alice", date: "Jan 05", amount: 200 },
    { region: "North", rep: "Alice", date: "Jan 10", amount: 300 },
    { region: "North", rep: "Bob", date: "Jan 02", amount: 150 },
    { region: "North", rep: "Bob", date: "Jan 06", amount: 300 },
    { region: "South", rep: "Carol", date: "Jan 03", amount: 400 },
    { region: "South", rep: "Carol", date: "Jan 07", amount: 500 },
  ];

  function makeTooltip(parent) {
    const div = document.createElement("div");
    div.className = "ts-tooltip";
    div.style.cssText = "opacity:0;position:absolute;pointer-events:none;";
    parent.style.position = "relative";
    parent.appendChild(div);
    return {
      show(html, x, y) {
        div.innerHTML = html;
        div.style.opacity = 1;
        div.style.left = x + "px";
        div.style.top = y + "px";
      },
      hide() { div.style.opacity = 0; },
    };
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. WINDOW CONCEPT — Animated step-by-step computation
   *    Shows how a window function processes each row, highlighting
   *    the current row, its partition, and the computed result.
   * ══════════════════════════════════════════════════════════════════ */
  function renderWindowConcept(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 380;
    const m = { t: 10, r: 10, b: 10, l: 10 };

    // Sort data by region then date for display
    const sorted = [...DATA].sort((a, b) =>
      a.region.localeCompare(b.region) || a.date.localeCompare(b.date)
    );

    let currentIdx = 0;
    let funcType = "ROW_NUMBER";

    const container = d3.select(el);
    container.selectAll("*").remove();

    // Function selector
    const controls = container.append("div")
      .style("display", "flex").style("align-items", "center")
      .style("gap", "0.75rem").style("margin-bottom", "0.75rem")
      .style("flex-wrap", "wrap").style("font-size", "0.82rem");

    controls.append("span").text("Function:")
      .style("color", COLORS.fg).style("font-weight", "600");

    const funcs = ["ROW_NUMBER", "RANK", "SUM", "LAG"];
    const btnGroup = controls.append("div")
      .style("display", "flex").style("gap", "0.3rem");

    btnGroup.selectAll("button").data(funcs).enter()
      .append("button")
      .text(d => d)
      .style("padding", "4px 10px")
      .style("border", d => d === funcType ? `2px solid ${COLORS.highlight}` : "1px solid #ccc")
      .style("border-radius", "4px")
      .style("cursor", "pointer")
      .style("background", d => d === funcType ? "#f5f0ff" : "#fafafa")
      .style("font-size", "0.75rem")
      .style("font-weight", d => d === funcType ? "700" : "400")
      .style("color", d => d === funcType ? COLORS.highlight : COLORS.fg)
      .on("click", function (event, d) {
        funcType = d;
        btnGroup.selectAll("button")
          .style("border", f => f === d ? `2px solid ${COLORS.highlight}` : "1px solid #ccc")
          .style("background", f => f === d ? "#f5f0ff" : "#fafafa")
          .style("font-weight", f => f === d ? "700" : "400")
          .style("color", f => f === d ? COLORS.highlight : COLORS.fg);
        update();
      });

    // SQL display
    const sqlDisp = controls.append("code")
      .style("background", "#f5f0ff").style("padding", "4px 10px")
      .style("border-radius", "4px").style("font-size", "0.72rem")
      .style("color", COLORS.highlight).style("white-space", "nowrap")
      .style("margin-left", "auto");

    const tip = makeTooltip(el);
    const svg = container.append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const rowH = 36;
    const rowGap = 3;
    const colWidths = [0.07, 0.12, 0.12, 0.12, 0.12, 0.15, 0.30];
    const headers = ["#", "Region", "Rep", "Date", "Amount", "Result", "Explanation"];
    const iw = W - m.l - m.r;

    function colX(i) {
      let x = 0;
      for (let k = 0; k < i; k++) x += colWidths[k] * iw;
      return x;
    }

    // Header
    g.selectAll(".hdr").data(headers).enter().append("text")
      .attr("x", (_, i) => colX(i) + (colWidths[i] * iw) / 2)
      .attr("y", 16).attr("text-anchor", "middle")
      .attr("fill", COLORS.fg).attr("font-size", "0.7rem").attr("font-weight", "700")
      .text(d => d);

    // Click hint
    container.append("div")
      .style("font-size", "0.72rem").style("color", COLORS.fgLight)
      .style("margin-top", "0.35rem").style("text-align", "center")
      .text("Click any row to set it as the current row");

    function computeResults() {
      const results = [];
      for (let i = 0; i < sorted.length; i++) {
        const row = sorted[i];
        const partRows = sorted.filter(r => r.region === row.region);
        const idxInPart = partRows.indexOf(row);

        let val, explanation;
        switch (funcType) {
          case "ROW_NUMBER":
            val = idxInPart + 1;
            explanation = `Row ${idxInPart + 1} in '${row.region}' partition`;
            break;
          case "RANK": {
            let rank = 1;
            for (let j = 0; j < idxInPart; j++) {
              if (partRows[j].amount !== row.amount) rank = j + 1;
            }
            // Proper rank: count how many have smaller date (ordered by date)
            rank = idxInPart + 1;
            val = rank;
            explanation = `Rank ${rank} ordered by date in '${row.region}'`;
            break;
          }
          case "SUM": {
            let sum = 0;
            for (let j = 0; j <= idxInPart; j++) sum += partRows[j].amount;
            val = sum;
            explanation = `Running total: ${partRows.slice(0, idxInPart + 1).map(r => r.amount).join(" + ")} = ${sum}`;
            break;
          }
          case "LAG": {
            val = idxInPart > 0 ? partRows[idxInPart - 1].amount : "NULL";
            explanation = idxInPart > 0
              ? `Previous row amount in '${row.region}': ${val}`
              : `No previous row in '${row.region}' → NULL`;
            break;
          }
        }
        results.push({ ...row, result: val, explanation, partIdx: idxInPart });
      }
      return results;
    }

    function update() {
      const results = computeResults();
      const tableY = 30;

      // SQL text
      const sqlText = funcType === "SUM"
        ? `${funcType}(amount) OVER (PARTITION BY region ORDER BY date ROWS UNBOUNDED PRECEDING)`
        : funcType === "LAG"
          ? `${funcType}(amount) OVER (PARTITION BY region ORDER BY date)`
          : `${funcType}() OVER (PARTITION BY region ORDER BY date)`;
      sqlDisp.text(sqlText);

      // Remove old rows
      g.selectAll(".data-row").remove();

      results.forEach((row, i) => {
        const y = tableY + i * (rowH + rowGap);
        const isCurrent = i === currentIdx;
        const inSamePartition = row.region === results[currentIdx].region;

        const rowG = g.append("g").attr("class", "data-row")
          .style("cursor", "pointer")
          .on("click", () => { currentIdx = i; update(); })
          .on("mouseenter", function (event) {
            const msg = `<strong>${row.rep}</strong> — ${row.date}<br/>Amount: ${row.amount}<br/>Result: ${row.result}`;
            tip.show(msg, event.offsetX + 10, event.offsetY - 30);
          })
          .on("mouseleave", () => tip.hide());

        // Row background
        rowG.append("rect")
          .attr("x", 0).attr("y", y)
          .attr("width", iw).attr("height", rowH)
          .attr("rx", 4)
          .attr("fill", isCurrent ? COLORS.frame
            : inSamePartition ? "rgba(124, 77, 255, 0.04)" : "transparent")
          .attr("stroke", isCurrent ? COLORS.frameBorder : "none")
          .attr("stroke-width", isCurrent ? 2 : 0);

        // Partition color indicator
        rowG.append("rect")
          .attr("x", 0).attr("y", y)
          .attr("width", 4).attr("height", rowH)
          .attr("rx", 2)
          .attr("fill", PARTITION_COLORS[row.region]);

        // Row number
        const cellData = [
          i + 1, row.region, row.rep, row.date, row.amount,
          row.result, row.explanation,
        ];
        cellData.forEach((val, ci) => {
          const tx = colX(ci) + (colWidths[ci] * iw) / 2;
          rowG.append("text")
            .attr("x", tx).attr("y", y + rowH / 2 + 4)
            .attr("text-anchor", "middle")
            .attr("fill", ci === 5 && isCurrent ? COLORS.highlight
              : ci === 5 ? COLORS.fg
                : isCurrent ? COLORS.fg : COLORS.fgLight)
            .attr("font-size", ci === 6 ? "0.62rem" : "0.7rem")
            .attr("font-weight", isCurrent ? "700" : ci === 5 ? "600" : "400")
            .text(val);
        });

        // Current row marker
        if (isCurrent) {
          rowG.append("text")
            .attr("x", iw + 6).attr("y", y + rowH / 2 + 4)
            .attr("fill", COLORS.current).attr("font-size", "0.7rem")
            .attr("font-weight", "700")
            .text("◄ current");
        }
      });
    }

    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. PARTITION DEMO — Visual partition grouping
   *    Shows how PARTITION BY splits data into independent groups,
   *    with animated row highlighting and computed aggregates.
   * ══════════════════════════════════════════════════════════════════ */
  function renderPartitionDemo(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 320;
    const m = { t: 20, r: 20, b: 20, l: 20 };
    const iw = W - m.l - m.r;

    const container = d3.select(el);
    container.selectAll("*").remove();

    // Group data by partition
    const partitions = {};
    DATA.forEach(r => {
      if (!partitions[r.region]) partitions[r.region] = [];
      partitions[r.region].push(r);
    });
    // Sort each partition by date
    Object.values(partitions).forEach(p => p.sort((a, b) => a.date.localeCompare(b.date)));

    const partNames = Object.keys(partitions);
    const partW = (iw - 30) / partNames.length;
    const rowH = 42;
    const headerH = 50;

    const svg = container.append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    // Title
    g.append("text")
      .attr("x", iw / 2).attr("y", 14)
      .attr("text-anchor", "middle")
      .attr("fill", COLORS.fg).attr("font-size", "0.8rem").attr("font-weight", "700")
      .text("PARTITION BY region → independent groups");

    let animIdx = 0;
    let animTimer = null;

    partNames.forEach((pName, pi) => {
      const px = pi * (partW + 30);
      const rows = partitions[pName];
      const color = PARTITION_COLORS[pName];

      // Partition box
      g.append("rect")
        .attr("x", px).attr("y", headerH - 10)
        .attr("width", partW)
        .attr("height", rows.length * (rowH + 2) + 40)
        .attr("rx", 8)
        .attr("fill", "none")
        .attr("stroke", color)
        .attr("stroke-width", 2)
        .attr("stroke-dasharray", "6,3")
        .attr("opacity", 0.6);

      // Partition label
      g.append("text")
        .attr("x", px + partW / 2).attr("y", headerH + 8)
        .attr("text-anchor", "middle")
        .attr("fill", color).attr("font-size", "0.78rem").attr("font-weight", "700")
        .text(`${pName} (${rows.length} rows)`);

      // Column headers
      const cols = ["Rep", "Date", "Amount"];
      const cw = partW / 3;
      cols.forEach((c, ci) => {
        g.append("text")
          .attr("x", px + ci * cw + cw / 2).attr("y", headerH + 28)
          .attr("text-anchor", "middle")
          .attr("fill", COLORS.fgLight).attr("font-size", "0.65rem").attr("font-weight", "600")
          .text(c);
      });

      // Data rows
      rows.forEach((row, ri) => {
        const ry = headerH + 38 + ri * (rowH + 2);
        const rowG = g.append("g").attr("class", `part-row part-${pName}-${ri}`);

        rowG.append("rect")
          .attr("x", px + 4).attr("y", ry)
          .attr("width", partW - 8).attr("height", rowH)
          .attr("rx", 4)
          .attr("fill", "transparent")
          .attr("class", "row-bg");

        const vals = [row.rep, row.date, row.amount];
        vals.forEach((v, ci) => {
          rowG.append("text")
            .attr("x", px + ci * cw + cw / 2).attr("y", ry + rowH / 2 + 4)
            .attr("text-anchor", "middle")
            .attr("fill", COLORS.fg).attr("font-size", "0.7rem")
            .text(v);
        });
      });

      // Running total annotation
      const total = rows.reduce((s, r) => s + r.amount, 0);
      const bottomY = headerH + 38 + rows.length * (rowH + 2) + 6;
      g.append("text")
        .attr("x", px + partW / 2).attr("y", bottomY)
        .attr("text-anchor", "middle")
        .attr("fill", color).attr("font-size", "0.7rem").attr("font-weight", "600")
        .text(`SUM = ${total}`);
    });

    // Animate: cycle through rows to show "current row" scan
    function animate() {
      const allRows = g.selectAll(".row-bg");
      const totalRows = DATA.length;

      allRows.attr("fill", "transparent");

      // Highlight current
      let count = 0;
      for (const pName of partNames) {
        const rows = partitions[pName];
        for (let ri = 0; ri < rows.length; ri++) {
          if (count === animIdx) {
            g.select(`.part-${pName}-${ri} .row-bg`)
              .attr("fill", COLORS.frame)
              .attr("stroke", COLORS.frameBorder)
              .attr("stroke-width", 1.5);
          }
          count++;
        }
      }

      animIdx = (animIdx + 1) % totalRows;
    }

    animate();
    animTimer = setInterval(animate, 1200);

    // Stop animation when element is removed
    const observer = new MutationObserver(() => {
      if (!document.contains(el)) {
        clearInterval(animTimer);
        observer.disconnect();
      }
    });
    observer.observe(el.parentNode || document.body, { childList: true, subtree: true });
  }

  /* ── Bootstrap ─────────────────────────────────────────────────── */
  function init() {
    const el1 = document.getElementById("viz-window-concept");
    if (el1 && !el1.dataset.loaded) {
      el1.dataset.loaded = "1";
      renderWindowConcept(el1);
    }
    const el2 = document.getElementById("viz-partition-demo");
    if (el2 && !el2.dataset.loaded) {
      el2.dataset.loaded = "1";
      renderPartitionDemo(el2);
    }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
