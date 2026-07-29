/**
 * window-frame-viz.js
 * D3 v7 interactive visualization for Window Frame Specification page.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-frame-rows   — interactive ROWS frame slider
 *   #viz-frame-range  — ROWS vs RANGE side-by-side comparison
 */
(function () {
  "use strict";

  const C = ["#7c4dff", "#ffa726", "#26a69a", "#ef5350", "#ab47bc", "#29b6f6"];
  const GRAY = "#90a4ae";
  const FG = "#546e7a";
  const HIGHLIGHT = "#7c4dff";
  const FRAME_BG = "rgba(124, 77, 255, 0.10)";
  const FRAME_BORDER = "rgba(124, 77, 255, 0.40)";
  const CURRENT_COLOR = "#ffa726";

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
   * 1. ROWS FRAME — Interactive slider
   *    Click a row to set it as "current". Use preceding/following
   *    controls to adjust frame boundaries. Shows SUM result.
   * ══════════════════════════════════════════════════════════════════ */
  function renderRowsFrame(el) {
    const W = Math.min(el.clientWidth || 720, 720), H = 340;
    const m = { t: 20, r: 20, b: 20, l: 20 };
    const iw = W - m.l - m.r;

    const data = [
      { date: "Jan 01", amount: 100 },
      { date: "Jan 03", amount: 200 },
      { date: "Jan 05", amount: 150 },
      { date: "Jan 05", amount: 300 },
      { date: "Jan 07", amount: 400 },
      { date: "Jan 09", amount: 250 },
      { date: "Jan 12", amount: 180 },
    ];

    let currentIdx = 3;
    let preceding = 2;
    let following = 1;

    const container = d3.select(el);
    container.selectAll("*").remove();

    // Controls
    const controls = container.append("div")
      .style("display", "flex").style("align-items", "center")
      .style("gap", "1rem").style("margin-bottom", "0.75rem")
      .style("flex-wrap", "wrap").style("font-size", "0.82rem");

    function addControl(label, value, onChange) {
      const grp = controls.append("div")
        .style("display", "flex").style("align-items", "center").style("gap", "0.35rem");
      grp.append("span").text(label).style("color", FG).style("font-weight", "600");
      const btn_minus = grp.append("button").text("−")
        .style("width", "26px").style("height", "26px").style("border", "1px solid #ccc")
        .style("border-radius", "4px").style("cursor", "pointer").style("background", "#fafafa")
        .style("font-size", "0.9rem").style("line-height", "1");
      const val = grp.append("span").text(value)
        .style("min-width", "18px").style("text-align", "center").style("font-weight", "700")
        .style("color", HIGHLIGHT);
      const btn_plus = grp.append("button").text("+")
        .style("width", "26px").style("height", "26px").style("border", "1px solid #ccc")
        .style("border-radius", "4px").style("cursor", "pointer").style("background", "#fafafa")
        .style("font-size", "0.9rem").style("line-height", "1");
      btn_minus.on("click", () => { onChange(-1); val.text(onChange.current()); update(); });
      btn_plus.on("click", () => { onChange(1); val.text(onChange.current()); update(); });
      return val;
    }

    const precCtrl = { current: () => preceding };
    const folCtrl = { current: () => following };
    const precVal = addControl("PRECEDING:", preceding, Object.assign(function (d) {
      preceding = Math.max(0, Math.min(6, preceding + d));
    }, precCtrl));
    const folVal = addControl("FOLLOWING:", following, Object.assign(function (d) {
      following = Math.max(0, Math.min(6, following + d));
    }, folCtrl));

    // Frame SQL display
    const sqlDisp = controls.append("code")
      .style("background", "#f5f0ff").style("padding", "4px 10px")
      .style("border-radius", "4px").style("font-size", "0.75rem")
      .style("color", HIGHLIGHT).style("white-space", "nowrap");

    const tip = makeTooltip(el);
    const svg = container.append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const rowH = 38;
    const rowGap = 4;
    const colW = iw / 5;
    const tableY = 20;

    // Header
    const headers = ["Row", "Date", "Amount", "In Frame?", "SUM"];
    g.selectAll(".hdr").data(headers).enter().append("text")
      .attr("x", (_, i) => i * colW + colW / 2)
      .attr("y", tableY).attr("text-anchor", "middle")
      .attr("fill", FG).attr("font-size", "0.72rem").attr("font-weight", "700")
      .text(d => d);

    function update() {
      const frameStart = Math.max(0, currentIdx - preceding);
      const frameEnd = Math.min(data.length - 1, currentIdx + following);

      sqlDisp.text(`ROWS BETWEEN ${preceding} PRECEDING AND ${following} FOLLOWING`);
      precVal.text(preceding);
      folVal.text(following);

      const frameSum = data.slice(frameStart, frameEnd + 1).reduce((s, d) => s + d.amount, 0);

      // Rows
      const rows = g.selectAll(".data-row").data(data);
      const rowsEnter = rows.enter().append("g").attr("class", "data-row");
      const rowsMerge = rowsEnter.merge(rows);

      // Background rect
      rowsEnter.append("rect")
        .attr("x", 0).attr("width", iw).attr("height", rowH)
        .attr("rx", 4).style("cursor", "pointer");
      rowsMerge.select("rect")
        .attr("y", (_, i) => tableY + 14 + i * (rowH + rowGap))
        .attr("fill", (_, i) => {
          if (i === currentIdx) return "rgba(255, 167, 38, 0.18)";
          if (i >= frameStart && i <= frameEnd) return FRAME_BG;
          return "transparent";
        })
        .attr("stroke", (_, i) => {
          if (i === currentIdx) return CURRENT_COLOR;
          if (i >= frameStart && i <= frameEnd) return FRAME_BORDER;
          return "transparent";
        })
        .attr("stroke-width", (_, i) => i === currentIdx ? 2 : 1);

      // Row number
      rowsEnter.append("text").attr("class", "col-row");
      rowsMerge.select(".col-row")
        .attr("x", colW / 2)
        .attr("y", (_, i) => tableY + 14 + i * (rowH + rowGap) + rowH / 2 + 4)
        .attr("text-anchor", "middle").attr("font-size", "0.78rem")
        .attr("fill", (_, i) => i === currentIdx ? CURRENT_COLOR : FG)
        .attr("font-weight", (_, i) => i === currentIdx ? "700" : "400")
        .text((_, i) => i === currentIdx ? `→ ${i + 1}` : i + 1);

      // Date
      rowsEnter.append("text").attr("class", "col-date");
      rowsMerge.select(".col-date")
        .attr("x", colW + colW / 2)
        .attr("y", (_, i) => tableY + 14 + i * (rowH + rowGap) + rowH / 2 + 4)
        .attr("text-anchor", "middle").attr("font-size", "0.78rem").attr("fill", FG)
        .text(d => d.date);

      // Amount
      rowsEnter.append("text").attr("class", "col-amt");
      rowsMerge.select(".col-amt")
        .attr("x", 2 * colW + colW / 2)
        .attr("y", (_, i) => tableY + 14 + i * (rowH + rowGap) + rowH / 2 + 4)
        .attr("text-anchor", "middle").attr("font-size", "0.78rem").attr("fill", FG)
        .text(d => d.amount);

      // In Frame?
      rowsEnter.append("text").attr("class", "col-frame");
      rowsMerge.select(".col-frame")
        .attr("x", 3 * colW + colW / 2)
        .attr("y", (_, i) => tableY + 14 + i * (rowH + rowGap) + rowH / 2 + 4)
        .attr("text-anchor", "middle").attr("font-size", "0.82rem")
        .attr("fill", (_, i) => (i >= frameStart && i <= frameEnd) ? HIGHLIGHT : GRAY)
        .text((_, i) => (i >= frameStart && i <= frameEnd) ? "✓" : "—");

      // SUM
      rowsEnter.append("text").attr("class", "col-sum");
      rowsMerge.select(".col-sum")
        .attr("x", 4 * colW + colW / 2)
        .attr("y", (_, i) => tableY + 14 + i * (rowH + rowGap) + rowH / 2 + 4)
        .attr("text-anchor", "middle").attr("font-size", "0.78rem")
        .attr("fill", (_, i) => i === currentIdx ? HIGHLIGHT : "transparent")
        .attr("font-weight", "700")
        .text((_, i) => i === currentIdx ? frameSum : "");

      // Click handler
      rowsMerge.on("click", (_, i) => {
        currentIdx = data.indexOf(_);
        update();
      }).on("mouseenter", function (ev, d) {
        const i = data.indexOf(d);
        const inFrame = i >= frameStart && i <= frameEnd;
        tip.show(
          `<strong>Row ${i + 1}</strong> · ${d.date}<br>` +
          `Amount: <strong>${d.amount}</strong><br>` +
          (inFrame ? `<span style="color:${HIGHLIGHT}">In frame</span>` : `<span style="color:${GRAY}">Outside frame</span>`),
          ev.offsetX + 12, ev.offsetY - 30
        );
      }).on("mouseleave", () => tip.hide());

      // Frame bracket
      g.selectAll(".frame-bracket").remove();
      const bracketX = iw + 2;
      const y1 = tableY + 14 + frameStart * (rowH + rowGap) + 2;
      const y2 = tableY + 14 + frameEnd * (rowH + rowGap) + rowH - 2;

      // Legend
      g.selectAll(".legend").remove();
      g.append("text").attr("class", "legend")
        .attr("x", iw / 2).attr("y", tableY + 14 + data.length * (rowH + rowGap) + 16)
        .attr("text-anchor", "middle").attr("font-size", "0.7rem").attr("fill", GRAY)
        .text("Click a row to set it as CURRENT ROW. Use ± buttons to adjust frame.");
    }

    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. ROWS vs RANGE — Side-by-side comparison
   *    Shows how ties are handled differently.
   * ══════════════════════════════════════════════════════════════════ */
  function renderRowsVsRange(el) {
    const W = Math.min(el.clientWidth || 720, 720), H = 300;
    const m = { t: 10, r: 10, b: 10, l: 10 };

    const data = [
      { date: "Jan 01", amount: 100 },
      { date: "Jan 03", amount: 200 },
      { date: "Jan 05", amount: 150 },
      { date: "Jan 05", amount: 300 },
      { date: "Jan 07", amount: 400 },
    ];

    const container = d3.select(el);
    container.selectAll("*").remove();

    const wrapper = container.append("div")
      .style("display", "flex").style("gap", "1rem").style("flex-wrap", "wrap");

    function renderPanel(parent, title, mode) {
      const panel = parent.append("div")
        .style("flex", "1").style("min-width", "300px");

      panel.append("div")
        .style("font-weight", "700").style("font-size", "0.82rem")
        .style("color", mode === "ROWS" ? HIGHLIGHT : C[2])
        .style("margin-bottom", "0.5rem").style("text-align", "center")
        .text(title);

      const panelEl = panel.node();
      const tip = makeTooltip(panelEl);
      const colW = 60;
      const rowH = 32;
      const tableW = colW * 4;

      const svg = panel.append("svg")
        .attr("width", "100%").attr("height", 240)
        .attr("viewBox", `0 0 ${tableW + 20} 240`).style("overflow", "visible");
      const g = svg.append("g").attr("transform", "translate(10, 10)");

      // Headers
      const hdrs = ["Date", "Amount", "SUM", "Frame"];
      g.selectAll(".hdr").data(hdrs).enter().append("text")
        .attr("x", (_, i) => i * colW + colW / 2)
        .attr("y", 12).attr("text-anchor", "middle")
        .attr("fill", FG).attr("font-size", "0.68rem").attr("font-weight", "700")
        .text(d => d);

      // Compute running sums
      let runningSums;
      if (mode === "ROWS") {
        runningSums = [];
        let s = 0;
        data.forEach(d => { s += d.amount; runningSums.push(s); });
      } else {
        // RANGE: ties get same sum (all values <= current ORDER BY value)
        runningSums = data.map((d, i) => {
          const curDate = d.date;
          let s = 0;
          data.forEach(dd => { if (dd.date <= curDate) s += dd.amount; });
          return s;
        });
      }

      // Frame membership per row when current = each row
      function getFrameRows(currentIdx) {
        if (mode === "ROWS") {
          return data.map((_, i) => i <= currentIdx);
        } else {
          const curDate = data[currentIdx].date;
          return data.map(d => d.date <= curDate);
        }
      }

      let activeRow = 2; // Start with first Jan 05

      function draw() {
        const inFrame = getFrameRows(activeRow);
        g.selectAll(".drow").remove();

        data.forEach((d, i) => {
          const y = 24 + i * (rowH + 3);
          const rowG = g.append("g").attr("class", "drow").style("cursor", "pointer");

          // Background
          rowG.append("rect")
            .attr("x", 0).attr("y", y).attr("width", colW * 4).attr("height", rowH)
            .attr("rx", 3)
            .attr("fill", i === activeRow ? "rgba(255, 167, 38, 0.18)" :
              inFrame[i] ? (mode === "ROWS" ? "rgba(124, 77, 255, 0.08)" : "rgba(38, 166, 154, 0.08)") : "transparent")
            .attr("stroke", i === activeRow ? CURRENT_COLOR :
              inFrame[i] ? (mode === "ROWS" ? FRAME_BORDER : "rgba(38, 166, 154, 0.40)") : "transparent")
            .attr("stroke-width", i === activeRow ? 2 : 1);

          const color = mode === "ROWS" ? HIGHLIGHT : C[2];

          // Date
          rowG.append("text")
            .attr("x", colW / 2).attr("y", y + rowH / 2 + 4)
            .attr("text-anchor", "middle").attr("font-size", "0.72rem").attr("fill", FG)
            .text(d.date);

          // Amount
          rowG.append("text")
            .attr("x", colW + colW / 2).attr("y", y + rowH / 2 + 4)
            .attr("text-anchor", "middle").attr("font-size", "0.72rem").attr("fill", FG)
            .text(d.amount);

          // Running SUM
          rowG.append("text")
            .attr("x", 2 * colW + colW / 2).attr("y", y + rowH / 2 + 4)
            .attr("text-anchor", "middle").attr("font-size", "0.72rem")
            .attr("fill", i === activeRow ? color : FG)
            .attr("font-weight", i === activeRow ? "700" : "400")
            .text(runningSums[i]);

          // In frame marker
          rowG.append("text")
            .attr("x", 3 * colW + colW / 2).attr("y", y + rowH / 2 + 4)
            .attr("text-anchor", "middle").attr("font-size", "0.78rem")
            .attr("fill", inFrame[i] ? color : GRAY)
            .text(inFrame[i] ? "✓" : "—");

          rowG.on("click", () => { activeRow = i; draw(); });
        });

        // Difference callout for tied rows
        if (mode === "RANGE" && (activeRow === 2 || activeRow === 3)) {
          g.selectAll(".callout").remove();
          const cy = 24 + 2 * (rowH + 3) + rowH + 3 + rowH / 2;
          g.append("text").attr("class", "callout")
            .attr("x", colW * 2).attr("y", 24 + data.length * (rowH + 3) + 18)
            .attr("text-anchor", "middle").attr("font-size", "0.65rem")
            .attr("fill", C[2]).attr("font-style", "italic")
            .text("↑ Tied dates — same SUM in RANGE mode");
        } else {
          g.selectAll(".callout").remove();
        }
      }

      draw();
    }

    renderPanel(wrapper, "ROWS (physical position)", "ROWS");
    renderPanel(wrapper, "RANGE (value-based)", "RANGE");

    // Legend
    container.append("div")
      .style("text-align", "center").style("font-size", "0.7rem")
      .style("color", GRAY).style("margin-top", "0.5rem")
      .text("Click a row to set it as CURRENT ROW and see how the frame differs.");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. RANGE TIMELINE — Interactive date-axis visualization
   *    Click a dot to set CURRENT ROW. Use slider to adjust INTERVAL
   *    days PRECEDING. Frame highlights on timeline + shows SUM.
   * ══════════════════════════════════════════════════════════════════ */
  function renderRangeTimeline(el) {
    const W = Math.min(el.clientWidth || 720, 720), H = 280;
    const m = { t: 30, r: 30, b: 60, l: 50 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { day: 1,  label: "Jan 01", amount: 100 },
      { day: 2,  label: "Jan 02", amount: 175 },
      { day: 3,  label: "Jan 03", amount: 200 },
      { day: 5,  label: "Jan 05", amount: 150 },
      { day: 5,  label: "Jan 05", amount: 300 },
      { day: 7,  label: "Jan 07", amount: 400 },
      { day: 10, label: "Jan 10", amount: 250 },
      { day: 12, label: "Jan 12", amount: 180 },
      { day: 14, label: "Jan 14", amount: 350 },
    ];

    let currentIdx = 5; // Jan 07
    let rangeDays = 6;

    const container = d3.select(el);
    container.selectAll("*").remove();

    // Controls
    const ctrl = container.append("div")
      .style("display", "flex").style("align-items", "center")
      .style("gap", "0.75rem").style("margin-bottom", "0.5rem")
      .style("flex-wrap", "wrap").style("font-size", "0.82rem");

    ctrl.append("span").text("INTERVAL").style("color", FG).style("font-weight", "600");
    const slider = ctrl.append("input").attr("type", "range")
      .attr("min", 1).attr("max", 13).attr("value", rangeDays)
      .style("width", "140px").style("accent-color", C[2]);
    const dayLabel = ctrl.append("code")
      .style("background", "#e0f2f1").style("padding", "3px 8px")
      .style("border-radius", "4px").style("font-size", "0.78rem")
      .style("color", C[2]).style("min-width", "220px");

    const sumDisp = ctrl.append("span")
      .style("font-weight", "700").style("color", HIGHLIGHT)
      .style("font-size", "0.82rem");

    const tip = makeTooltip(el);

    const svg = container.append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const x = d3.scaleLinear().domain([0, 15]).range([0, iw]);
    const y = d3.scaleLinear().domain([0, 450]).range([ih, 0]);

    // X axis
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues([1,2,3,5,7,10,12,14])
        .tickFormat(d => {
          const labels = {1:"Jan 01",2:"Jan 02",3:"Jan 03",5:"Jan 05",7:"Jan 07",10:"Jan 10",12:"Jan 12",14:"Jan 14"};
          return labels[d] || "";
        }))
      .selectAll("text").attr("font-size", "0.65rem").attr("transform", "rotate(-30)").attr("text-anchor", "end");

    // Y axis
    g.append("g").call(d3.axisLeft(y).ticks(5).tickSize(-iw).tickFormat(d => d))
      .selectAll(".tick line").attr("stroke", "#e8e8e8");
    g.selectAll(".domain").attr("stroke", "#ccc");

    // Y label
    g.append("text").attr("transform", "rotate(-90)")
      .attr("x", -ih/2).attr("y", -38).attr("text-anchor", "middle")
      .attr("font-size", "0.7rem").attr("fill", GRAY).text("Amount");

    // Frame rect (will be updated)
    const frameRect = g.append("rect")
      .attr("y", 0).attr("height", ih)
      .attr("fill", "rgba(38, 166, 154, 0.10)")
      .attr("stroke", "rgba(38, 166, 154, 0.35)")
      .attr("stroke-width", 1.5)
      .attr("stroke-dasharray", "4,3")
      .attr("rx", 4);

    // Frame labels
    const frameLabelLeft = g.append("text")
      .attr("y", -8).attr("font-size", "0.62rem").attr("fill", C[2]).attr("text-anchor", "middle");
    const frameLabelRight = g.append("text")
      .attr("y", -8).attr("font-size", "0.62rem").attr("fill", CURRENT_COLOR).attr("text-anchor", "middle");

    // Dots (stagger tied dates vertically)
    const dotGroups = g.selectAll(".dot-g").data(data).enter().append("g").attr("class", "dot-g");

    // For tied dates, offset y slightly
    const tieOffset = {};
    data.forEach((d, i) => {
      if (!tieOffset[d.day]) tieOffset[d.day] = [];
      tieOffset[d.day].push(i);
    });

    const dots = dotGroups.append("circle")
      .attr("cx", d => x(d.day))
      .attr("cy", d => y(d.amount))
      .attr("r", 7)
      .attr("stroke-width", 2)
      .style("cursor", "pointer");

    // Amount labels on dots
    dotGroups.append("text")
      .attr("x", d => x(d.day))
      .attr("y", d => y(d.amount) - 12)
      .attr("text-anchor", "middle")
      .attr("font-size", "0.62rem")
      .attr("fill", FG)
      .text(d => d.amount);

    function update() {
      const cur = data[currentIdx];
      const frameStart = cur.day - rangeDays;

      dayLabel.text(`RANGE BETWEEN INTERVAL '${rangeDays}' DAY PRECEDING AND CURRENT ROW`);

      // Determine in-frame
      const inFrame = data.map(d => d.day >= frameStart && d.day <= cur.day);
      const frameSum = data.filter((_, i) => inFrame[i]).reduce((s, d) => s + d.amount, 0);
      const frameCount = inFrame.filter(Boolean).length;

      sumDisp.text(`SUM = ${frameSum}  (${frameCount} rows)`);

      // Update frame rect
      const xStart = Math.max(x(frameStart), 0);
      const xEnd = x(cur.day);
      frameRect.attr("x", xStart).attr("width", Math.max(xEnd - xStart, 2));

      frameLabelLeft.attr("x", xStart).text(frameStart >= 1 ? `Jan ${String(frameStart).padStart(2, "0")}` : "←");
      frameLabelRight.attr("x", xEnd).text(cur.label);

      // Update dots
      dots
        .attr("fill", (d, i) => i === currentIdx ? CURRENT_COLOR :
          inFrame[i] ? C[2] : "#e0e0e0")
        .attr("stroke", (d, i) => i === currentIdx ? "#e65100" :
          inFrame[i] ? "#00897b" : "#bdbdbd")
        .attr("r", (d, i) => i === currentIdx ? 9 : 7);
    }

    // Click to change current
    dotGroups.on("click", function (ev, d) {
      currentIdx = data.indexOf(d);
      update();
    }).on("mouseenter", function (ev, d) {
      const i = data.indexOf(d);
      const cur = data[currentIdx];
      const inF = d.day >= (cur.day - rangeDays) && d.day <= cur.day;
      const dist = Math.abs(cur.day - d.day);
      tip.show(
        `<strong>${d.label}</strong> · Amount: <strong>${d.amount}</strong><br>` +
        `Distance from current: ${dist} day${dist !== 1 ? "s" : ""}<br>` +
        (i === currentIdx ? `<span style="color:${CURRENT_COLOR}">◉ Current row</span>` :
          inF ? `<span style="color:${C[2]}">✓ In frame (within ${rangeDays}d)</span>` :
          `<span style="color:${GRAY}">✗ Outside frame</span>`),
        ev.offsetX + 14, ev.offsetY - 40
      );
    }).on("mouseleave", () => tip.hide());

    slider.on("input", function () {
      rangeDays = +this.value;
      update();
    });

    update();

    // Legend
    container.append("div")
      .style("text-align", "center").style("font-size", "0.68rem")
      .style("color", GRAY).style("margin-top", "0.3rem")
      .html("Click a dot to set <strong>CURRENT ROW</strong>. Drag the slider to change the INTERVAL. " +
        "<span style='color:" + C[2] + "'>●</span> in frame  " +
        "<span style='color:" + CURRENT_COLOR + "'>●</span> current  " +
        "<span style='color:#bdbdbd'>●</span> outside");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. ROWS BAR CHART — Interactive bar chart for ROWS frame page
   *    Click a bar to set CURRENT ROW. Use ± sliders for preceding
   *    and following offsets. Shows frame boundaries + SUM/AVG.
   * ══════════════════════════════════════════════════════════════════ */
  function renderRowsBarChart(el) {
    const W = Math.min(el.clientWidth || 720, 720), H = 320;
    const m = { t: 30, r: 20, b: 50, l: 50 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { date: "Jan 01", amount: 100 },
      { date: "Jan 02", amount: 200 },
      { date: "Jan 03", amount: 150 },
      { date: "Jan 04", amount: 300 },
      { date: "Jan 05", amount: 250 },
      { date: "Jan 06", amount: 175 },
      { date: "Jan 07", amount: 400 },
      { date: "Jan 08", amount: 220 },
    ];

    let currentIdx = 3;
    let preceding = 2;
    let following = 1;

    const container = d3.select(el);
    container.selectAll("*").remove();

    // Controls row
    const ctrl = container.append("div")
      .style("display", "flex").style("align-items", "center")
      .style("gap", "0.6rem").style("margin-bottom", "0.5rem")
      .style("flex-wrap", "wrap").style("font-size", "0.8rem");

    function addSlider(parent, label, color, min, max, val, onChange) {
      const wrap = parent.append("span")
        .style("display", "inline-flex").style("align-items", "center").style("gap", "4px");
      wrap.append("span").style("color", FG).style("font-weight", "600").text(label);
      const sl = wrap.append("input").attr("type", "range")
        .attr("min", min).attr("max", max).attr("value", val)
        .style("width", "80px").style("accent-color", color);
      const lbl = wrap.append("code")
        .style("background", "#f5f5f5").style("padding", "2px 6px")
        .style("border-radius", "3px").style("font-size", "0.75rem")
        .style("min-width", "18px").style("text-align", "center")
        .text(val);
      sl.on("input", function () {
        const v = +this.value;
        lbl.text(v);
        onChange(v);
      });
      return { slider: sl, label: lbl };
    }

    addSlider(ctrl, "Preceding:", HIGHLIGHT, 0, 7, preceding, v => { preceding = v; update(); });
    addSlider(ctrl, "Following:", C[2], 0, 7, following, v => { following = v; update(); });

    const specLabel = ctrl.append("code")
      .style("background", "#ede7f6").style("padding", "3px 8px")
      .style("border-radius", "4px").style("font-size", "0.72rem")
      .style("color", HIGHLIGHT);

    const sumDisp = ctrl.append("span")
      .style("font-weight", "700").style("color", HIGHLIGHT).style("font-size", "0.82rem");

    const tip = makeTooltip(el);

    const svg = container.append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const x = d3.scaleBand().domain(data.map((_, i) => i)).range([0, iw]).padding(0.15);
    const y = d3.scaleLinear().domain([0, 450]).range([ih, 0]);

    // X axis
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickFormat(i => data[i].date))
      .selectAll("text").attr("font-size", "0.65rem").attr("transform", "rotate(-30)").attr("text-anchor", "end");

    // Y axis
    g.append("g").call(d3.axisLeft(y).ticks(5).tickSize(-iw))
      .selectAll(".tick line").attr("stroke", "#eee");
    g.selectAll(".domain").attr("stroke", "#ccc");

    g.append("text").attr("transform", "rotate(-90)")
      .attr("x", -ih / 2).attr("y", -38).attr("text-anchor", "middle")
      .attr("font-size", "0.7rem").attr("fill", GRAY).text("Amount");

    // Frame highlight rect
    const frameRect = g.append("rect")
      .attr("y", 0).attr("height", ih)
      .attr("fill", FRAME_BG).attr("stroke", FRAME_BORDER)
      .attr("stroke-width", 1.5).attr("stroke-dasharray", "5,3").attr("rx", 4);

    // Bars
    const bars = g.selectAll(".bar").data(data).enter().append("rect")
      .attr("class", "bar")
      .attr("x", (_, i) => x(i)).attr("width", x.bandwidth())
      .attr("y", d => y(d.amount)).attr("height", d => ih - y(d.amount))
      .attr("rx", 3).style("cursor", "pointer");

    // Amount labels above bars
    const amtLabels = g.selectAll(".amt").data(data).enter().append("text")
      .attr("class", "amt")
      .attr("x", (_, i) => x(i) + x.bandwidth() / 2)
      .attr("y", d => y(d.amount) - 6)
      .attr("text-anchor", "middle").attr("font-size", "0.65rem");

    // Position markers (Row 0, Row 1 ...)
    g.selectAll(".pos").data(data).enter().append("text")
      .attr("class", "pos")
      .attr("x", (_, i) => x(i) + x.bandwidth() / 2)
      .attr("y", ih + 38)
      .attr("text-anchor", "middle").attr("font-size", "0.58rem").attr("fill", GRAY)
      .text((_, i) => `Row ${i}`);

    // Current row marker (triangle)
    const marker = g.append("text")
      .attr("text-anchor", "middle").attr("font-size", "0.85rem").attr("fill", CURRENT_COLOR)
      .text("▼");

    function update() {
      const lo = Math.max(0, currentIdx - preceding);
      const hi = Math.min(data.length - 1, currentIdx + following);
      const inFrame = data.map((_, i) => i >= lo && i <= hi);
      const frameSum = data.filter((_, i) => inFrame[i]).reduce((s, d) => s + d.amount, 0);
      const frameCount = inFrame.filter(Boolean).length;
      const frameAvg = frameCount > 0 ? Math.round(frameSum / frameCount) : 0;

      specLabel.text(`ROWS BETWEEN ${preceding} PRECEDING AND ${following} FOLLOWING`);
      sumDisp.text(`SUM=${frameSum}  AVG=${frameAvg}  (${frameCount} rows)`);

      // Frame rect
      const fxStart = x(lo);
      const fxEnd = x(hi) + x.bandwidth();
      frameRect.attr("x", fxStart).attr("width", fxEnd - fxStart);

      // Bar colors
      bars
        .attr("fill", (_, i) => i === currentIdx ? CURRENT_COLOR :
          inFrame[i] ? HIGHLIGHT : "#e0e0e0")
        .attr("stroke", (_, i) => i === currentIdx ? "#e65100" :
          inFrame[i] ? "#5e35b1" : "#bdbdbd")
        .attr("stroke-width", (_, i) => i === currentIdx ? 2.5 : 1);

      // Amount labels
      amtLabels
        .attr("fill", (_, i) => inFrame[i] ? FG : GRAY)
        .attr("font-weight", (_, i) => i === currentIdx ? "700" : "400")
        .text(d => d.amount);

      // Marker position
      marker
        .attr("x", x(currentIdx) + x.bandwidth() / 2)
        .attr("y", y(data[currentIdx].amount) - 16);
    }

    // Click bars to set current
    bars.on("click", function (ev, d) {
      currentIdx = data.indexOf(d);
      update();
    }).on("mouseenter", function (ev, d) {
      const i = data.indexOf(d);
      const lo = Math.max(0, currentIdx - preceding);
      const hi = Math.min(data.length - 1, currentIdx + following);
      const inF = i >= lo && i <= hi;
      const dist = i - currentIdx;
      const pos = dist === 0 ? "CURRENT ROW" :
        dist < 0 ? `${Math.abs(dist)} PRECEDING` : `${dist} FOLLOWING`;
      tip.show(
        `<strong>${d.date}</strong> · Amount: <strong>${d.amount}</strong><br>` +
        `Position: Row ${i} (${pos})<br>` +
        (inF ? `<span style="color:${HIGHLIGHT}">✓ In frame</span>` :
          `<span style="color:${GRAY}">✗ Outside frame</span>`),
        ev.offsetX + 14, ev.offsetY - 40
      );
    }).on("mouseleave", () => tip.hide());

    update();

    container.append("div")
      .style("text-align", "center").style("font-size", "0.68rem")
      .style("color", GRAY).style("margin-top", "0.3rem")
      .html("Click a bar to set <strong>CURRENT ROW</strong>. Drag sliders to adjust frame boundaries. " +
        "<span style='color:" + HIGHLIGHT + "'>■</span> in frame  " +
        "<span style='color:" + CURRENT_COLOR + "'>■</span> current  " +
        "<span style='color:#e0e0e0'>■</span> outside");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. FRAME BOUNDARIES — Visual diagram showing all boundary types
   *    Drag the current row indicator; shows multiple frame spans
   *    overlaid on a row strip with labeled boundaries.
   * ══════════════════════════════════════════════════════════════════ */
  function renderFrameBoundaries(el) {
    const W = Math.min(el.clientWidth || 720, 720), H = 320;
    const m = { t: 20, r: 30, b: 20, l: 30 };
    const iw = W - m.l - m.r;

    const data = [
      { row: 1, amount: 100 },
      { row: 2, amount: 200 },
      { row: 3, amount: 150 },
      { row: 4, amount: 300 },
      { row: 5, amount: 250 },
      { row: 6, amount: 400 },
      { row: 7, amount: 180 },
    ];

    let currentIdx = 3; // row 4 (0-indexed)

    const container = d3.select(el);
    container.selectAll("*").remove();

    const svg = container.append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const cellW = iw / data.length;
    const rowY = 60;
    const cellH = 44;

    // Frame definitions to visualise
    const frames = [
      { label: "UNBOUNDED PRECEDING → CURRENT ROW", color: "#7c4dff", getRange: (ci) => [0, ci] },
      { label: "CURRENT ROW → UNBOUNDED FOLLOWING", color: "#26a69a", getRange: (ci) => [ci, data.length - 1] },
      { label: "2 PRECEDING → 2 FOLLOWING", color: "#ffa726", getRange: (ci) => [Math.max(0, ci - 2), Math.min(data.length - 1, ci + 2)] },
      { label: "UNBOUNDED PRECEDING → UNBOUNDED FOLLOWING", color: "#ef5350", getRange: () => [0, data.length - 1] },
    ];

    function update() {
      g.selectAll("*").remove();

      // Row cells
      data.forEach((d, i) => {
        const x = i * cellW;
        const isCurrent = i === currentIdx;

        // Cell background
        g.append("rect")
          .attr("x", x + 2).attr("y", rowY).attr("width", cellW - 4).attr("height", cellH)
          .attr("rx", 6)
          .attr("fill", isCurrent ? "rgba(255, 167, 38, 0.22)" : "#f5f5f5")
          .attr("stroke", isCurrent ? CURRENT_COLOR : "#e0e0e0")
          .attr("stroke-width", isCurrent ? 2.5 : 1)
          .style("cursor", "pointer")
          .on("click", () => { currentIdx = i; update(); });

        // Row number
        g.append("text")
          .attr("x", x + cellW / 2).attr("y", rowY - 8)
          .attr("text-anchor", "middle").attr("font-size", "0.68rem")
          .attr("fill", isCurrent ? CURRENT_COLOR : GRAY)
          .attr("font-weight", isCurrent ? "700" : "400")
          .text(isCurrent ? `[Row ${d.row}]` : `Row ${d.row}`);

        // Amount
        g.append("text")
          .attr("x", x + cellW / 2).attr("y", rowY + cellH / 2 + 5)
          .attr("text-anchor", "middle").attr("font-size", "0.78rem")
          .attr("fill", isCurrent ? CURRENT_COLOR : FG)
          .attr("font-weight", isCurrent ? "700" : "400")
          .text(d.amount)
          .style("cursor", "pointer")
          .on("click", () => { currentIdx = i; update(); });
      });

      // Current row label
      g.append("text")
        .attr("x", currentIdx * cellW + cellW / 2).attr("y", rowY + cellH + 16)
        .attr("text-anchor", "middle").attr("font-size", "0.7rem")
        .attr("fill", CURRENT_COLOR).attr("font-weight", "700")
        .text("▲ CURRENT ROW");

      // Frame spans (drawn below the row strip)
      const spanY = rowY + cellH + 36;
      const spanH = 22;
      const spanGap = 6;

      frames.forEach((frame, fi) => {
        const [start, end] = frame.getRange(currentIdx);
        const x1 = start * cellW + 4;
        const x2 = (end + 1) * cellW - 4;
        const y = spanY + fi * (spanH + spanGap);

        // Span bar
        g.append("rect")
          .attr("x", x1).attr("y", y)
          .attr("width", x2 - x1).attr("height", spanH)
          .attr("rx", 4)
          .attr("fill", frame.color).attr("opacity", 0.15)
          .attr("stroke", frame.color).attr("stroke-width", 1.5);

        // Left cap
        g.append("line")
          .attr("x1", x1).attr("y1", y).attr("x2", x1).attr("y2", y + spanH)
          .attr("stroke", frame.color).attr("stroke-width", 2.5);

        // Right cap
        g.append("line")
          .attr("x1", x2).attr("y1", y).attr("x2", x2).attr("y2", y + spanH)
          .attr("stroke", frame.color).attr("stroke-width", 2.5);

        // Label
        g.append("text")
          .attr("x", (x1 + x2) / 2).attr("y", y + spanH / 2 + 4)
          .attr("text-anchor", "middle").attr("font-size", "0.62rem")
          .attr("fill", frame.color).attr("font-weight", "600")
          .text(frame.label);

        // SUM value at end
        const frameSum = data.slice(start, end + 1).reduce((s, d) => s + d.amount, 0);
        g.append("text")
          .attr("x", x2 + 6).attr("y", y + spanH / 2 + 4)
          .attr("text-anchor", "start").attr("font-size", "0.62rem")
          .attr("fill", frame.color).attr("font-weight", "700")
          .text(`Σ${frameSum}`);
      });
    }

    update();

    container.append("div")
      .style("text-align", "center").style("font-size", "0.68rem")
      .style("color", GRAY).style("margin-top", "0.5rem")
      .text("Click any cell to move CURRENT ROW. Frame spans and SUM values update automatically.");
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const r = document.getElementById("viz-frame-rows");
    if (r && !r.dataset.rendered) { renderRowsFrame(r); r.dataset.rendered = "1"; }
    const c = document.getElementById("viz-frame-compare");
    if (c && !c.dataset.rendered) { renderRowsVsRange(c); c.dataset.rendered = "1"; }
    const t = document.getElementById("viz-range-timeline");
    if (t && !t.dataset.rendered) { renderRangeTimeline(t); t.dataset.rendered = "1"; }
    const b = document.getElementById("viz-rows-bars");
    if (b && !b.dataset.rendered) { renderRowsBarChart(b); b.dataset.rendered = "1"; }
    const fb = document.getElementById("viz-frame-boundaries");
    if (fb && !fb.dataset.rendered) { renderFrameBoundaries(fb); fb.dataset.rendered = "1"; }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
