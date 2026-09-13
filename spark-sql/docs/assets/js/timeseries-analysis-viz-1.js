/**
 * timeseries-analysis-viz-1.js
 * D3 v7 interactive visualizations for Spark SQL time-series analysis pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-capacity         — capacity planning trend and threshold forecast
 *   #viz-cost-attribution — stacked daily cost attribution with legend focus
 *   #viz-feature-engineering — raw metric with rolling feature toggles
 *   #viz-forecast-features — lag-based forecast feature navigator
 *   #viz-idle-time        — active vs idle timeline with gap summaries
 */
(function () {
  "use strict";

  /* ── Shared palette (Material purple / amber / teal / red) ───────── */
  const C    = ["#7c4dff", "#ffa726", "#26a69a", "#ef5350", "#ab47bc", "#29b6f6"];
  const GRAY = "#90a4ae";
  const FG   = "#546e7a";

  /** Floating tooltip attached to a container element. */
  function makeTooltip(parent) {
    const div = document.createElement("div");
    div.className = "ts-tooltip";
    div.style.cssText = "opacity:0;position:absolute;pointer-events:none;";
    parent.style.position = "relative";
    parent.appendChild(div);
    return {
      show(html, x, y) { div.innerHTML = html; div.style.opacity = 1; div.style.left = x + "px"; div.style.top = y + "px"; },
      hide()           { div.style.opacity = 0; },
    };
  }

  function fmtHour(v) {
    const h = Math.floor(v);
    const m = Math.round((v - h) * 60);
    return String(h).padStart(2, "0") + ":" + String(m).padStart(2, "0");
  }

  function fmtDuration(hours) {
    const mins = Math.round(hours * 60);
    const h = Math.floor(mins / 60);
    const m = mins % 60;
    if (h && m) return `${h}h ${m}m`;
    if (h) return `${h}h`;
    return `${m}m`;
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. CAPACITY PLANNING
   *    Historical weekly usage, a linear trend projection, and the
   *    future week where the fitted line crosses the capacity limit.
   * ══════════════════════════════════════════════════════════════════ */
  function renderCapacity(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 34, r: 34, b: 56, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { label: "W1", usage: 61 }, { label: "W2", usage: 64 }, { label: "W3", usage: 68 },
      { label: "W4", usage: 72 }, { label: "W5", usage: 76 }, { label: "W6", usage: 79 },
      { label: "W7", usage: 83 }, { label: "W8", usage: 87 },
    ];
    const capacity = 100;
    const xs = data.map((_, i) => i);
    const xMean = d3.mean(xs);
    const yMean = d3.mean(data, d => d.usage);
    const slope = d3.sum(xs.map((xv, i) => (xv - xMean) * (data[i].usage - yMean)))
      / d3.sum(xs.map(xv => (xv - xMean) ** 2));
    const intercept = yMean - slope * xMean;
    const crossX = (capacity - intercept) / slope;
    const projEnd = Math.ceil(crossX) + 1;
    const futureWeeks = Math.max(1, Math.round(crossX - (data.length - 1)));

    const x = d3.scaleLinear().domain([0, projEnd]).range([0, iw]);
    const y = d3.scaleLinear().domain([55, 108]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0, projEnd + 1)).tickFormat(v => `W${v + 1}`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"));
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("transform", "rotate(-90)").attr("x", -ih / 2)
      .attr("y", -38).attr("text-anchor", "middle")
      .attr("font-size", 10).attr("fill", FG).text("Utilization (%)");

    g.append("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", y(capacity)).attr("y2", y(capacity))
      .attr("stroke", C[3]).attr("stroke-width", 2).attr("stroke-dasharray", "6,4");
    g.append("text")
      .attr("x", iw - 4).attr("y", y(capacity) - 8).attr("text-anchor", "end")
      .attr("font-size", 10).attr("fill", C[3]).attr("font-weight", "700")
      .text("Capacity limit");

    const observedLine = d3.line()
      .x((d, i) => x(i))
      .y(d => y(d.usage))
      .curve(d3.curveMonotoneX);

    g.append("path").datum(data)
      .attr("fill", "none").attr("stroke", C[0]).attr("stroke-width", 3)
      .attr("d", observedLine);

    const trendData = d3.range(0, projEnd + 0.01, 0.25).map(v => ({ x: v, y: intercept + slope * v }));
    g.append("path").datum(trendData)
      .attr("fill", "none").attr("stroke", C[1]).attr("stroke-width", 2.5)
      .attr("stroke-dasharray", "7,5")
      .attr("d", d3.line().x(d => x(d.x)).y(d => y(d.y)).curve(d3.curveMonotoneX));

    const tip = makeTooltip(el);
    g.selectAll(".pt").data(data).join("circle")
      .attr("cx", (_, i) => x(i)).attr("cy", d => y(d.usage)).attr("r", 5.5)
      .attr("fill", C[0]).attr("stroke", "#fff").attr("stroke-width", 2)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 8);
        tip.show(
          `<b>${d.label}</b><br>Observed usage: <b>${d.usage}%</b>`,
          ev.offsetX + 12, ev.offsetY - 36
        );
      })
      .on("mouseout", function () { d3.select(this).attr("r", 5.5); tip.hide(); });

    g.append("circle")
      .attr("cx", x(crossX)).attr("cy", y(capacity)).attr("r", 6)
      .attr("fill", C[3]).attr("stroke", "#fff").attr("stroke-width", 2);
    g.append("line")
      .attr("x1", x(crossX)).attr("x2", x(crossX)).attr("y1", y(capacity)).attr("y2", ih)
      .attr("stroke", C[3]).attr("stroke-dasharray", "3,3");
    g.append("text")
      .attr("x", Math.min(iw - 10, x(crossX) + 10)).attr("y", y(capacity) - 22)
      .attr("font-size", 11).attr("font-weight", "700").attr("fill", C[3])
      .text(`Capacity reached in ~${futureWeeks} weeks`);
    g.append("text")
      .attr("x", Math.min(iw - 10, x(crossX) + 10)).attr("y", y(capacity) - 8)
      .attr("font-size", 10).attr("fill", FG)
      .text(`Projected at W${Math.round(crossX) + 1}`);

    const legend = g.append("g").attr("transform", "translate(0,-18)");
    [
      { label: "Observed usage", color: C[0], dash: null },
      { label: "Trend projection", color: C[1], dash: "7,5" },
      { label: "Threshold", color: C[3], dash: "6,4" },
    ].forEach((d, i) => {
      const gx = i * 170;
      legend.append("line")
        .attr("x1", gx).attr("x2", gx + 18).attr("y1", 4).attr("y2", 4)
        .attr("stroke", d.color).attr("stroke-width", 2.5)
        .attr("stroke-dasharray", d.dash || null);
      legend.append("text")
        .attr("x", gx + 24).attr("y", 8).attr("font-size", 10).attr("fill", FG)
        .text(d.label);
    });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. COST ATTRIBUTION
   *    Stacked daily cost bars with a clickable legend to focus one
   *    warehouse (or clear focus and view the full composition).
   * ══════════════════════════════════════════════════════════════════ */
  function renderCostAttribution(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 320;
    const m = { t: 62, r: 24, b: 56, l: 54 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const keys = ["warehouse_a", "warehouse_b", "warehouse_c", "warehouse_d"];
    const data = [
      { day: "Mon", warehouse_a: 88, warehouse_b: 52, warehouse_c: 34, warehouse_d: 18 },
      { day: "Tue", warehouse_a: 92, warehouse_b: 58, warehouse_c: 30, warehouse_d: 24 },
      { day: "Wed", warehouse_a: 84, warehouse_b: 60, warehouse_c: 38, warehouse_d: 20 },
      { day: "Thu", warehouse_a: 96, warehouse_b: 55, warehouse_c: 42, warehouse_d: 19 },
      { day: "Fri", warehouse_a: 108, warehouse_b: 62, warehouse_c: 36, warehouse_d: 27 },
      { day: "Sat", warehouse_a: 78, warehouse_b: 44, warehouse_c: 28, warehouse_d: 16 },
    ];
    const totals = data.map(d => d3.sum(keys, k => d[k]));
    const stack = d3.stack().keys(keys)(data);
    let focusKey = null;

    const x = d3.scaleBand().domain(data.map(d => d.day)).range([0, iw]).padding(0.24);
    const y = d3.scaleLinear().domain([0, d3.max(totals) * 1.12]).range([ih, 0]);
    const color = d3.scaleOrdinal().domain(keys).range([C[0], C[1], C[2], C[5]]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5).tickFormat(v => `$${v}`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("transform", "rotate(-90)").attr("x", -ih / 2)
      .attr("y", -40).attr("text-anchor", "middle")
      .attr("font-size", 10).attr("fill", FG).text("Daily cost (USD)");

    const tip = makeTooltip(el);
    const layer = g.selectAll("g.layer").data(stack).join("g")
      .attr("class", "layer")
      .attr("fill", d => color(d.key));

    const segs = layer.selectAll("rect").data(d => d.map(v => ({ ...v, key: d.key }))).join("rect")
      .attr("x", d => x(d.data.day))
      .attr("width", x.bandwidth())
      .attr("y", d => y(d[1]))
      .attr("height", d => y(d[0]) - y(d[1]))
      .attr("rx", 2)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("stroke", "#fff").attr("stroke-width", 2);
        tip.show(
          `<b>${d.data.day}</b><br>${d.key}: <b>$${(d[1] - d[0]).toFixed(0)}</b>`,
          ev.offsetX + 12, ev.offsetY - 36
        );
      })
      .on("mouseout", function () {
        d3.select(this).attr("stroke", "none");
        tip.hide();
      });

    g.selectAll(".totals").data(data).join("text")
      .attr("x", d => x(d.day) + x.bandwidth() / 2)
      .attr("y", (_, i) => y(totals[i]) - 6)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text((_, i) => `$${totals[i]}`);

    function updateLegend() {
      legend.selectAll("rect.key-btn").each(function (d) {
        const active = focusKey === d;
        d3.select(this)
          .attr("fill", active ? color(d) : "#f0f0f0")
          .attr("stroke", active ? color(d) : "#ccc");
      });
      legend.selectAll("text.key-btn").each(function (d) {
        d3.select(this).attr("fill", focusKey === d ? "#fff" : FG);
      });
    }

    function updateFocus() {
      segs
        .attr("opacity", d => !focusKey || d.key === focusKey ? 0.88 : 0.22)
        .attr("stroke-width", d => focusKey === d.key ? 1.8 : 0)
        .attr("stroke", d => focusKey === d.key ? color(d.key) : "none");
      updateLegend();
    }

    const legend = g.append("g").attr("transform", "translate(0,-44)");
    keys.forEach((key, i) => {
      const gx = i * 160;
      const bg = legend.append("rect").datum(key).attr("class", "ts-btn key-btn")
        .attr("x", gx).attr("y", 0).attr("width", 148).attr("height", 22).attr("rx", 4)
        .attr("fill", "#f0f0f0").attr("stroke", "#ccc")
        .style("cursor", "pointer");
      const txt = legend.append("text").datum(key).attr("class", "ts-btn key-btn")
        .attr("x", gx + 74).attr("y", 14).attr("text-anchor", "middle")
        .attr("font-size", 10).attr("fill", FG).style("cursor", "pointer")
        .text(key.replace("_", " "));
      const click = () => {
        focusKey = focusKey === key ? null : key;
        updateFocus();
      };
      bg.on("click", click);
      txt.on("click", click);
    });

    updateFocus();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. FEATURE ENGINEERING
   *    Raw metric with a rolling average and rolling maximum overlays.
   *    Legend buttons toggle the derived feature lines.
   * ══════════════════════════════════════════════════════════════════ */
  function renderFeatureEngineering(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 310;
    const m = { t: 58, r: 26, b: 56, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const values = [82, 95, 77, 108, 93, 122, 101, 128, 110, 136, 118, 142];
    const data = values.map((v, i) => ({ idx: i, label: `D${i + 1}`, raw: v }));
    data.forEach((d, i) => {
      const win = data.slice(Math.max(0, i - 2), i + 1);
      d.ma = d3.mean(win, s => s.raw);
      d.rmax = d3.max(win, s => s.raw);
    });

    const x = d3.scalePoint().domain(data.map(d => d.idx)).range([0, iw]).padding(0.4);
    const y = d3.scaleLinear().domain([70, d3.max(data, d => d.rmax) + 10]).range([ih, 0]);
    const visible = { ma: true, rmax: true };

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickFormat(v => `D${v + 1}`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("transform", "rotate(-90)").attr("x", -ih / 2)
      .attr("y", -38).attr("text-anchor", "middle")
      .attr("font-size", 10).attr("fill", FG).text("Metric value");

    const rawLine = d3.line().x(d => x(d.idx)).y(d => y(d.raw)).curve(d3.curveMonotoneX);
    const maLine = d3.line().x(d => x(d.idx)).y(d => y(d.ma)).curve(d3.curveMonotoneX);
    const maxLine = d3.line().x(d => x(d.idx)).y(d => y(d.rmax)).curve(d3.curveMonotoneX);

    g.append("path").datum(data)
      .attr("fill", "none").attr("stroke", C[0]).attr("stroke-width", 2.5)
      .attr("d", rawLine);
    const maPath = g.append("path").datum(data)
      .attr("fill", "none").attr("stroke", C[1]).attr("stroke-width", 2.5)
      .attr("d", maLine);
    const maxPath = g.append("path").datum(data)
      .attr("fill", "none").attr("stroke", C[2]).attr("stroke-width", 2.5)
      .attr("d", maxLine);

    const rawPts = g.selectAll("circle.raw").data(data).join("circle")
      .attr("cx", d => x(d.idx)).attr("cy", d => y(d.raw)).attr("r", 4.5)
      .attr("fill", C[0]).attr("stroke", "#fff").attr("stroke-width", 1.5);
    const maPts = g.selectAll("circle.ma").data(data).join("circle")
      .attr("cx", d => x(d.idx)).attr("cy", d => y(d.ma)).attr("r", 3.5)
      .attr("fill", C[1]).attr("stroke", "#fff").attr("stroke-width", 1.2);
    const maxPts = g.selectAll("circle.rmax").data(data).join("circle")
      .attr("cx", d => x(d.idx)).attr("cy", d => y(d.rmax)).attr("r", 3.5)
      .attr("fill", C[2]).attr("stroke", "#fff").attr("stroke-width", 1.2);

    const tip = makeTooltip(el);
    g.selectAll("circle.hover").data(data).join("circle")
      .attr("cx", d => x(d.idx)).attr("cy", d => y(d.raw)).attr("r", 11)
      .attr("fill", "transparent")
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        rawPts.attr("r", p => p === d ? 6.5 : 4.5);
        tip.show(
          `<b>${d.label}</b><br>Raw: <b>${d.raw}</b><br>MA(3): <b>${d.ma.toFixed(1)}</b><br>Rolling max(3): <b>${d.rmax}</b>`,
          ev.offsetX + 12, ev.offsetY - 50
        );
      })
      .on("mouseout", function () { rawPts.attr("r", 4.5); tip.hide(); });

    function updateFeatureVisibility() {
      maPath.attr("display", visible.ma ? null : "none");
      maPts.attr("display", visible.ma ? null : "none");
      maxPath.attr("display", visible.rmax ? null : "none");
      maxPts.attr("display", visible.rmax ? null : "none");
      legend.selectAll("rect.feature-btn").each(function (d) {
        const active = visible[d.key];
        d3.select(this).attr("fill", active ? d.color : "#f0f0f0").attr("stroke", active ? d.color : "#ccc");
      });
      legend.selectAll("text.feature-btn").each(function (d) {
        d3.select(this).attr("fill", visible[d.key] ? "#fff" : FG);
      });
    }

    const legend = g.append("g").attr("transform", "translate(0,-40)");
    [
      { key: "ma", label: "Rolling avg (3)", color: C[1] },
      { key: "rmax", label: "Rolling max (3)", color: C[2] },
    ].forEach((d, i) => {
      const gx = i * 150;
      const bg = legend.append("rect").datum(d).attr("class", "ts-btn feature-btn")
        .attr("x", gx).attr("y", 0).attr("width", 138).attr("height", 22).attr("rx", 4)
        .style("cursor", "pointer");
      const txt = legend.append("text").datum(d).attr("class", "ts-btn feature-btn")
        .attr("x", gx + 69).attr("y", 14).attr("text-anchor", "middle")
        .attr("font-size", 10).style("cursor", "pointer")
        .text(d.label);
      const click = () => {
        visible[d.key] = !visible[d.key];
        updateFeatureVisibility();
      };
      bg.on("click", click);
      txt.on("click", click);
    });

    g.append("text").attr("x", iw - 88).attr("y", -26)
      .attr("font-size", 10).attr("fill", C[0]).attr("font-weight", "700")
      .text("Raw metric");
    g.append("line").attr("x1", iw - 112).attr("x2", iw - 94).attr("y1", -30).attr("y2", -30)
      .attr("stroke", C[0]).attr("stroke-width", 2.5);

    updateFeatureVisibility();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. FORECAST FEATURES
   *    Step through forecast origins and show which prior points become
   *    lag-1 and lag-7 features for the selected row.
   * ══════════════════════════════════════════════════════════════════ */
  function renderForecastFeatures(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 330;
    const m = { t: 48, r: 26, b: 74, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const values = [62, 66, 64, 69, 74, 72, 78, 81, 79, 84, 88, 86, 91, 95];
    const data = values.map((v, i) => ({ idx: i, label: `D${i + 1}`, value: v }));
    let active = 8;

    const x = d3.scalePoint().domain(data.map(d => d.idx)).range([0, iw]).padding(0.4);
    const y = d3.scaleLinear().domain([58, 100]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickFormat(v => `D${v + 1}`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("transform", "rotate(-90)").attr("x", -ih / 2)
      .attr("y", -38).attr("text-anchor", "middle")
      .attr("font-size", 10).attr("fill", FG).text("Series value");

    g.append("path").datum(data)
      .attr("fill", "none").attr("stroke", C[0]).attr("stroke-width", 2.5)
      .attr("opacity", 0.35)
      .attr("d", d3.line().x(d => x(d.idx)).y(d => y(d.value)).curve(d3.curveMonotoneX));

    const ann = g.append("g");
    const pts = g.selectAll("circle.pt").data(data).join("circle")
      .attr("cx", d => x(d.idx)).attr("cy", d => y(d.value)).attr("r", 4.5)
      .attr("fill", GRAY).attr("stroke", "#fff").attr("stroke-width", 1.6);

    const title = svg.append("text")
      .attr("x", W / 2).attr("y", 18).attr("text-anchor", "middle")
      .attr("font-size", 13).attr("font-weight", "700").attr("fill", FG);

    const defs = svg.append("defs");
    [["lag1-arrow", C[1]], ["lag7-arrow", C[2]]].forEach(([id, color]) => {
      defs.append("marker").attr("id", id).attr("viewBox", "0 0 8 8")
        .attr("refX", 6).attr("refY", 4).attr("markerWidth", 6).attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path").attr("d", "M0,0 L8,4 L0,8 Z").attr("fill", color);
    });

    function drawConnector(target, source, color, label, yOffset, markerId) {
      ann.append("line")
        .attr("x1", x(target.idx)).attr("y1", y(target.value) + yOffset)
        .attr("x2", x(source.idx)).attr("y2", y(source.value) + yOffset)
        .attr("stroke", color).attr("stroke-width", 2)
        .attr("stroke-dasharray", "6,4")
        .attr("marker-end", `url(#${markerId})`);
      ann.append("text")
        .attr("x", (x(target.idx) + x(source.idx)) / 2)
        .attr("y", (y(target.value) + y(source.value)) / 2 + yOffset - 8)
        .attr("text-anchor", "middle")
        .attr("font-size", 10).attr("font-weight", "700").attr("fill", color)
        .text(`${label} = ${source.value}`);
    }

    function update() {
      const current = data[active];
      const lag1 = data[active - 1];
      const lag7 = data[active - 7];
      ann.selectAll("*").remove();
      pts
        .attr("r", d => d.idx === active ? 7 : d.idx === active - 1 || d.idx === active - 7 ? 6 : 4.5)
        .attr("fill", d => {
          if (d.idx === active) return C[0];
          if (d.idx === active - 1) return C[1];
          if (d.idx === active - 7) return C[2];
          return GRAY;
        });

      drawConnector(current, lag1, C[1], "lag-1", -8, "lag1-arrow");
      drawConnector(current, lag7, C[2], "lag-7", 10, "lag7-arrow");

      ann.append("text")
        .attr("x", x(current.idx)).attr("y", y(current.value) - 16)
        .attr("text-anchor", "middle").attr("font-size", 11)
        .attr("font-weight", "700").attr("fill", C[0])
        .text(`Forecast row: ${current.label}`);
      title.text(`${current.label} uses ${lag1.label} and ${lag7.label} as lag features`);
    }

    const btnY = ih + 34;
    [["◀ Prev", -1], ["Next ▶", 1]].forEach(([label, dir], i) => {
      const bx = iw / 2 - 70 + i * 78;
      const bg = g.append("rect").attr("class", "ts-btn")
        .attr("x", bx).attr("y", btnY).attr("width", 66).attr("height", 24).attr("rx", 4)
        .attr("fill", "#f0f0f0").attr("stroke", "#ccc")
        .style("cursor", "pointer");
      const txt = g.append("text").attr("class", "ts-btn")
        .attr("x", bx + 33).attr("y", btnY + 15).attr("text-anchor", "middle")
        .attr("font-size", 10).attr("fill", FG).style("cursor", "pointer")
        .text(label);
      const click = () => {
        active = Math.max(7, Math.min(data.length - 1, active + dir));
        update();
      };
      bg.on("click", click);
      txt.on("click", click);
    });

    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. IDLE TIME ANALYSIS
   *    Single-resource timeline with active spans and hatched idle gaps.
   *    Hover idle segments to inspect the time lost between intervals.
   * ══════════════════════════════════════════════════════════════════ */
  function renderIdleTime(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 240;
    const m = { t: 40, r: 24, b: 66, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const active = [
      { start: 8.0, end: 8.75 },
      { start: 9.5, end: 10.25 },
      { start: 10.5, end: 11.1 },
      { start: 13.3, end: 15.0 },
      { start: 16.0, end: 17.2 },
    ];
    const idle = active.slice(0, -1).map((d, i) => ({
      start: d.end,
      end: active[i + 1].start,
      duration: active[i + 1].start - d.end,
    }));
    const trackedStart = active[0].start;
    const trackedEnd = active[active.length - 1].end;
    const totalIdle = d3.sum(idle, d => d.duration);
    const totalSpan = trackedEnd - trackedStart;
    const idlePct = (totalIdle / totalSpan) * 100;

    const x = d3.scaleLinear().domain([8, 18]).range([0, iw]);
    const barY = ih / 2 - 18;
    const barH = 36;

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const defs = svg.append("defs");
    const pat = defs.append("pattern")
      .attr("id", "idle-hatch").attr("patternUnits", "userSpaceOnUse")
      .attr("width", 8).attr("height", 8).attr("patternTransform", "rotate(45)");
    pat.append("rect").attr("width", 8).attr("height", 8).attr("fill", "#eceff1");
    pat.append("line").attr("x1", 0).attr("x2", 0).attr("y1", 0).attr("y2", 8)
      .attr("stroke", "#b0bec5").attr("stroke-width", 3);

    g.append("rect")
      .attr("x", 0).attr("y", barY).attr("width", iw).attr("height", barH).attr("rx", 18)
      .attr("fill", "#fafafa").attr("stroke", "#e0e0e0");

    const tip = makeTooltip(el);
    g.selectAll("rect.idle").data(idle).join("rect")
      .attr("class", "idle")
      .attr("x", d => x(d.start)).attr("y", barY).attr("width", d => x(d.end) - x(d.start))
      .attr("height", barH).attr("fill", "url(#idle-hatch)").attr("stroke", "#b0bec5")
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("stroke", C[3]).attr("stroke-width", 1.5);
        tip.show(
          `<b>Idle gap</b><br>${fmtHour(d.start)} → ${fmtHour(d.end)}<br>Duration: <b>${fmtDuration(d.duration)}</b>`,
          ev.offsetX + 12, ev.offsetY - 44
        );
      })
      .on("mouseout", function () {
        d3.select(this).attr("stroke", "#b0bec5").attr("stroke-width", 1);
        tip.hide();
      });

    g.selectAll("rect.active").data(active).join("rect")
      .attr("x", d => x(d.start)).attr("y", barY).attr("width", d => x(d.end) - x(d.start))
      .attr("height", barH).attr("rx", 6).attr("fill", C[2]).attr("opacity", 0.78);

    g.selectAll("text.active-label").data(active).join("text")
      .attr("x", d => (x(d.start) + x(d.end)) / 2).attr("y", barY + 22)
      .attr("text-anchor", "middle").attr("font-size", 9).attr("fill", "#fff")
      .text((_, i) => `Run ${i + 1}`);

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(d3.range(8, 19)).tickFormat(fmtHour))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"));

    const legend = g.append("g").attr("transform", "translate(0,-18)");
    legend.append("rect").attr("x", 0).attr("y", -8).attr("width", 12).attr("height", 12).attr("rx", 2).attr("fill", C[2]);
    legend.append("text").attr("x", 18).attr("y", 2).attr("font-size", 10).attr("fill", FG).text("Active interval");
    legend.append("rect").attr("x", 116).attr("y", -8).attr("width", 12).attr("height", 12).attr("fill", "url(#idle-hatch)").attr("stroke", "#b0bec5");
    legend.append("text").attr("x", 134).attr("y", 2).attr("font-size", 10).attr("fill", FG).text("Idle gap");

    svg.append("text")
      .attr("x", W / 2).attr("y", H - 14).attr("text-anchor", "middle")
      .attr("font-size", 12).attr("font-weight", "700").attr("fill", FG)
      .text(`Idle time: ${fmtDuration(totalIdle)} of ${fmtDuration(totalSpan)} tracked span (${idlePct.toFixed(1)}%)`);
  }

  /* ── Router ──────────────────────────────────────────────────────── */
  const VIZ_MAP = {
    "viz-capacity": renderCapacity,
    "viz-cost-attribution": renderCostAttribution,
    "viz-feature-engineering": renderFeatureEngineering,
    "viz-forecast-features": renderForecastFeatures,
    "viz-idle-time": renderIdleTime,
  };

  function init() {
    for (const [id, fn] of Object.entries(VIZ_MAP)) {
      const el = document.getElementById(id);
      if (el) { el.innerHTML = ""; fn(el); }
    }
  }

  /* MkDocs Material instant navigation compatibility */
  if (typeof document$ !== "undefined") {
    document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
