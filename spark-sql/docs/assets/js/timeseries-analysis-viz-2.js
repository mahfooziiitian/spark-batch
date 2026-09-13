/**
 * timeseries-analysis-viz-2.js
 * D3 v7 interactive visualizations for Spark SQL time-series analysis pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-interval     — stacked interval timeline with overlap highlighting
 *   #viz-inventory    — inventory line/area chart with reorder-point warnings
 *   #viz-p95-latency  — latency histogram with percentile markers
 *   #viz-peak-detection — noisy series with local/global peak toggle
 *   #viz-queue        — wait vs processing stacked queue timeline
 */
(function () {
  "use strict";

  /* ── Shared palette (Material theme) ───────────────────────────── */
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

  function fmtMinute(minute) {
    const h = Math.floor(minute / 60);
    const m = minute % 60;
    return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. INTERVAL ANALYTICS
   *    Overlapping/adjacent intervals on stacked rows.
   * ══════════════════════════════════════════════════════════════════ */
  function renderInterval(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 280;
    const m = { t: 42, r: 24, b: 50, l: 88 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const intervals = [
      { id: "A", start: 15,  end: 75,  row: 0, color: C[0] },
      { id: "B", start: 45,  end: 105, row: 1, color: C[1] },
      { id: "C", start: 105, end: 145, row: 0, color: C[2] },
      { id: "D", start: 118, end: 165, row: 2, color: C[4] },
      { id: "E", start: 155, end: 190, row: 1, color: C[5] },
    ];
    const rows = ["Batch A", "Batch B", "Batch C"];
    const rowH = ih / rows.length;

    intervals.forEach(d => {
      d.overlaps = intervals
        .filter(o => o !== d && d.start < o.end && o.start < d.end)
        .map(o => o.id);
    });

    const overlaps = [];
    for (let i = 0; i < intervals.length; i++) {
      for (let j = i + 1; j < intervals.length; j++) {
        const s = Math.max(intervals[i].start, intervals[j].start);
        const e = Math.min(intervals[i].end, intervals[j].end);
        if (s < e) overlaps.push({ a: intervals[i], b: intervals[j], start: s, end: e });
      }
    }

    const x = d3.scaleLinear().domain([0, 200]).range([0, iw]);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const defs = svg.append("defs");
    const hatchId = `${el.id}-hatch`;
    defs.append("pattern")
      .attr("id", hatchId)
      .attr("patternUnits", "userSpaceOnUse")
      .attr("width", 8)
      .attr("height", 8)
      .attr("patternTransform", "rotate(45)")
      .append("line")
      .attr("x1", 0).attr("x2", 0)
      .attr("y1", 0).attr("y2", 8)
      .attr("stroke", "rgba(0,0,0,0.35)")
      .attr("stroke-width", 3);

    g.selectAll(".gx").data(d3.range(0, 201, 30)).join("line")
      .attr("x1", d => x(d)).attr("x2", d => x(d))
      .attr("y1", 0).attr("y2", ih)
      .attr("stroke", "#eceff1");

    rows.forEach((label, i) => {
      const cy = i * rowH + rowH / 2;
      g.append("line")
        .attr("x1", 0).attr("x2", iw).attr("y1", cy).attr("y2", cy)
        .attr("stroke", "#eceff1");
      g.append("text")
        .attr("x", -12).attr("y", cy + 4)
        .attr("text-anchor", "end")
        .attr("font-size", 11)
        .attr("font-weight", "600")
        .attr("fill", FG)
        .text(label);
    });

    const tip = makeTooltip(el);
    const groups = g.selectAll(".interval").data(intervals).join("g").attr("class", "interval");
    groups.append("rect")
      .attr("x", d => x(d.start))
      .attr("y", d => d.row * rowH + 10)
      .attr("width", d => x(d.end) - x(d.start))
      .attr("height", rowH - 20)
      .attr("rx", 6)
      .attr("fill", d => d.color)
      .attr("opacity", 0.26)
      .attr("stroke", d => d.color)
      .attr("stroke-width", 1.8)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("opacity", 0.42).attr("stroke-width", 2.4);
        tip.show(
          `<b>Interval ${d.id}</b><br>` +
          `Start: <b>${fmtMinute(d.start)}</b><br>` +
          `End: <b>${fmtMinute(d.end)}</b><br>` +
          `Overlaps: <b>${d.overlaps.length ? d.overlaps.join(", ") : "none"}</b>`,
          ev.offsetX + 12, ev.offsetY - 46
        );
      })
      .on("mouseout", function () {
        d3.select(this).attr("opacity", 0.26).attr("stroke-width", 1.8);
        tip.hide();
      });

    groups.append("text")
      .attr("x", d => x(d.start) + 8)
      .attr("y", d => d.row * rowH + rowH / 2 + 4)
      .attr("font-size", 11)
      .attr("font-weight", "700")
      .attr("fill", d => d.color)
      .text(d => `Interval ${d.id}`);

    overlaps.forEach(o => {
      [o.a, o.b].forEach(d => {
        g.append("rect")
          .attr("x", x(o.start))
          .attr("y", d.row * rowH + 10)
          .attr("width", x(o.end) - x(o.start))
          .attr("height", rowH - 20)
          .attr("rx", 6)
          .attr("fill", `url(#${hatchId})`)
          .attr("opacity", 0.55)
          .attr("stroke", d3.color(d.color).darker(0.7))
          .attr("stroke-width", 0.8)
          .attr("pointer-events", "none");
      });
    });

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0, 201, 30)).tickFormat(d => fmtMinute(d)))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#dfe5e8"));

    g.append("text")
      .attr("x", iw / 2).attr("y", -16)
      .attr("text-anchor", "middle")
      .attr("font-size", 12)
      .attr("font-weight", "700")
      .attr("fill", FG)
      .text("Hover an interval to inspect overlap and adjacency");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. INVENTORY ANALYTICS
   *    Stock levels vs reorder point with days-of-inventory tooltip.
   * ══════════════════════════════════════════════════════════════════ */
  function renderInventory(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 30, r: 24, b: 56, l: 54 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { day: "May 1",  level: 148, burn: 10 },
      { day: "May 2",  level: 139, burn: 10 },
      { day: "May 3",  level: 126, burn: 11 },
      { day: "May 4",  level: 118, burn: 12 },
      { day: "May 5",  level: 102, burn: 13 },
      { day: "May 6",  level: 87,  burn: 14 },
      { day: "May 7",  level: 78,  burn: 14 },
      { day: "May 8",  level: 74,  burn: 15 },
      { day: "May 9",  level: 66,  burn: 15 },
      { day: "May 10", level: 92,  burn: 11 },
      { day: "May 11", level: 84,  burn: 11 },
      { day: "May 12", level: 71,  burn: 12 },
      { day: "May 13", level: 55,  burn: 13 },
      { day: "May 14", level: 38,  burn: 13 },
    ];
    const reorderPoint = 80;

    data.forEach(d => {
      d.daysRemaining = +(d.level / d.burn).toFixed(1);
    });

    const x = d3.scalePoint().domain(data.map(d => d.day)).range([0, iw]).padding(0.5);
    const y = d3.scaleLinear().domain([0, 165]).range([ih, 0]);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw)
      .attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#eceff1");

    g.append("path").datum(data)
      .attr("fill", C[2]).attr("opacity", 0.16)
      .attr("d", d3.area().x(d => x(d.day)).y0(ih).y1(d => y(d.level)).curve(d3.curveMonotoneX));

    g.append("path").datum(data)
      .attr("fill", "none")
      .attr("stroke", C[0])
      .attr("stroke-width", 2.8)
      .attr("d", d3.line().x(d => x(d.day)).y(d => y(d.level)).curve(d3.curveMonotoneX));

    g.append("line")
      .attr("x1", 0).attr("x2", iw)
      .attr("y1", y(reorderPoint)).attr("y2", y(reorderPoint))
      .attr("stroke", C[1]).attr("stroke-width", 2)
      .attr("stroke-dasharray", "6,4");
    g.append("text")
      .attr("x", iw - 4).attr("y", y(reorderPoint) - 6)
      .attr("text-anchor", "end").attr("font-size", 10)
      .attr("fill", C[1]).text("Reorder point");

    g.append("line")
      .attr("x1", 0).attr("x2", iw)
      .attr("y1", y(0)).attr("y2", y(0))
      .attr("stroke", C[3]).attr("stroke-width", 2);
    g.append("text")
      .attr("x", iw - 4).attr("y", y(0) - 6)
      .attr("text-anchor", "end").attr("font-size", 10)
      .attr("fill", C[3]).text("Stock-out");

    const tip = makeTooltip(el);
    g.selectAll(".pt").data(data).join("circle")
      .attr("cx", d => x(d.day)).attr("cy", d => y(d.level)).attr("r", 4.5)
      .attr("fill", C[0]).attr("stroke", "#fff").attr("stroke-width", 1.5)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 6.5);
        tip.show(
          `<b>${d.day}</b><br>` +
          `Stock level: <b>${d.level} units</b><br>` +
          `Days remaining: <b>${d.daysRemaining}</b>`,
          ev.offsetX + 12, ev.offsetY - 42
        );
      })
      .on("mouseout", function () { d3.select(this).attr("r", 4.5); tip.hide(); });

    g.selectAll(".warn").data(data.filter(d => d.level < reorderPoint)).join("circle")
      .attr("cx", d => x(d.day)).attr("cy", d => y(d.level)).attr("r", 7.5)
      .attr("fill", C[1]).attr("opacity", 0.22)
      .attr("stroke", C[3]).attr("stroke-width", 1.5)
      .attr("pointer-events", "none");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(data.filter((_, i) => i % 2 === 0).map(d => d.day)))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text")
      .attr("transform", "rotate(-90)")
      .attr("x", -ih / 2).attr("y", -38)
      .attr("text-anchor", "middle")
      .attr("font-size", 10).attr("fill", FG)
      .text("Units on hand");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. P95 LATENCY ANALYSIS
   *    Histogram with P50 / P95 / P99 markers.
   * ══════════════════════════════════════════════════════════════════ */
  function renderP95Latency(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 28, r: 24, b: 52, l: 50 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const latencies = d3.range(100).map(i => {
      let v = 72 + (i % 12) * 10 + Math.floor(i / 8) * 4 + ((i * 17) % 9);
      if (i >= 90) v += (i - 89) * 28;
      if (i >= 96) v += (i - 95) * 42;
      return v;
    });
    const sorted = [...latencies].sort(d3.ascending);
    const p50 = d3.quantileSorted(sorted, 0.50);
    const p95 = d3.quantileSorted(sorted, 0.95);
    const p99 = d3.quantileSorted(sorted, 0.99);

    const x = d3.scaleLinear().domain([0, d3.max(latencies) * 1.05]).nice().range([0, iw]);
    const bins = d3.bin().domain(x.domain()).thresholds(14)(latencies);
    const y = d3.scaleLinear().domain([0, d3.max(bins, d => d.length) + 1]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw)
      .attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#eceff1");

    const tip = makeTooltip(el);
    g.selectAll(".bin").data(bins).join("rect")
      .attr("x", d => x(d.x0) + 1)
      .attr("y", d => y(d.length))
      .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 2))
      .attr("height", d => ih - y(d.length))
      .attr("rx", 3)
      .attr("fill", C[0])
      .attr("opacity", 0.42)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("opacity", 0.68);
        tip.show(
          `<b>${Math.round(d.x0)}–${Math.round(d.x1)} ms</b><br>` +
          `Requests: <b>${d.length}</b>`,
          ev.offsetX + 12, ev.offsetY - 36
        );
      })
      .on("mouseout", function () { d3.select(this).attr("opacity", 0.42); tip.hide(); });

    [
      { label: "P50", value: p50, color: C[2] },
      { label: "P95", value: p95, color: C[1] },
      { label: "P99", value: p99, color: C[3] },
    ].forEach(d => {
      g.append("line")
        .attr("x1", x(d.value)).attr("x2", x(d.value))
        .attr("y1", 0).attr("y2", ih)
        .attr("stroke", d.color).attr("stroke-width", 2.4)
        .attr("stroke-dasharray", "6,4");
      g.append("rect")
        .attr("x", x(d.value) - 8).attr("y", 0)
        .attr("width", 16).attr("height", ih)
        .attr("fill", "transparent")
        .style("cursor", "pointer")
        .on("mouseover", ev => {
          tip.show(`<b>${d.label}</b><br>${d.value.toFixed(1)} ms`, ev.offsetX + 12, ev.offsetY - 36);
        })
        .on("mouseout", () => tip.hide());
      g.append("text")
        .attr("x", x(d.value) + 4).attr("y", 14)
        .attr("font-size", 10).attr("font-weight", "700")
        .attr("fill", d.color)
        .text(`${d.label} ${d.value.toFixed(0)} ms`);
    });

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).ticks(7).tickFormat(d => `${d} ms`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text")
      .attr("transform", "rotate(-90)")
      .attr("x", -ih / 2).attr("y", -36)
      .attr("text-anchor", "middle")
      .attr("font-size", 10).attr("fill", FG)
      .text("Request count");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. PEAK DETECTION
   *    Toggle between all local peaks and the global peak only.
   * ══════════════════════════════════════════════════════════════════ */
  function renderPeakDetection(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 44, r: 24, b: 52, l: 50 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const values = [44, 52, 49, 61, 57, 67, 59, 73, 68, 81, 74, 78, 70, 88, 79, 83, 77, 69, 75, 66, 71, 63];
    const data = values.map((v, i) => ({ t: i + 1, v }));
    const localPeaks = data.filter((d, i) => i > 0 && i < data.length - 1 && d.v > data[i - 1].v && d.v > data[i + 1].v);
    const globalPeak = d3.greatest(data, d => d.v);
    let mode = "all";

    const x = d3.scaleLinear().domain([1, data.length]).range([0, iw]);
    const y = d3.scaleLinear().domain([40, 95]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw)
      .attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#eceff1");

    g.append("path").datum(data)
      .attr("fill", "none")
      .attr("stroke", C[0])
      .attr("stroke-width", 2.6)
      .attr("d", d3.line().x(d => x(d.t)).y(d => y(d.v)).curve(d3.curveMonotoneX));

    const tip = makeTooltip(el);
    g.selectAll(".pt").data(data).join("circle")
      .attr("cx", d => x(d.t)).attr("cy", d => y(d.v)).attr("r", 4)
      .attr("fill", GRAY).attr("stroke", "#fff").attr("stroke-width", 1.2)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 5.8).attr("fill", C[5]);
        tip.show(`<b>T${d.t}</b><br>Value: <b>${d.v}</b>`, ev.offsetX + 12, ev.offsetY - 34);
      })
      .on("mouseout", function () { d3.select(this).attr("r", 4).attr("fill", GRAY); tip.hide(); });

    const peakLayer = g.append("g");
    function updatePeaks() {
      const peaks = mode === "all" ? localPeaks : [globalPeak];
      const marks = peakLayer.selectAll("g.peak").data(peaks, d => d.t);
      marks.exit().remove();
      const enter = marks.enter().append("g").attr("class", "peak");
      enter.append("circle");
      enter.append("text");
      peakLayer.selectAll("g.peak circle")
        .attr("cx", d => x(d.t)).attr("cy", d => y(d.v)).attr("r", d => d === globalPeak ? 7 : 6)
        .attr("fill", d => d === globalPeak ? C[3] : C[1])
        .attr("stroke", "#fff").attr("stroke-width", 1.8);
      peakLayer.selectAll("g.peak text")
        .attr("x", d => x(d.t)).attr("y", d => y(d.v) - 11)
        .attr("text-anchor", "middle").attr("font-size", 10)
        .attr("font-weight", "700")
        .attr("fill", d => d === globalPeak ? C[3] : C[1])
        .text(d => d === globalPeak ? "Global" : `Peak ${d.t}`);
      btnText.text(mode === "all" ? "Showing: all local peaks" : "Showing: global peak only");
    }

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).ticks(8).tickFormat(d => `T${d}`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    const btn = g.append("rect").attr("class", "ts-btn")
      .attr("x", iw - 188).attr("y", -30)
      .attr("width", 176).attr("height", 24).attr("rx", 4)
      .attr("fill", C[0]).attr("opacity", 0.16).attr("stroke", C[0])
      .style("cursor", "pointer");
    const btnText = g.append("text").attr("class", "ts-btn")
      .attr("x", iw - 100).attr("y", -14)
      .attr("text-anchor", "middle").attr("font-size", 10)
      .attr("fill", C[0]).style("cursor", "pointer");

    const toggle = () => {
      mode = mode === "all" ? "global" : "all";
      updatePeaks();
    };
    btn.on("click", toggle);
    btnText.on("click", toggle);

    updatePeaks();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. QUEUE ANALYSIS
   *    Wait time + processing time as total time in system.
   * ══════════════════════════════════════════════════════════════════ */
  function renderQueue(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 28, r: 32, b: 50, l: 84 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { id: "REQ-01", wait: 4,  process: 7 },
      { id: "REQ-02", wait: 9,  process: 6 },
      { id: "REQ-03", wait: 13, process: 10 },
      { id: "REQ-04", wait: 6,  process: 8 },
      { id: "REQ-05", wait: 11, process: 9 },
      { id: "REQ-06", wait: 3,  process: 5 },
      { id: "REQ-07", wait: 8,  process: 12 },
    ];
    data.forEach(d => { d.total = d.wait + d.process; });

    const x = d3.scaleLinear().domain([0, d3.max(data, d => d.total) + 3]).range([0, iw]);
    const y = d3.scaleBand().domain(data.map(d => d.id)).range([0, ih]).padding(0.24);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gx").data(x.ticks(6)).join("line")
      .attr("x1", d => x(d)).attr("x2", d => x(d))
      .attr("y1", 0).attr("y2", ih)
      .attr("stroke", "#eceff1");

    const tip = makeTooltip(el);
    g.selectAll(".wait").data(data).join("rect")
      .attr("x", 0)
      .attr("y", d => y(d.id))
      .attr("width", d => x(d.wait))
      .attr("height", y.bandwidth())
      .attr("rx", 4)
      .attr("fill", C[1])
      .attr("opacity", 0.7)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("opacity", 0.92);
        tip.show(`<b>${d.id}</b><br>Segment: <b>Wait time</b><br>Duration: <b>${d.wait} min</b>`, ev.offsetX + 12, ev.offsetY - 40);
      })
      .on("mouseout", function () { d3.select(this).attr("opacity", 0.7); tip.hide(); });

    g.selectAll(".proc").data(data).join("rect")
      .attr("x", d => x(d.wait))
      .attr("y", d => y(d.id))
      .attr("width", d => x(d.process))
      .attr("height", y.bandwidth())
      .attr("rx", 4)
      .attr("fill", C[2])
      .attr("opacity", 0.76)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("opacity", 0.96);
        tip.show(`<b>${d.id}</b><br>Segment: <b>Processing time</b><br>Duration: <b>${d.process} min</b>`, ev.offsetX + 12, ev.offsetY - 40);
      })
      .on("mouseout", function () { d3.select(this).attr("opacity", 0.76); tip.hide(); });

    g.selectAll(".total").data(data).join("text")
      .attr("x", d => x(d.total) + 6)
      .attr("y", d => y(d.id) + y.bandwidth() / 2 + 4)
      .attr("font-size", 10)
      .attr("fill", FG)
      .text(d => `${d.total}m`);

    g.append("g").call(d3.axisLeft(y))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).ticks(6).tickFormat(d => `${d}m`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    const leg = g.append("g").attr("transform", "translate(0,-18)");
    leg.append("rect").attr("width", 10).attr("height", 10).attr("fill", C[1]).attr("opacity", 0.7);
    leg.append("text").attr("x", 14).attr("y", 9).attr("font-size", 10).attr("fill", FG).text("Wait time");
    leg.append("rect").attr("x", 84).attr("width", 10).attr("height", 10).attr("fill", C[2]).attr("opacity", 0.76);
    leg.append("text").attr("x", 98).attr("y", 9).attr("font-size", 10).attr("fill", FG).text("Processing time");
  }

  /* ── Router ──────────────────────────────────────────────────────── */
  const VIZ_MAP = {
    "viz-interval": renderInterval,
    "viz-inventory": renderInventory,
    "viz-p95-latency": renderP95Latency,
    "viz-peak-detection": renderPeakDetection,
    "viz-queue": renderQueue,
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
