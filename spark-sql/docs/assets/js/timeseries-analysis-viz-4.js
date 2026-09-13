/**
 * timeseries-analysis-viz-4.js
 * D3 v7 interactive visualizations for Spark SQL time-series analysis pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-time-aggregation       — daily → weekly → monthly rollups
 *   #viz-time-allocation        — 100% stacked time-allocation breakdown
 *   #viz-time-binning           — event scatter with hourly bin assignment
 *   #viz-trend-detection        — regression-based trend classification
 *   #viz-utilization            — utilization area chart vs target threshold
 *   #viz-workload-classification — workload cluster explorer with legend filter
 *   #viz-date-patterns          — calendar-style date activity heatmap
 */
(function () {
  "use strict";

  /* ── Shared palette (deep-purple + amber Material theme) ─────────── */
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

  function fmtDate(date) {
    return d3.timeFormat("%b %-d")(date);
  }

  function fmtClock(date) {
    return d3.timeFormat("%H:%M")(date);
  }

  function nearestPoint(data, x0, accessor) {
    let best = data[0];
    let bestDist = Math.abs(accessor(best) - x0);
    for (let i = 1; i < data.length; i++) {
      const dist = Math.abs(accessor(data[i]) - x0);
      if (dist < bestDist) {
        best = data[i];
        bestDist = dist;
      }
    }
    return best;
  }

  function regression(points) {
    const n = points.length;
    const sumX = d3.sum(points, d => d.x);
    const sumY = d3.sum(points, d => d.y);
    const sumXY = d3.sum(points, d => d.x * d.y);
    const sumX2 = d3.sum(points, d => d.x * d.x);
    const den = n * sumX2 - sumX * sumX;
    const slope = den === 0 ? 0 : (n * sumXY - sumX * sumY) / den;
    const intercept = (sumY - slope * sumX) / n;
    return { slope, intercept };
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. TIME AGGREGATION
   *    Daily values can be rolled up to weekly or monthly totals.
   *    Buttons animate the same raw series into coarser bar groups.
   * ══════════════════════════════════════════════════════════════════ */
  function renderTimeAggregation(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 320;
    const m = { t: 44, r: 20, b: 84, l: 54 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const start = new Date(2024, 2, 22);
    const values = [18, 22, 19, 24, 20, 28, 26, 30, 27, 31, 29, 34, 33, 36, 38, 35, 41, 39, 44, 42, 46];
    const daily = values.map((v, i) => ({
      date: d3.timeDay.offset(start, i),
      value: v,
      week: Math.floor(i / 7) + 1,
      month: d3.timeFormat("%b")(d3.timeDay.offset(start, i)),
    }));

    function aggregate(mode) {
      if (mode === "Daily") {
        return daily.map(d => ({ key: fmtDate(d.date), value: d.value, span: fmtDate(d.date) }));
      }
      if (mode === "Weekly") {
        return d3.rollups(daily, v => d3.sum(v, d => d.value), d => d.week)
          .map(([week, total]) => {
            const rows = daily.filter(d => d.week === week);
            return {
              key: `W${week}`,
              value: total,
              span: `${fmtDate(rows[0].date)}–${fmtDate(rows[rows.length - 1].date)}`,
            };
          });
      }
      return d3.rollups(daily, v => d3.sum(v, d => d.value), d => d.month)
        .map(([month, total]) => {
          const rows = daily.filter(d => d.month === month);
          return {
            key: month,
            value: total,
            span: `${fmtDate(rows[0].date)}–${fmtDate(rows[rows.length - 1].date)}`,
          };
        });
    }

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const x = d3.scaleBand().range([0, iw]).padding(0.18);
    const y = d3.scaleLinear().range([ih, 0]);

    g.append("g").attr("class", "x-axis").attr("transform", `translate(0,${ih})`);
    g.append("g").attr("class", "y-axis");

    const grid = g.append("g").attr("class", "grid");
    const bars = g.append("g").attr("class", "bars");
    const labels = g.append("g").attr("class", "labels");
    const title = g.append("text").attr("x", iw / 2).attr("y", -18)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG);

    g.append("text").attr("transform", "rotate(-90)")
      .attr("x", -ih / 2).attr("y", -40)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Total value");

    const tip = makeTooltip(el);
    let active = "Daily";

    function draw(mode) {
      const data = aggregate(mode);
      x.domain(data.map(d => d.key));
      y.domain([0, d3.max(data, d => d.value) * 1.15]);

      grid.selectAll("line").data(y.ticks(5)).join("line")
        .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
        .attr("stroke", "#e8e8e8");

      g.select(".x-axis").transition().duration(550)
        .call(d3.axisBottom(x))
        .call(a => a.select(".domain").attr("stroke", "#ccc"))
        .call(a => a.selectAll(".tick line").remove());
      g.select(".y-axis").transition().duration(550)
        .call(d3.axisLeft(y).ticks(5))
        .call(a => a.select(".domain").remove())
        .call(a => a.selectAll(".tick line").remove());

      const barSel = bars.selectAll("rect").data(data, d => d.key).join(
        enter => enter.append("rect")
          .attr("x", d => x(d.key))
          .attr("width", x.bandwidth())
          .attr("y", ih)
          .attr("height", 0)
          .attr("rx", 4)
          .attr("fill", C[0])
          .attr("opacity", 0.68),
        update => update,
        exit => exit.transition().duration(400).attr("y", ih).attr("height", 0).remove()
      );

      barSel.on("mouseover", function (ev, d) {
        d3.select(this).attr("opacity", 0.88);
        tip.show(
          `<b>${d.key}</b><br>${d.span}<br>Total: <b>${d.value}</b>`,
          ev.offsetX + 12, ev.offsetY - 42
        );
      }).on("mouseout", function () {
        d3.select(this).attr("opacity", 0.68);
        tip.hide();
      });

      barSel.transition().duration(550)
        .attr("x", d => x(d.key))
        .attr("width", x.bandwidth())
        .attr("y", d => y(d.value))
        .attr("height", d => ih - y(d.value));

      labels.selectAll("text").data(data, d => d.key).join(
        enter => enter.append("text")
          .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG),
        update => update,
        exit => exit.remove()
      ).transition().duration(550)
        .attr("x", d => x(d.key) + x.bandwidth() / 2)
        .attr("y", d => y(d.value) - 6)
        .text(d => d.value);

      title.text(`${mode} totals from the same 21 daily records`);
    }

    const modes = ["Daily", "Weekly", "Monthly"];
    const btnW = 78;
    const gap = 10;
    const totalW = modes.length * btnW + (modes.length - 1) * gap;
    const bx0 = (iw - totalW) / 2;
    const btnY = ih + 40;

    modes.forEach((mode, i) => {
      const bx = bx0 + i * (btnW + gap);
      const bg = g.append("rect").attr("class", "ts-btn")
        .attr("x", bx).attr("y", btnY).attr("width", btnW).attr("height", 24).attr("rx", 4)
        .attr("fill", i === 0 ? C[0] : "#f0f0f0")
        .attr("stroke", i === 0 ? C[0] : "#ccc")
        .style("cursor", "pointer");
      const txt = g.append("text").attr("class", "ts-btn")
        .attr("x", bx + btnW / 2).attr("y", btnY + 15)
        .attr("text-anchor", "middle").attr("font-size", 10)
        .attr("fill", i === 0 ? "#fff" : FG)
        .style("cursor", "pointer").text(mode);

      const click = () => {
        if (active === mode) return;
        active = mode;
        g.selectAll("rect.ts-btn").attr("fill", "#f0f0f0").attr("stroke", "#ccc");
        g.selectAll("text.ts-btn").attr("fill", FG);
        bg.attr("fill", C[0]).attr("stroke", C[0]);
        txt.attr("fill", "#fff");
        draw(mode);
      };
      bg.on("click", click);
      txt.on("click", click);
    });

    draw(active);
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. TIME ALLOCATION
   *    One normalized bar shows how a 24-hour day is allocated
   *    across running, idle, error, and maintenance states.
   * ══════════════════════════════════════════════════════════════════ */
  function renderTimeAllocation(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 220;
    const m = { t: 42, r: 22, b: 66, l: 22 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { state: "Running", hours: 14.5, color: C[2] },
      { state: "Idle", hours: 6.0, color: C[5] },
      { state: "Error", hours: 1.5, color: C[3] },
      { state: "Maintenance", hours: 2.0, color: C[1] },
    ];
    const total = d3.sum(data, d => d.hours);
    let acc = 0;
    data.forEach(d => {
      d.pct = d.hours / total * 100;
      d.x0 = acc;
      d.x1 = acc + d.pct;
      acc = d.x1;
    });

    const x = d3.scaleLinear().domain([0, 100]).range([0, iw]);
    const barY = ih / 2 - 16;
    const barH = 32;

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.append("text").attr("x", iw / 2).attr("y", -16)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG).text("24-hour state allocation (normalized to 100%)");

    g.append("g").attr("transform", `translate(0,${barY + barH + 18})`)
      .call(d3.axisBottom(x).tickValues([0, 25, 50, 75, 100]).tickFormat(d => d + "%"))
      .call(a => a.select(".domain").attr("stroke", "#ccc"))
      .call(a => a.selectAll(".tick line").attr("y2", -barH - 18).attr("stroke", "#eceff1"));

    const tip = makeTooltip(el);
    g.selectAll("rect.seg").data(data).join("rect")
      .attr("class", "seg")
      .attr("x", d => x(d.x0)).attr("y", barY)
      .attr("width", d => x(d.x1) - x(d.x0)).attr("height", barH)
      .attr("rx", 4).attr("fill", d => d.color).attr("opacity", 0.78)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("opacity", 0.96);
        tip.show(
          `<b>${d.state}</b><br>Hours: <b>${d.hours.toFixed(1)}h</b><br>Share: <b>${d.pct.toFixed(1)}%</b>`,
          ev.offsetX + 12, ev.offsetY - 42
        );
      })
      .on("mouseout", function () { d3.select(this).attr("opacity", 0.78); tip.hide(); });

    g.selectAll("text.seg-label").data(data).join("text")
      .attr("class", "seg-label")
      .attr("x", d => (x(d.x0) + x(d.x1)) / 2)
      .attr("y", barY + barH / 2 + 4)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("font-weight", "700")
      .attr("fill", "#fff")
      .text(d => `${d.pct.toFixed(0)}%`)
      .filter(d => x(d.x1) - x(d.x0) < 52)
      .remove();

    const legend = g.append("g").attr("transform", `translate(0,${ih - 4})`);
    data.forEach((d, i) => {
      const lx = i * 120;
      legend.append("rect").attr("x", lx).attr("y", 0).attr("width", 12).attr("height", 12)
        .attr("rx", 2).attr("fill", d.color).attr("opacity", 0.85);
      legend.append("text").attr("x", lx + 18).attr("y", 10)
        .attr("font-size", 10).attr("fill", FG)
        .text(`${d.state} (${d.hours.toFixed(1)}h)`);
    });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. TIME BINNING
   *    Events land in hourly bins first, then exact interval logic
   *    runs on a much smaller candidate set.
   * ══════════════════════════════════════════════════════════════════ */
  function renderTimeBinning(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 280;
    const m = { t: 34, r: 20, b: 54, l: 46 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const start = new Date(2024, 2, 1, 9, 0, 0);
    const hourStarts = d3.range(6).map(i => d3.timeHour.offset(start, i));
    const hourColors = hourStarts.slice(0, 5).map((_, i) => C[i % C.length]);
    const eventSpec = [
      [10, 1.2], [22, 3.8], [47, 2.6], [68, 4.4], [79, 1.9],
      [95, 3.1], [118, 2.0], [132, 4.8], [149, 2.8], [173, 1.4],
      [188, 3.5], [214, 4.2], [229, 2.2], [251, 5.0], [286, 3.0],
    ];
    const events = eventSpec.map(([mins, level], i) => {
      const time = d3.timeMinute.offset(start, mins);
      const bin = d3.timeHour.count(start, d3.timeHour.floor(time));
      return { id: `E${String(i + 1).padStart(2, "0")}`, time, level, bin };
    });

    const x = d3.scaleTime().domain([start, hourStarts[5]]).range([0, iw]);
    const y = d3.scaleLinear().domain([0, 5.6]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll("line.grid").data(y.ticks(5)).join("line")
      .attr("class", "grid")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.selectAll("line.bin").data(hourStarts).join("line")
      .attr("class", "bin")
      .attr("x1", d => x(d)).attr("x2", d => x(d)).attr("y1", 0).attr("y2", ih)
      .attr("stroke", GRAY).attr("stroke-dasharray", "4,4").attr("opacity", 0.8);

    g.selectAll("text.bin-label").data(hourStarts.slice(0, 5)).join("text")
      .attr("class", "bin-label")
      .attr("x", d => x(d) + 6).attr("y", 12)
      .attr("font-size", 10).attr("fill", FG)
      .text((d, i) => `Bin ${i + 1}`);

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).ticks(d3.timeHour.every(1)).tickFormat(d3.timeFormat("%H:%M")))
      .call(a => a.select(".domain").attr("stroke", "#ccc"));
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("transform", "rotate(-90)")
      .attr("x", -ih / 2).attr("y", -34)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Event score");

    const tip = makeTooltip(el);
    g.selectAll("circle.event").data(events).join("circle")
      .attr("class", "event")
      .attr("cx", d => x(d.time)).attr("cy", d => y(d.level)).attr("r", 6)
      .attr("fill", d => hourColors[d.bin]).attr("stroke", "#fff").attr("stroke-width", 1.6)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 8);
        const binStart = d3.timeHour.floor(d.time);
        const binEnd = d3.timeHour.offset(binStart, 1);
        tip.show(
          `<b>${d.id}</b><br>Time: <b>${fmtClock(d.time)}</b><br>` +
          `Assigned bin: <b>${fmtClock(binStart)}–${fmtClock(binEnd)}</b>`,
          ev.offsetX + 12, ev.offsetY - 42
        );
      })
      .on("mouseout", function () { d3.select(this).attr("r", 6); tip.hide(); });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. TREND DETECTION
   *    Cycle between upward, downward, and flat datasets while a
   *    least-squares trend line and slope classification update live.
   * ══════════════════════════════════════════════════════════════════ */
  function renderTrendDetection(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 320;
    const m = { t: 44, r: 24, b: 78, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const datasets = [
      {
        name: "Upward",
        values: [42, 46, 48, 51, 55, 58, 60, 63, 66, 69, 72, 76],
      },
      {
        name: "Downward",
        values: [80, 77, 74, 72, 70, 67, 65, 61, 59, 56, 53, 50],
      },
      {
        name: "Flat",
        values: [60, 61, 60, 59, 61, 60, 60, 59, 60, 61, 60, 60],
      },
    ].map((set, si) => ({
      ...set,
      data: set.values.map((v, i) => ({
        step: i + 1,
        date: d3.timeDay.offset(new Date(2024, 2, 1), i),
        value: v,
        x: i,
        y: v,
        key: `${si}-${i}`,
      })),
    }));

    const x = d3.scalePoint().range([0, iw]).padding(0.4);
    const y = d3.scaleLinear().range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.append("g").attr("class", "x-axis").attr("transform", `translate(0,${ih})`);
    g.append("g").attr("class", "y-axis");
    const linePath = g.append("path").attr("fill", "none").attr("stroke", C[0]).attr("stroke-width", 2.6);
    const regPath = g.append("path").attr("fill", "none").attr("stroke", C[1]).attr("stroke-width", 2.4).attr("stroke-dasharray", "6,4");
    const title = g.append("text").attr("x", iw / 2).attr("y", -18)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG);
    const stat = g.append("text").attr("x", iw).attr("y", -18)
      .attr("text-anchor", "end").attr("font-size", 11).attr("fill", FG);

    g.append("text").attr("transform", "rotate(-90)")
      .attr("x", -ih / 2).attr("y", -38)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Metric value");

    const tip = makeTooltip(el);
    let idx = 0;

    function classify(slope) {
      if (slope > 1.2) return "Upward";
      if (slope < -1.2) return "Downward";
      return "Flat";
    }

    function update() {
      const current = datasets[idx];
      x.domain(current.data.map(d => d.step));
      y.domain([d3.min(current.data, d => d.value) - 6, d3.max(current.data, d => d.value) + 6]);

      g.select(".x-axis").call(d3.axisBottom(x).tickFormat(v => `P${v}`))
        .call(a => a.select(".domain").attr("stroke", "#ccc"))
        .call(a => a.selectAll(".tick line").remove());
      g.select(".y-axis").call(d3.axisLeft(y).ticks(5))
        .call(a => a.select(".domain").remove())
        .call(a => a.selectAll(".tick line").remove());

      const line = d3.line().x(d => x(d.step)).y(d => y(d.value)).curve(d3.curveMonotoneX);
      linePath.datum(current.data).transition().duration(500).attr("d", line);

      const fit = regression(current.data);
      const fitData = current.data.map(d => ({ step: d.step, value: fit.intercept + fit.slope * d.x }));
      const regLine = d3.line().x(d => x(d.step)).y(d => y(d.value));
      regPath.datum(fitData).transition().duration(500).attr("d", regLine);

      const cls = classify(fit.slope);
      title.text(`${current.name} example with least-squares trend line`);
      stat.text(`Slope ${fit.slope.toFixed(2)} → ${cls}`);

      const pts = g.selectAll("circle.point").data(current.data, d => d.key).join("circle")
        .attr("class", "point")
        .attr("r", 5.5).attr("fill", C[0]).attr("stroke", "#fff").attr("stroke-width", 1.5)
        .style("cursor", "pointer")
        .on("mouseover", function (ev, d) {
          d3.select(this).attr("r", 7.5);
          tip.show(
            `<b>${fmtDate(d.date)}</b><br>Value: <b>${d.value}</b><br>Trend class: <b>${cls}</b>`,
            ev.offsetX + 12, ev.offsetY - 42
          );
        })
        .on("mouseout", function () { d3.select(this).attr("r", 5.5); tip.hide(); });

      pts.transition().duration(500).attr("cx", d => x(d.step)).attr("cy", d => y(d.value));
    }

    const btnW = 160;
    const btnY = ih + 38;
    const btn = g.append("rect").attr("class", "ts-btn")
      .attr("x", iw / 2 - btnW / 2).attr("y", btnY).attr("width", btnW).attr("height", 24).attr("rx", 4)
      .attr("fill", C[1]).attr("opacity", 0.2).attr("stroke", C[1]).style("cursor", "pointer");
    const btnTxt = g.append("text").attr("class", "ts-btn")
      .attr("x", iw / 2).attr("y", btnY + 15)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", C[1])
      .style("cursor", "pointer").text("Cycle dataset");

    const cycle = () => { idx = (idx + 1) % datasets.length; update(); };
    btn.on("click", cycle);
    btnTxt.on("click", cycle);

    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. UTILIZATION ANALYSIS
   *    Area under the hourly utilization curve is split visually into
   *    below-target and above-target regions around a 70% threshold.
   * ══════════════════════════════════════════════════════════════════ */
  function renderUtilization(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 34, r: 24, b: 54, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const target = 70;

    const data = [48, 44, 42, 40, 38, 45, 55, 64, 72, 78, 83, 88, 76, 69, 74, 79, 82, 71, 66, 61, 58, 54, 50, 46]
      .map((u, hour) => ({ hour, util: u }));

    const x = d3.scaleLinear().domain([0, 23]).range([0, iw]);
    const y = d3.scaleLinear().domain([0, 100]).range([ih, 0]);
    const area = d3.area().x(d => x(d.hour)).y0(ih).y1(d => y(d.util)).curve(d3.curveMonotoneX);
    const line = d3.line().x(d => x(d.hour)).y(d => y(d.util)).curve(d3.curveMonotoneX);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const defs = svg.append("defs");
    defs.append("clipPath").attr("id", "util-below-clip-4")
      .append("rect").attr("x", 0).attr("y", y(target)).attr("width", iw).attr("height", ih - y(target));
    defs.append("clipPath").attr("id", "util-above-clip-4")
      .append("rect").attr("x", 0).attr("y", 0).attr("width", iw).attr("height", y(target));

    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll("line.grid").data(d3.range(0, 101, 20)).join("line")
      .attr("class", "grid")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.append("path").datum(data).attr("d", area).attr("fill", C[0]).attr("opacity", 0.2)
      .attr("clip-path", "url(#util-below-clip-4)");
    g.append("path").datum(data).attr("d", area).attr("fill", C[1]).attr("opacity", 0.42)
      .attr("clip-path", "url(#util-above-clip-4)");
    g.append("path").datum(data).attr("d", line).attr("fill", "none").attr("stroke", C[0]).attr("stroke-width", 2.6);

    g.append("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", y(target)).attr("y2", y(target))
      .attr("stroke", C[3]).attr("stroke-width", 2).attr("stroke-dasharray", "6,4");
    g.append("text").attr("x", iw - 4).attr("y", y(target) - 6)
      .attr("text-anchor", "end").attr("font-size", 10).attr("fill", C[3])
      .text(`Target ${target}%`);

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues([0, 4, 8, 12, 16, 20, 23]).tickFormat(d => `${String(d).padStart(2, "0")}:00`))
      .call(a => a.select(".domain").attr("stroke", "#ccc"));
    g.append("g").call(d3.axisLeft(y).ticks(5).tickFormat(d => `${d}%`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("x", iw / 2).attr("y", -12)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG).text("Hourly utilization vs threshold");

    const tip = makeTooltip(el);
    const focus = g.append("circle").attr("r", 5.5).attr("fill", C[0]).attr("stroke", "#fff")
      .attr("stroke-width", 1.6).style("display", "none");

    g.append("rect").attr("width", iw).attr("height", ih).attr("fill", "transparent")
      .on("mousemove", function (ev) {
        const [mx] = d3.pointer(ev, this);
        const hour = Math.max(0, Math.min(23, Math.round(x.invert(mx))));
        const point = data[hour];
        focus.style("display", null).attr("cx", x(point.hour)).attr("cy", y(point.util));
        tip.show(
          `<b>${String(point.hour).padStart(2, "0")}:00</b><br>Utilization: <b>${point.util}%</b><br>` +
          `${point.util >= target ? "Above" : "Below"} target`,
          ev.offsetX + 12, ev.offsetY - 42
        );
      })
      .on("mouseleave", function () { focus.style("display", "none"); tip.hide(); });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 6. WORKLOAD CLASSIFICATION
   *    Queries are plotted by duration and frequency, then filtered by
   *    clicking a workload-class legend entry.
   * ══════════════════════════════════════════════════════════════════ */
  function renderWorkloadClassification(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 330;
    const m = { t: 42, r: 22, b: 64, l: 60 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const colors = {
      Interactive: C[0],
      Batch: C[1],
      ETL: C[2],
      BI: C[5],
      ML: C[4],
    };
    const data = [
      ["Q01", 4, 160, "Interactive"], ["Q02", 7, 138, "Interactive"], ["Q03", 12, 120, "Interactive"],
      ["Q04", 18, 95, "BI"], ["Q05", 10, 112, "BI"], ["Q06", 22, 88, "BI"],
      ["Q07", 130, 34, "Batch"], ["Q08", 180, 28, "Batch"], ["Q09", 240, 22, "Batch"],
      ["Q10", 260, 18, "ETL"], ["Q11", 320, 15, "ETL"], ["Q12", 380, 11, "ETL"],
      ["Q13", 420, 9, "ML"], ["Q14", 520, 6, "ML"], ["Q15", 610, 4, "ML"],
    ].map(([id, duration, freq, type]) => ({ id, duration, freq, type }));

    const classes = ["Interactive", "BI", "Batch", "ETL", "ML"];
    let activeClass = null;

    const x = d3.scaleLinear().domain([0, 650]).range([0, iw]);
    const y = d3.scaleLinear().domain([0, 170]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll("line.grid-y").data(y.ticks(5)).join("line")
      .attr("class", "grid-y")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).ticks(6).tickFormat(d => `${d}s`))
      .call(a => a.select(".domain").attr("stroke", "#ccc"));
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("x", iw / 2).attr("y", -18)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG).text("Duration vs frequency by workload class");
    g.append("text").attr("x", iw / 2).attr("y", ih + 44)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Duration (seconds)");
    g.append("text").attr("transform", "rotate(-90)")
      .attr("x", -ih / 2).attr("y", -42)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Frequency / count");

    const tip = makeTooltip(el);
    const points = g.selectAll("circle.point").data(data).join("circle")
      .attr("class", "point")
      .attr("cx", d => x(d.duration)).attr("cy", d => y(d.freq)).attr("r", 7)
      .attr("fill", d => colors[d.type]).attr("stroke", "#fff").attr("stroke-width", 1.5)
      .attr("opacity", 0.86).style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 9);
        tip.show(
          `<b>${d.id}</b><br>Class: <b>${d.type}</b><br>Duration: <b>${d.duration}s</b><br>Frequency: <b>${d.freq}</b>`,
          ev.offsetX + 12, ev.offsetY - 48
        );
      })
      .on("mouseout", function () { d3.select(this).attr("r", 7); tip.hide(); });

    function restyle() {
      points
        .attr("opacity", d => activeClass == null || d.type === activeClass ? 0.92 : 0.14)
        .attr("r", d => activeClass == null || d.type === activeClass ? 7 : 5);
      legend.selectAll("rect.legend-box")
        .attr("opacity", d => activeClass == null || d === activeClass ? 0.9 : 0.22);
      legend.selectAll("text.legend-text")
        .attr("font-weight", d => activeClass === d ? "700" : "400")
        .attr("opacity", d => activeClass == null || d === activeClass ? 1 : 0.45);
    }

    const legend = g.append("g").attr("transform", `translate(0,${ih + 10})`);
    classes.forEach((cls, i) => {
      const lx = i * 124;
      const box = legend.append("rect").datum(cls).attr("class", "legend-box ts-btn")
        .attr("x", lx).attr("y", 0).attr("width", 14).attr("height", 14).attr("rx", 2)
        .attr("fill", colors[cls]).attr("opacity", 0.9).style("cursor", "pointer");
      const txt = legend.append("text").datum(cls).attr("class", "legend-text ts-btn")
        .attr("x", lx + 20).attr("y", 11).attr("font-size", 10).attr("fill", FG)
        .style("cursor", "pointer").text(cls);
      const click = () => {
        activeClass = activeClass === cls ? null : cls;
        restyle();
      };
      box.on("click", click);
      txt.on("click", click);
    });

    restyle();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 7. DATE PATTERNS
   *    Calendar-style grid reveals weekday and week-of-month intensity
   *    patterns at a glance.
   * ══════════════════════════════════════════════════════════════════ */
  function renderDatePatterns(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 40, r: 24, b: 30, l: 68 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const start = new Date(2024, 3, 1);
    const values = [12, 14, 18, 22, 20, 9, 7, 15, 17, 21, 24, 23, 11, 8, 16, 19, 25, 27, 26, 13, 10, 14, 18, 22, 28, 24, 12, 9, 20, 23];
    const data = values.map((value, i) => {
      const date = d3.timeDay.offset(start, i);
      return {
        date,
        value,
        dow: (date.getDay() + 6) % 7,
        week: Math.floor(i / 7),
      };
    });

    const cell = Math.min(70, Math.min(iw / 7, ih / 5) - 6);
    const color = d3.scaleLinear().domain([d3.min(data, d => d.value), d3.max(data, d => d.value)]).range(["#ede7f6", C[0]]);
    const dayNames = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.append("text").attr("x", iw / 2).attr("y", -16)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG).text("April activity heatmap by weekday and week");

    dayNames.forEach((day, i) => {
      g.append("text").attr("x", i * (cell + 6) + cell / 2).attr("y", -2)
        .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG).text(day);
    });
    d3.range(5).forEach(week => {
      g.append("text").attr("x", -12).attr("y", week * (cell + 6) + cell / 2 + 4)
        .attr("text-anchor", "end").attr("font-size", 10).attr("fill", FG)
        .text(`W${week + 1}`);
    });

    const tip = makeTooltip(el);
    g.selectAll("rect.cell").data(data).join("rect")
      .attr("class", "cell")
      .attr("x", d => d.dow * (cell + 6))
      .attr("y", d => d.week * (cell + 6))
      .attr("width", cell).attr("height", cell).attr("rx", 6)
      .attr("fill", d => color(d.value)).attr("stroke", "#fff").attr("stroke-width", 1.5)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("stroke", C[1]).attr("stroke-width", 2.2);
        tip.show(
          `<b>${d3.timeFormat("%Y-%m-%d")(d.date)}</b><br>Value: <b>${d.value}</b>`,
          ev.offsetX + 12, ev.offsetY - 38
        );
      })
      .on("mouseout", function () { d3.select(this).attr("stroke", "#fff").attr("stroke-width", 1.5); tip.hide(); });

    g.selectAll("text.cell-val").data(data).join("text")
      .attr("class", "cell-val")
      .attr("x", d => d.dow * (cell + 6) + cell / 2)
      .attr("y", d => d.week * (cell + 6) + cell / 2 + 4)
      .attr("text-anchor", "middle").attr("font-size", 10)
      .attr("fill", d => d.value > 20 ? "#fff" : FG)
      .text(d => d.value);
  }

  /* ── Router ──────────────────────────────────────────────────────── */
  const VIZ_MAP = {
    "viz-time-aggregation": renderTimeAggregation,
    "viz-time-allocation": renderTimeAllocation,
    "viz-time-binning": renderTimeBinning,
    "viz-trend-detection": renderTrendDetection,
    "viz-utilization": renderUtilization,
    "viz-workload-classification": renderWorkloadClassification,
    "viz-date-patterns": renderDatePatterns,
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
