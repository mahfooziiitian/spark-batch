/**
 * timeseries-analysis-viz-3.js
 * D3 v7 interactive visualizations for Spark SQL time-series analysis pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-reliability    — service up/down timeline with MTBF / MTTR summary
 *   #viz-contention     — overlapping job timelines with concurrency callouts
 *   #viz-efficiency     — utilization vs cost scatter plot with efficiency frontier
 *   #viz-rolling-stats  — rolling mean ± stddev band with window-size toggle
 *   #viz-seasonality    — aligned last-week vs this-week seasonal overlay
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

  function fmtHour(h) {
    const day = Math.floor(h / 24);
    const hour = h % 24;
    return `D${day + 1} ${String(hour).padStart(2, "0")}:00`;
  }

  function fmtMinutes(m) {
    const hour = Math.floor(m / 60);
    const mins = m % 60;
    return `${String(hour).padStart(2, "0")}:${String(mins).padStart(2, "0")}`;
  }

  function rollingStats(values, windowSize) {
    return values.map((value, i) => {
      const slice = values.slice(Math.max(0, i - windowSize + 1), i + 1);
      const mean = d3.mean(slice);
      const variance = d3.mean(slice, d => (d - mean) ** 2) || 0;
      const stddev = Math.sqrt(variance);
      return {
        value,
        mean,
        stddev,
        upper: mean + stddev,
        lower: mean - stddev,
        warm: slice.length === windowSize,
      };
    });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. RELIABILITY METRICS
   *    Up/down state timeline with computed MTBF, MTTR, availability.
   * ══════════════════════════════════════════════════════════════════ */
  function renderReliability(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 240;
    const m = { t: 40, r: 20, b: 68, l: 38 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const segments = [
      { s: 0,  e: 18, state: "up" },
      { s: 18, e: 22, state: "down" },
      { s: 22, e: 45, state: "up" },
      { s: 45, e: 48, state: "down" },
      { s: 48, e: 74, state: "up" },
      { s: 74, e: 80, state: "down" },
      { s: 80, e: 96, state: "up" },
    ];
    const upSegments = segments.filter(d => d.state === "up");
    const downSegments = segments.filter(d => d.state === "down");
    const mtbf = d3.mean(upSegments, d => d.e - d.s);
    const mttr = d3.mean(downSegments, d => d.e - d.s);
    const totalUp = d3.sum(upSegments, d => d.e - d.s);
    const totalDown = d3.sum(downSegments, d => d.e - d.s);
    const availability = totalUp * 100 / (totalUp + totalDown);

    const x = d3.scaleLinear().domain([0, 96]).range([0, iw]);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    g.append("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", ih / 2).attr("y2", ih / 2)
      .attr("stroke", "#cfd8dc").attr("stroke-width", 10).attr("stroke-linecap", "round");

    const seg = g.selectAll(".seg").data(segments).join("g").attr("class", "seg");
    seg.append("rect")
      .attr("x", d => x(d.s)).attr("width", d => x(d.e) - x(d.s))
      .attr("y", ih / 2 - 18).attr("height", 36).attr("rx", 6)
      .attr("fill", d => d.state === "up" ? C[2] : C[3])
      .attr("opacity", d => d.state === "up" ? 0.32 : 0.26)
      .attr("stroke", d => d.state === "up" ? C[2] : C[3])
      .attr("stroke-width", 1.6)
      .style("cursor", d => d.state === "down" ? "pointer" : "default")
      .on("mouseover", function (ev, d) {
        if (d.state !== "down") return;
        d3.select(this).attr("opacity", 0.42).attr("stroke-width", 2.2);
        tip.show(
          `<b>Downtime</b><br>${fmtHour(d.s)} → ${fmtHour(d.e)}<br>` +
          `Duration: <b>${d.e - d.s}h</b> (MTTR input)`,
          ev.offsetX + 12, ev.offsetY - 46
        );
      })
      .on("mouseout", function (ev, d) {
        if (d.state !== "down") return;
        d3.select(this).attr("opacity", 0.26).attr("stroke-width", 1.6);
        tip.hide();
      });

    seg.append("text")
      .attr("x", d => x((d.s + d.e) / 2)).attr("y", ih / 2 + 4)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("font-weight", "700")
      .attr("fill", d => d.state === "up" ? C[2] : C[3])
      .text(d => d.state.toUpperCase());

    g.selectAll(".fail-marker").data(downSegments).join("line")
      .attr("x1", d => x(d.s)).attr("x2", d => x(d.s))
      .attr("y1", ih / 2 - 26).attr("y2", ih / 2 + 26)
      .attr("stroke", C[0]).attr("stroke-dasharray", "4,3");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0, 97, 12)).tickFormat(fmtHour))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"))
      .call(a => a.selectAll("text").attr("font-size", 10));

    g.append("text").attr("x", 0).attr("y", -12)
      .attr("font-size", 11).attr("fill", FG)
      .text("Illustrative 4-day service timeline");

    g.append("text").attr("x", iw / 2).attr("y", ih + 44)
      .attr("text-anchor", "middle").attr("font-size", 11).attr("font-weight", "700")
      .attr("fill", FG)
      .text(`MTBF ${mtbf.toFixed(1)}h · MTTR ${mttr.toFixed(1)}h · Availability ${availability.toFixed(1)}%`);
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. RESOURCE CONTENTION
   *    Parallel job bars with overlap windows and concurrency badges.
   * ══════════════════════════════════════════════════════════════════ */
  function renderContention(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 310;
    const m = { t: 26, r: 20, b: 50, l: 116 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const jobs = [
      { name: "etl_daily",    s: 0,  e: 45, color: C[0] },
      { name: "bi_report",    s: 10, e: 30, color: C[1] },
      { name: "ml_training",  s: 20, e: 75, color: C[4] },
      { name: "ad_hoc_1",     s: 25, e: 35, color: C[5] },
      { name: "ad_hoc_2",     s: 60, e: 70, color: C[2] },
    ];
    const boundaries = Array.from(new Set(jobs.flatMap(d => [d.s, d.e]))).sort((a, b) => a - b);
    const overlaps = [];
    for (let i = 0; i < boundaries.length - 1; i++) {
      const s = boundaries[i], e = boundaries[i + 1];
      const active = jobs.filter(d => d.s < e && d.e > s);
      if (active.length >= 2) overlaps.push({ s, e, count: active.length, jobs: active.map(d => d.name) });
    }

    const x = d3.scaleLinear().domain([0, 80]).range([0, iw]);
    const y = d3.scaleBand().domain(jobs.map(d => d.name)).range([0, ih]).padding(0.28);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    g.selectAll(".gy").data(jobs.map(d => d.name)).join("line")
      .attr("x1", 0).attr("x2", iw)
      .attr("y1", d => y(d) + y.bandwidth() / 2)
      .attr("y2", d => y(d) + y.bandwidth() / 2)
      .attr("stroke", "#eceff1");

    g.selectAll(".ov").data(overlaps).join("rect")
      .attr("x", d => x(d.s)).attr("width", d => Math.max(6, x(d.e) - x(d.s)))
      .attr("y", 0).attr("height", ih)
      .attr("fill", C[3]).attr("opacity", 0.06)
      .attr("stroke", C[3]).attr("stroke-dasharray", "5,4");

    const jobGs = g.selectAll(".job").data(jobs).join("g").attr("class", "job");
    jobGs.append("rect")
      .attr("x", d => x(d.s)).attr("width", d => x(d.e) - x(d.s))
      .attr("y", d => y(d.name)).attr("height", y.bandwidth()).attr("rx", 5)
      .attr("fill", d => d.color).attr("opacity", 0.28)
      .attr("stroke", d => d.color).attr("stroke-width", 1.5)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("opacity", 0.42).attr("stroke-width", 2.2);
        tip.show(
          `<b>${d.name}</b><br>${fmtMinutes(d.s)} → ${fmtMinutes(d.e)}<br>` +
          `Runtime: <b>${d.e - d.s} min</b>`,
          ev.offsetX + 12, ev.offsetY - 42
        );
      })
      .on("mouseout", function (ev, d) {
        d3.select(this).attr("opacity", 0.28).attr("stroke-width", 1.5);
        tip.hide();
      });

    jobGs.append("text")
      .attr("x", d => x(d.s) + 8).attr("y", d => y(d.name) + y.bandwidth() / 2 + 4)
      .attr("font-size", 10).attr("font-weight", "700")
      .attr("fill", d => d.color)
      .text(d => d.name);

    const badges = g.selectAll(".badge").data(overlaps).join("g").attr("class", "badge");
    badges.append("rect")
      .attr("x", d => x((d.s + d.e) / 2) - 13).attr("y", 8)
      .attr("width", 26).attr("height", 18).attr("rx", 9)
      .attr("fill", "#fff").attr("stroke", C[3]).attr("stroke-width", 1.8);
    badges.append("text")
      .attr("x", d => x((d.s + d.e) / 2)).attr("y", 21)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("font-weight", "700")
      .attr("fill", C[3]).text(d => `${d.count}×`);

    g.append("g").call(d3.axisLeft(y))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove())
      .call(a => a.selectAll("text").attr("fill", FG).attr("font-size", 11));

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0, 81, 10)).tickFormat(fmtMinutes))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"))
      .call(a => a.selectAll("text").attr("font-size", 10));

    g.append("text").attr("x", 0).attr("y", -8)
      .attr("font-size", 11).attr("fill", FG)
      .text("Red windows indicate 2+ jobs running concurrently on one warehouse");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. RESOURCE EFFICIENCY
   *    Utilization vs cost scatter with frontier and inefficiency flag.
   * ══════════════════════════════════════════════════════════════════ */
  function renderEfficiency(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 24, r: 20, b: 54, l: 54 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const points = [
      { name: "wh-bi",    util: 82, cost: 52 },
      { name: "wh-prod",  util: 74, cost: 58 },
      { name: "wh-dev",   util: 41, cost: 54 },
      { name: "cluster-a", util: 68, cost: 50 },
      { name: "cluster-b", util: 29, cost: 44 },
      { name: "adhoc-xl", util: 55, cost: 76 },
      { name: "etl-l",    util: 88, cost: 63 },
    ];

    const frontier = x => 12 + 0.55 * x;
    points.forEach(d => { d.efficient = d.cost <= frontier(d.util) + 2; });

    const x = d3.scaleLinear().domain([20, 95]).range([0, iw]);
    const y = d3.scaleLinear().domain([35, 82]).range([ih, 0]);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    g.selectAll(".gx").data(x.ticks(6)).join("line")
      .attr("x1", d => x(d)).attr("x2", d => x(d)).attr("y1", 0).attr("y2", ih)
      .attr("stroke", "#eceff1");
    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#eceff1");

    const frontierData = d3.range(20, 96, 1).map(util => ({ util, cost: frontier(util) }));
    g.append("path").datum(frontierData)
      .attr("fill", "none").attr("stroke", C[0]).attr("stroke-width", 2.4)
      .attr("stroke-dasharray", "6,4")
      .attr("d", d3.line().x(d => x(d.util)).y(d => y(d.cost)).curve(d3.curveMonotoneX));

    g.append("text").attr("x", x(77)).attr("y", y(frontier(77)) - 10)
      .attr("font-size", 10).attr("font-weight", "700").attr("fill", C[0])
      .text("Efficiency frontier");

    const pt = g.selectAll(".pt").data(points).join("g").attr("class", "pt");
    pt.append("circle")
      .attr("cx", d => x(d.util)).attr("cy", d => y(d.cost)).attr("r", 7)
      .attr("fill", d => d.efficient ? C[2] : C[3])
      .attr("stroke", "#fff").attr("stroke-width", 2)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 9);
        tip.show(
          `<b>${d.name}</b><br>Utilization: <b>${d.util}%</b><br>` +
          `Cost: <b>$${d.cost}</b><br>Status: ${d.efficient ? "Efficient" : "Inefficient"}`,
          ev.offsetX + 12, ev.offsetY - 52
        );
      })
      .on("mouseout", function () { d3.select(this).attr("r", 7); tip.hide(); });

    pt.append("text")
      .attr("x", d => x(d.util) + 9).attr("y", d => y(d.cost) - 8)
      .attr("font-size", 10).attr("fill", FG)
      .text(d => d.name);

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).ticks(6).tickFormat(d => `${d}%`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"));
    g.append("g").call(d3.axisLeft(y).ticks(5).tickFormat(d => `$${d}`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("x", iw / 2).attr("y", ih + 40)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Higher utilization for the same or lower cost moves a point toward the frontier");
    g.append("text").attr("x", -ih / 2).attr("y", -38).attr("transform", "rotate(-90)")
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG).text("Cost ($)");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. ROLLING STATISTICS
   *    Rolling mean ± stddev band with 3 / 7 period toggle.
   * ══════════════════════════════════════════════════════════════════ */
  function renderRollingStats(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 320;
    const m = { t: 42, r: 22, b: 62, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const values = [72.2, 71.9, 73.4, 72.8, 74.0, 73.2, 75.1, 74.4, 76.0, 84.5, 75.2, 74.8, 76.1, 75.5];
    const data = values.map((v, i) => ({ idx: i + 1, value: v }));
    const x = d3.scalePoint().domain(data.map(d => d.idx)).range([0, iw]).padding(0.4);
    const y = d3.scaleLinear().domain([68, 87]).range([ih, 0]);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#eceff1");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickFormat(d => `T${d}`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    const band = g.append("path");
    const meanPath = g.append("path");
    const rawPath = g.append("path")
      .attr("fill", "none").attr("stroke", GRAY).attr("stroke-width", 1.8)
      .attr("stroke-dasharray", "4,3")
      .attr("d", d3.line().x(d => x(d.idx)).y(d => y(d.value)).curve(d3.curveMonotoneX)(data));
    const meanDots = g.append("g");
    const rawDots = g.append("g");
    const note = g.append("text").attr("x", iw / 2).attr("y", -18)
      .attr("text-anchor", "middle").attr("font-size", 11).attr("font-weight", "700")
      .attr("fill", FG);

    let activeWindow = 3;

    function update(windowSize) {
      activeWindow = windowSize;
      const stats = rollingStats(values, windowSize).map((s, i) => ({ ...data[i], ...s }));
      const warm = stats.filter(d => d.warm);
      warm.forEach(d => { d.anomaly = d.value > d.upper || d.value < d.lower; });

      band.datum(warm)
        .attr("fill", C[1]).attr("opacity", 0.16).attr("stroke", "none")
        .attr(
          "d",
          d3.area()
            .x(d => x(d.idx))
            .y0(d => y(d.lower))
            .y1(d => y(d.upper))
            .curve(d3.curveMonotoneX)
        );

      meanPath.datum(warm)
        .attr("fill", "none").attr("stroke", C[0]).attr("stroke-width", 2.6)
        .attr("d", d3.line().x(d => x(d.idx)).y(d => y(d.mean)).curve(d3.curveMonotoneX));

      meanDots.selectAll("circle").data(warm).join("circle")
        .attr("cx", d => x(d.idx)).attr("cy", d => y(d.mean)).attr("r", 4)
        .attr("fill", C[0]).attr("stroke", "#fff").attr("stroke-width", 1.5);

      rawDots.selectAll("circle").data(stats).join("circle")
        .attr("cx", d => x(d.idx)).attr("cy", d => y(d.value)).attr("r", d => d.anomaly ? 6 : 4.5)
        .attr("fill", d => d.anomaly ? C[3] : C[2])
        .attr("stroke", "#fff").attr("stroke-width", 1.5)
        .style("cursor", "pointer")
        .on("mouseover", function (ev, d) {
          d3.select(this).attr("r", d.anomaly ? 8 : 6);
          tip.show(
            `<b>T${d.idx}</b><br>Value: <b>${d.value.toFixed(1)}</b><br>` +
            `Mean: ${d.mean.toFixed(1)} · StdDev: ${d.stddev.toFixed(2)}<br>` +
            `Band: ${d.lower.toFixed(1)} → ${d.upper.toFixed(1)}`,
            ev.offsetX + 12, ev.offsetY - 54
          );
        })
        .on("mouseout", function (ev, d) {
          d3.select(this).attr("r", d.anomaly ? 6 : 4.5);
          tip.hide();
        });

      note.text(`Window ${windowSize} periods · ${warm.filter(d => d.anomaly).length} anomaly point(s)`);

      btnRow.selectAll("rect")
        .attr("fill", d => d === activeWindow ? C[2] : "#f0f0f0")
        .attr("stroke", d => d === activeWindow ? C[2] : "#cfd8dc")
        .attr("opacity", d => d === activeWindow ? 0.22 : 1);
      btnRow.selectAll("text")
        .attr("fill", d => d === activeWindow ? C[2] : FG);
    }

    const btnRow = g.append("g").attr("transform", `translate(${iw / 2 - 76},${ih + 30})`);
    [3, 7].forEach((size, i) => {
      const bx = i * 86;
      btnRow.append("rect").datum(size).attr("class", "ts-btn")
        .attr("x", bx).attr("y", 0).attr("width", 74).attr("height", 24).attr("rx", 4)
        .style("cursor", "pointer")
        .on("click", () => update(size));
      btnRow.append("text").datum(size)
        .attr("x", bx + 37).attr("y", 16)
        .attr("text-anchor", "middle").attr("font-size", 10).attr("font-weight", "700")
        .style("cursor", "pointer")
        .text(`${size}-period`)
        .on("click", () => update(size));
    });

    g.append("text").attr("x", -ih / 2).attr("y", -38).attr("transform", "rotate(-90)")
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG).text("Metric value");

    update(3);
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. SEASONALITY DETECTION
   *    This-week vs last-week overlay on aligned weekday axis.
   * ══════════════════════════════════════════════════════════════════ */
  function renderSeasonality(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 32, r: 20, b: 58, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const lastWeek = [118, 126, 143, 152, 166, 132, 120];
    const thisWeek = [122, 130, 147, 155, 171, 136, 124];
    const rows = days.map((day, i) => ({ day, idx: i, last: lastWeek[i], current: thisWeek[i], delta: thisWeek[i] - lastWeek[i] }));

    const x = d3.scalePoint().domain(days).range([0, iw]).padding(0.3);
    const y = d3.scaleLinear().domain([110, 178]).range([ih, 0]);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#eceff1");

    [
      { key: "last", label: "Last week", color: C[1] },
      { key: "current", label: "This week", color: C[5] },
    ].forEach((series, i) => {
      g.append("path").datum(rows)
        .attr("fill", "none").attr("stroke", series.color).attr("stroke-width", 2.6)
        .attr("d", d3.line().x(d => x(d.day)).y(d => y(d[series.key])).curve(d3.curveMonotoneX));

      g.selectAll(`.pt-${series.key}`).data(rows).join("circle")
        .attr("cx", d => x(d.day)).attr("cy", d => y(d[series.key])).attr("r", 5)
        .attr("fill", series.color).attr("stroke", "#fff").attr("stroke-width", 1.5)
        .style("cursor", "pointer")
        .on("mouseover", function (ev, d) {
          d3.select(this).attr("r", 7);
          const otherKey = series.key === "current" ? "last" : "current";
          const delta = d[series.key] - d[otherKey];
          tip.show(
            `<b>${series.label}</b> · ${d.day}<br>Value: <b>${d[series.key]}</b><br>` +
            `Δ vs ${otherKey === "last" ? "last week" : "this week"}: <b>${delta > 0 ? "+" : ""}${delta}</b>`,
            ev.offsetX + 12, ev.offsetY - 46
          );
        })
        .on("mouseout", function () { d3.select(this).attr("r", 5); tip.hide(); });
    });

    const leg = g.append("g").attr("transform", "translate(0,-16)");
    [
      { label: "Last week", color: C[1], x: 0 },
      { label: "This week", color: C[5], x: 106 },
    ].forEach(item => {
      leg.append("line").attr("x1", item.x).attr("x2", item.x + 18).attr("y1", 5).attr("y2", 5)
        .attr("stroke", item.color).attr("stroke-width", 2.6);
      leg.append("text").attr("x", item.x + 24).attr("y", 9)
        .attr("font-size", 10).attr("fill", FG).text(item.label);
    });

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    g.append("text").attr("x", iw / 2).attr("y", ih + 40)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Aligned weekday positions make recurring demand peaks easy to compare");
    g.append("text").attr("x", -ih / 2).attr("y", -36).attr("transform", "rotate(-90)")
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG).text("Revenue");
  }

  const VIZ_MAP = {
    "viz-reliability": renderReliability,
    "viz-contention": renderContention,
    "viz-efficiency": renderEfficiency,
    "viz-rolling-stats": renderRollingStats,
    "viz-seasonality": renderSeasonality,
  };

  function init() {
    if (typeof d3 === "undefined") return;
    Object.entries(VIZ_MAP).forEach(([id, render]) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.innerHTML = "";
      render(el);
    });
  }

  if (typeof document$ !== "undefined" && document$.subscribe) {
    document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
