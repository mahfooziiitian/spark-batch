/**
 * timeseries-viz.js
 * D3 v7 interactive visualizations for Spark SQL Time Series tutorial pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-tumbling  — fixed non-overlapping windows
 *   #viz-hopping   — overlapping hop windows, hover-highlight
 *   #viz-sliding   — rolling window with draggable/step frame + MA line
 *   #viz-session   — gap-based session assignment per user
 *   #viz-gapfill   — animated gap-fill strategy switcher
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

  /* ══════════════════════════════════════════════════════════════════
   * 1. TUMBLING WINDOW
   *    Shows fixed 1-hour non-overlapping buckets.
   *    Hover an event dot → tooltip showing revenue + window.
   * ══════════════════════════════════════════════════════════════════ */
  function renderTumbling(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 220;
    const m = { t: 50, r: 20, b: 48, l: 44 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const events = [
      { t: 10, r: 29.99 }, { t: 45, r: 49 },   { t: 65, r: 15.5 },
      { t: 110, r: 99.99 }, { t: 140, r: 34 },  { t: 150, r: 12 },
      { t: 160, r: 77.5 },  { t: 175, r: 55 },
    ];
    const windows = [
      { s: 0,   e: 60,  label: "Window 1 (00:00–01:00)" },
      { s: 60,  e: 120, label: "Window 2 (01:00–02:00)" },
      { s: 120, e: 180, label: "Window 3 (02:00–03:00)" },
    ];

    const x  = d3.scaleLinear().domain([0, 180]).range([0, iw]);
    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    /* Window rectangles */
    const winGs = g.selectAll(".win").data(windows).join("g").attr("class", "win");
    winGs.append("rect")
      .attr("x", d => x(d.s) + 1).attr("width", d => x(d.e) - x(d.s) - 2)
      .attr("y", 0).attr("height", ih).attr("rx", 6)
      .attr("fill", (_, i) => C[i]).attr("opacity", 0.13)
      .attr("stroke", (_, i) => C[i]).attr("stroke-width", 1.5);

    /* Window labels above */
    winGs.append("text")
      .attr("x", d => x((d.s + d.e) / 2)).attr("y", -14)
      .attr("text-anchor", "middle").attr("font-size", 11).attr("font-weight", "700")
      .attr("fill", (_, i) => C[i])
      .text((_, i) => `W${i + 1}`);

    /* Divider lines between windows */
    windows.slice(1).forEach(w => {
      g.append("line")
        .attr("x1", x(w.s)).attr("x2", x(w.s)).attr("y1", 0).attr("y2", ih)
        .attr("stroke", "#ccc").attr("stroke-dasharray", "4,3");
    });

    /* X axis */
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues([0, 60, 120, 180]).tickFormat(v => `${v / 60}h`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"));

    g.append("text").attr("x", iw / 2).attr("y", ih + 38)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text("Event time");

    /* Window revenue totals */
    windows.forEach((w, i) => {
      const sum = events.filter(e => e.t >= w.s && e.t < w.e).reduce((a, b) => a + b.r, 0);
      g.append("text")
        .attr("x", x((w.s + w.e) / 2)).attr("y", ih - 6)
        .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", C[i])
        .text(`$${sum.toFixed(2)}`);
    });

    /* Event dots */
    const tip = makeTooltip(el);
    g.selectAll(".ev").data(events).join("circle")
      .attr("cx", d => x(d.t)).attr("cy", ih / 2).attr("r", 7)
      .attr("fill", d => { const i = windows.findIndex(w => d.t >= w.s && d.t < w.e); return i >= 0 ? C[i] : GRAY; })
      .attr("stroke", "#fff").attr("stroke-width", 2).style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 9);
        const i = windows.findIndex(w => d.t >= w.s && d.t < w.e);
        tip.show(
          `<b>t = ${d.t} min</b><br>Revenue: <b>$${d.r}</b><br>` +
          (i >= 0 ? `<span style="color:${C[i]}">● W${i + 1}</span>` : "No window"),
          ev.offsetX + 12, ev.offsetY - 36
        );
      })
      .on("mouseout", function () { d3.select(this).attr("r", 7); tip.hide(); });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. HOPPING WINDOW
   *    1-hour windows with a 30-minute hop → overlapping.
   *    Hover a window row to highlight events inside it.
   * ══════════════════════════════════════════════════════════════════ */
  function renderHopping(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 280;
    const m = { t: 20, r: 20, b: 48, l: 44 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const events   = [{ t: 5 }, { t: 18 }, { t: 35 }, { t: 50 }, { t: 70 }, { t: 85 }, { t: 100 }, { t: 125 }, { t: 170 }];
    const domainEnd = 200;
    /* 1-hour windows, 30-min hop */
    const windows  = d3.range(0, domainEnd - 29, 30).map((s, i) => ({ s, e: s + 60, label: `W${i + 1}` }));

    const x      = d3.scaleLinear().domain([0, domainEnd]).range([0, iw]);
    const evY    = 28;
    const rowH   = (ih - evY - 20) / windows.length;

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    /* Event track */
    g.append("line").attr("x1", 0).attr("x2", iw).attr("y1", evY).attr("y2", evY)
      .attr("stroke", "#e0e0e0");
    g.append("text").attr("x", -8).attr("y", evY + 4).attr("text-anchor", "end")
      .attr("font-size", 10).attr("fill", FG).text("Events");

    const tip     = makeTooltip(el);
    const evDots  = g.selectAll(".ev").data(events).join("circle")
      .attr("cx", d => x(d.t)).attr("cy", evY).attr("r", 5)
      .attr("fill", GRAY).attr("stroke", "#fff").attr("stroke-width", 1.5);

    /* Window bars */
    const wGs = g.selectAll(".wrow").data(windows).join("g")
      .attr("transform", (_, i) => `translate(0,${evY + 20 + i * rowH})`);

    wGs.append("rect")
      .attr("x", d => x(d.s)).attr("width", d => x(d.e) - x(d.s))
      .attr("height", rowH - 3).attr("rx", 4)
      .attr("fill", (_, i) => C[i % C.length]).attr("opacity", 0.22)
      .attr("stroke", (_, i) => C[i % C.length]).attr("stroke-width", 1.2)
      .style("cursor", "pointer");

    wGs.append("text")
      .attr("x", d => x((d.s + d.e) / 2)).attr("y", rowH / 2)
      .attr("text-anchor", "middle").attr("dominant-baseline", "middle")
      .attr("font-size", 10).attr("font-weight", "700")
      .attr("fill", (_, i) => C[i % C.length]).attr("pointer-events", "none")
      .text(d => d.label);

    /* X axis */
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0, domainEnd + 1, 30)).tickFormat(v => `${v}m`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"));

    /* Hover: highlight events belonging to window */
    wGs.on("mouseover", function (ev, d) {
      const i = windows.indexOf(d);
      evDots
        .attr("fill", dd => dd.t >= d.s && dd.t < d.e ? C[i % C.length] : GRAY)
        .attr("r",    dd => dd.t >= d.s && dd.t < d.e ? 8 : 4);
      const inside = events.filter(dd => dd.t >= d.s && dd.t < d.e).length;
      tip.show(
        `<b>${d.label}</b> [${d.s}m – ${d.e}m]<br>` +
        `Events inside: <b>${inside}</b>`,
        ev.offsetX + 12, ev.offsetY - 36
      );
    })
    .on("mouseout", () => { evDots.attr("fill", GRAY).attr("r", 5); tip.hide(); });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. SLIDING (ROLLING) WINDOW
   *    Bar chart + rolling-average line.
   *    ◀ / ▶ buttons (and drag) move the highlighted window frame.
   * ══════════════════════════════════════════════════════════════════ */
  function renderSliding(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 260;
    const m = { t: 40, r: 30, b: 55, l: 50 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const N  = 3; /* rolling window size (rows) */

    const data = [
      { d: 0, v: 120 }, { d: 1, v: 200 }, { d: 2, v: 80  }, { d: 3, v: 300 },
      { d: 4, v: 150 }, { d: 5, v: 90  }, { d: 6, v: 210 }, { d: 7, v: 175 },
      { d: 8, v: 250 }, { d: 9, v: 130 },
    ];
    data.forEach((dd, i) => {
      if (i >= N - 1) dd.ma = d3.mean(data.slice(i - N + 1, i + 1), s => s.v);
    });

    const x     = d3.scalePoint().domain(data.map(d => d.d)).range([0, iw]).padding(0.5);
    const y     = d3.scaleLinear().domain([0, d3.max(data, d => d.v) * 1.12]).range([ih, 0]);
    const barW  = (iw / data.length) * 0.4;

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    /* Grid */
    g.selectAll(".gy").data(y.ticks(4)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    /* Axes */
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickFormat(v => `D${v + 1}`))
      .call(a => a.select(".domain").attr("stroke", "#ccc"))
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(4))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("text").attr("x", -ih / 2).attr("y", -38).attr("transform", "rotate(-90)")
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG).text("Revenue ($)");

    /* Bars */
    const bars = g.selectAll(".bar").data(data).join("rect")
      .attr("x", d => x(d.d) - barW / 2).attr("width", barW)
      .attr("y", d => y(d.v)).attr("height", d => ih - y(d.v))
      .attr("fill", C[0]).attr("opacity", 0.35).attr("rx", 2);

    /* Sliding window frame */
    let winStart = 0;
    const frame = g.append("rect").attr("class", "win-frame")
      .attr("y", -8).attr("height", ih + 8).attr("rx", 5)
      .attr("fill", C[2]).attr("opacity", 0.13)
      .attr("stroke", C[2]).attr("stroke-width", 2).attr("stroke-dasharray", "5,3")
      .style("cursor", "ew-resize");

    const maAvgTxt = g.append("text")
      .attr("y", -14).attr("font-size", 11).attr("font-weight", "700").attr("fill", C[2]);

    function updateFrame() {
      const x1 = x(winStart)         - barW / 2 - 5;
      const x2 = x(winStart + N - 1) + barW / 2 + 5;
      frame.attr("x", x1).attr("width", x2 - x1);
      bars.attr("opacity", (_, i) => (i >= winStart && i < winStart + N) ? 0.75 : 0.25);
      const avg = d3.mean(data.slice(winStart, winStart + N), d => d.v);
      maAvgTxt.attr("x", (x1 + x2) / 2).attr("text-anchor", "middle")
        .text(`MA = $${avg.toFixed(0)}`);
    }
    updateFrame();

    /* MA line */
    const maData = data.filter(d => d.ma != null);
    g.append("path").datum(maData)
      .attr("fill", "none").attr("stroke", C[1]).attr("stroke-width", 2.5)
      .attr("d", d3.line().x(d => x(d.d)).y(d => y(d.ma)).curve(d3.curveMonotoneX));
    g.selectAll(".mad").data(maData).join("circle")
      .attr("cx", d => x(d.d)).attr("cy", d => y(d.ma)).attr("r", 4)
      .attr("fill", C[1]).attr("stroke", "#fff").attr("stroke-width", 1.5);

    /* Legend */
    const leg = g.append("g").attr("transform", `translate(0,-32)`);
    leg.append("rect").attr("width", 10).attr("height", 10).attr("fill", C[0]).attr("opacity", 0.5);
    leg.append("text").attr("x", 14).attr("y", 9).attr("font-size", 10).attr("fill", FG).text("Daily revenue");
    leg.append("line").attr("x1", 110).attr("x2", 128).attr("y1", 5).attr("y2", 5).attr("stroke", C[1]).attr("stroke-width", 2.5);
    leg.append("text").attr("x", 132).attr("y", 9).attr("font-size", 10).attr("fill", FG).text("3-day MA");
    leg.append("rect").attr("x", 200).attr("width", 10).attr("height", 10).attr("rx", 2).attr("fill", C[2]).attr("opacity", 0.3).attr("stroke", C[2]);
    leg.append("text").attr("x", 214).attr("y", 9).attr("font-size", 10).attr("fill", FG).text("Sliding window");

    /* Drag to shift window */
    let dragging = false, dragX0 = 0, winStart0 = 0;
    frame.on("mousedown", ev => { dragging = true; dragX0 = ev.clientX; winStart0 = winStart; ev.preventDefault(); });
    d3.select(window)
      .on("mousemove.ts-slide", ev => {
        if (!dragging) return;
        const stepPx = x(1) - x(0);
        const steps  = Math.round((ev.clientX - dragX0) / stepPx);
        winStart     = Math.max(0, Math.min(data.length - N, winStart0 + steps));
        updateFrame();
      })
      .on("mouseup.ts-slide", () => { dragging = false; });

    /* Step buttons */
    const btnY = ih + 36;
    [["◀ Prev", -1], ["Next ▶", 1]].forEach(([label, dir], i) => {
      const bx = iw / 2 - 60 + i * 68;
      const bg = g.append("rect").attr("x", bx).attr("y", btnY).attr("width", 58).attr("height", 22)
        .attr("rx", 4).attr("fill", C[2]).attr("opacity", 0.18).attr("stroke", C[2]).style("cursor", "pointer");
      const txt = g.append("text").attr("x", bx + 29).attr("y", btnY + 14)
        .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", C[2]).style("cursor", "pointer").text(label);
      const click = () => {
        winStart = Math.max(0, Math.min(data.length - N, winStart + dir));
        updateFrame();
      };
      bg.on("click", click); txt.on("click", click);
    });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. SESSION WINDOW
   *    Per-user event timeline. Sessions detected by a 30-minute gap.
   *    Hover an event dot for session details.
   * ══════════════════════════════════════════════════════════════════ */
  function renderSession(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 210;
    const m = { t: 20, r: 20, b: 48, l: 64 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    const GAP = 30;

    const users = [
      { id: "alice", events: [{ t: 0 }, { t: 5 }, { t: 12 }, { t: 70 }, { t: 75 }] },
      { id: "bob",   events: [{ t: 0 }, { t: 20 }, { t: 90 }, { t: 105 }, { t: 180 }] },
    ];

    /* Assign session IDs by gap detection */
    users.forEach(u => {
      let sid = 0;
      u.events.forEach((ev, i) => {
        if (i === 0) { ev.sid = 0; return; }
        if (ev.t - u.events[i - 1].t > GAP) sid++;
        ev.sid = sid;
      });
    });

    const domEnd = 200;
    const x      = d3.scaleLinear().domain([0, domEnd]).range([0, iw]);
    const rowH   = ih / users.length;

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    const tip = makeTooltip(el);

    users.forEach((u, ui) => {
      const cy = ui * rowH + rowH / 2;

      g.append("text").attr("x", -10).attr("y", cy + 4)
        .attr("text-anchor", "end").attr("font-size", 11).attr("font-weight", "600")
        .attr("fill", FG).text(u.id);

      g.append("line").attr("x1", 0).attr("x2", iw).attr("y1", cy).attr("y2", cy)
        .attr("stroke", "#e0e0e0");

      /* Session spans */
      const sessionGroups = d3.groups(u.events, d => d.sid);
      sessionGroups.forEach(([sid, evs]) => {
        const sx = x(evs[0].t) - 10, ex = x(evs[evs.length - 1].t) + 10;
        g.append("rect").attr("x", sx).attr("width", ex - sx)
          .attr("y", cy - 16).attr("height", 32).attr("rx", 16)
          .attr("fill", C[sid % C.length]).attr("opacity", 0.16)
          .attr("stroke", C[sid % C.length]).attr("stroke-width", 1.2);
        g.append("text").attr("x", (sx + ex) / 2).attr("y", cy - 20)
          .attr("text-anchor", "middle").attr("font-size", 9)
          .attr("fill", C[sid % C.length]).attr("font-weight", "700")
          .text(`Session ${sid + 1}`);
      });

      /* Event dots */
      g.selectAll(`.ev-u${ui}`).data(u.events).join("circle")
        .attr("cx", d => x(d.t)).attr("cy", cy).attr("r", 6)
        .attr("fill", d => C[d.sid % C.length]).attr("stroke", "#fff").attr("stroke-width", 1.5)
        .style("cursor", "pointer")
        .on("mouseover", function (ev, d) {
          d3.select(this).attr("r", 8);
          const dur = u.events.filter(e => e.sid === d.sid);
          const dMin = dur[dur.length - 1].t - dur[0].t;
          tip.show(
            `<b>${u.id}</b> · t = ${d.t} min<br>` +
            `Session <b>${d.sid + 1}</b> · duration ≈ ${dMin} min`,
            ev.offsetX + 12, ev.offsetY - 40
          );
        })
        .on("mouseout", function () { d3.select(this).attr("r", 6); tip.hide(); });
    });

    /* Gap annotation arrow */
    const gapX = x(GAP + 8);
    g.append("line").attr("x1", gapX).attr("x2", gapX).attr("y1", 0).attr("y2", ih)
      .attr("stroke", "#ef5350").attr("stroke-dasharray", "3,3").attr("stroke-width", 1);
    g.append("text").attr("x", gapX + 4).attr("y", 10)
      .attr("font-size", 9).attr("fill", "#ef5350").text(`> ${GAP}m gap → new session`);

    /* X axis */
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickValues(d3.range(0, domEnd + 1, 30)).tickFormat(v => `${v}m`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").attr("stroke", "#e0e0e0"));
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. GAP FILL — animated strategy switcher
   *    Sparse bar chart + buttons: Raw / Zero / Forward / Interpolate
   *    Filled bars animate in orange; known values stay in purple.
   * ══════════════════════════════════════════════════════════════════ */
  function renderGapFill(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 250;
    const m = { t: 36, r: 20, b: 56, l: 50 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const allDays = d3.range(1, 8);
    const known   = { 1: 120, 3: 80, 4: 300, 7: 210 }; /* days 2, 5, 6 missing */

    const strategies = {
      Raw: allDays.map(d => ({ d, v: known[d] ?? null, filled: false })),

      "Zero-fill": allDays.map(d => ({ d, v: known[d] ?? 0, filled: known[d] == null })),

      "Fwd-fill": allDays.map((d, i, arr) => {
        if (known[d] != null) return { d, v: known[d], filled: false };
        for (let j = i - 1; j >= 0; j--) if (known[arr[j]] != null) return { d, v: known[arr[j]], filled: true };
        return { d, v: null, filled: true };
      }),

      Interpolate: allDays.map((d, i, arr) => {
        if (known[d] != null) return { d, v: known[d], filled: false };
        let prev = null, next = null;
        for (let j = i - 1; j >= 0; j--)        if (known[arr[j]] != null) { prev = { d: arr[j], v: known[arr[j]] }; break; }
        for (let j = i + 1; j < arr.length; j++) if (known[arr[j]] != null) { next = { d: arr[j], v: known[arr[j]] }; break; }
        if (prev && next) {
          const frac = (d - prev.d) / (next.d - prev.d);
          return { d, v: Math.round(prev.v + (next.v - prev.v) * frac), filled: true };
        }
        return { d, v: prev?.v ?? null, filled: true };
      }),
    };

    const x    = d3.scaleBand().domain(allDays).range([0, iw]).padding(0.28);
    const y    = d3.scaleLinear().domain([0, 330]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    /* Grid */
    g.selectAll(".gy").data(y.ticks(4)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    /* Axes */
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickFormat(v => `Day ${v}`))
      .call(a => a.select(".domain").attr("stroke", "#ccc"))
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(4))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());
    g.append("text").attr("x", -ih / 2).attr("y", -38).attr("transform", "rotate(-90)")
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG).text("Revenue ($)");

    /* Title */
    const title = g.append("text").attr("x", iw / 2).attr("y", -14)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG).text("Raw data (missing: Day 2, 5, 6)");

    /* Bars */
    const bars = g.selectAll(".bar").data(strategies.Raw).join("rect")
      .attr("x", d => x(d.d)).attr("width", x.bandwidth())
      .attr("y", d => d.v != null ? y(d.v) : ih).attr("height", d => d.v != null ? ih - y(d.v) : 0)
      .attr("fill", C[0]).attr("opacity", 0.6).attr("rx", 3);

    /* Value labels */
    const vl = g.selectAll(".vl").data(strategies.Raw).join("text")
      .attr("x", d => x(d.d) + x.bandwidth() / 2)
      .attr("y", d => d.v != null ? y(d.v) - 4 : ih)
      .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", FG)
      .text(d => d.v != null ? d.v : "—");

    let activeStrategy = "Raw";

    function update(name) {
      if (name === activeStrategy) return;
      activeStrategy = name;
      const dat = strategies[name];
      title.text(name === "Raw"
        ? "Raw data (missing: Day 2, 5, 6)"
        : `Strategy: ${name} — orange bars are filled values`
      );
      bars.data(dat).transition().duration(500)
        .attr("y",      d => d.v != null ? y(d.v) : ih)
        .attr("height", d => d.v != null ? ih - y(d.v) : 0)
        .attr("fill",   d => d.filled ? C[1] : C[0])
        .attr("opacity", 0.68);
      vl.data(dat).transition().duration(500)
        .attr("y",  d => d.v != null ? y(d.v) - 4 : ih)
        .text(d => d.v != null ? d.v : "—");
    }

    /* Strategy buttons */
    const stratNames = Object.keys(strategies);
    const btnW       = Math.min(88, (iw - (stratNames.length - 1) * 8) / stratNames.length);
    const totalW     = stratNames.length * btnW + (stratNames.length - 1) * 8;
    const bx0        = (iw - totalW) / 2;
    const btnY       = ih + 32;

    stratNames.forEach((name, i) => {
      const bx   = bx0 + i * (btnW + 8);
      const isActive = () => activeStrategy === name;

      const bg = g.append("rect").attr("class", "ts-btn")
        .attr("x", bx).attr("y", btnY).attr("width", btnW).attr("height", 22).attr("rx", 4)
        .attr("fill", i === 0 ? C[0] : "#f0f0f0").attr("stroke", i === 0 ? C[0] : "#ccc")
        .style("cursor", "pointer");

      const txt = g.append("text").attr("class", "ts-btn")
        .attr("x", bx + btnW / 2).attr("y", btnY + 14)
        .attr("text-anchor", "middle").attr("font-size", 10)
        .attr("fill", i === 0 ? "#fff" : FG).style("cursor", "pointer")
        .text(name);

      const click = () => {
        /* Reset all buttons */
        g.selectAll("rect.ts-btn").each(function (_, j) {
          d3.select(this).attr("fill", "#f0f0f0").attr("stroke", "#ccc");
        });
        g.selectAll("text.ts-btn").each(function () { d3.select(this).attr("fill", FG); });
        bg.attr("fill", C[0]).attr("stroke", C[0]);
        txt.attr("fill", "#fff");
        update(name);
      };
      bg.on("click", click); txt.on("click", click);
    });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 6. LAG & LEAD
   *    Shows 7 daily revenue bars.  Highlight a bar to see the LAG
   *    (previous row, amber arrow) and LEAD (next row, teal arrow)
   *    values annotated on the chart.
   * ══════════════════════════════════════════════════════════════════ */
  function renderLagLead(el) {
    const W = Math.min(el.clientWidth || 720, 720), H = 280;
    const m = { t: 60, r: 140, b: 56, l: 52 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { day: "Mon", rev: 120 },
      { day: "Tue", rev: 95  },
      { day: "Wed", rev: 160 },
      { day: "Thu", rev: 140 },
      { day: "Fri", rev: 200 },
      { day: "Sat", rev: 175 },
      { day: "Sun", rev: 90  },
    ];
    let active = 2; // Wed highlighted by default

    const x = d3.scaleBand().domain(data.map(d => d.day)).range([0, iw]).padding(0.3);
    const y = d3.scaleLinear().domain([0, 220]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    /* Axes */
    g.append("g").attr("transform", `translate(0,${ih})`).call(d3.axisBottom(x));
    g.append("g").call(d3.axisLeft(y).ticks(5).tickFormat(d => `$${d}`));

    /* Y label */
    g.append("text").attr("transform", "rotate(-90)").attr("x", -ih / 2)
      .attr("y", -42).attr("text-anchor", "middle")
      .attr("font-size", 11).attr("fill", FG).text("Revenue ($)");

    /* Title */
    svg.append("text").attr("x", W / 2).attr("y", 18)
      .attr("text-anchor", "middle").attr("font-size", 13).attr("font-weight", 600)
      .attr("fill", FG).text("Click a bar — see LAG (prev) and LEAD (next)");

    /* Annotation layer (drawn behind bars) */
    const annLayer = g.append("g");

    /* Bars */
    const bars = g.selectAll("rect.bar").data(data).join("rect")
      .attr("class", "bar")
      .attr("x", d => x(d.day))
      .attr("width", x.bandwidth())
      .attr("y", d => y(d.rev))
      .attr("height", d => ih - y(d.rev))
      .attr("rx", 3)
      .attr("fill", (_, i) => i === active ? C[0] : "#b0bec5")
      .attr("cursor", "pointer");

    /* Value labels on bars */
    const valLabels = g.selectAll("text.val").data(data).join("text")
      .attr("class", "val")
      .attr("x", d => x(d.day) + x.bandwidth() / 2)
      .attr("y", d => y(d.rev) - 5)
      .attr("text-anchor", "middle")
      .attr("font-size", 11)
      .attr("fill", FG)
      .text(d => `$${d.rev}`);

    /* Legend */
    const legendData = [
      { label: "LAG (prev day)",  color: C[1] },
      { label: "LEAD (next day)", color: C[2] },
      { label: "Current row",     color: C[0] },
    ];
    const lx = iw + 12, ly = 10;
    legendData.forEach((ld, i) => {
      g.append("rect").attr("x", lx).attr("y", ly + i * 22)
        .attr("width", 14).attr("height", 14).attr("rx", 2).attr("fill", ld.color);
      g.append("text").attr("x", lx + 18).attr("y", ly + i * 22 + 11)
        .attr("font-size", 11).attr("fill", FG).text(ld.label);
    });

    function drawAnnotations(idx) {
      annLayer.selectAll("*").remove();

      const bw = x.bandwidth();
      const cx = i => x(data[i].day) + bw / 2;

      /* LAG arrow (current → prev) */
      if (idx > 0) {
        const x1 = cx(idx) - 2, x2 = cx(idx - 1) + 2;
        const ay = y(data[idx].rev) - 18;
        annLayer.append("line")
          .attr("x1", x1).attr("y1", ay).attr("x2", x2).attr("y2", ay)
          .attr("stroke", C[1]).attr("stroke-width", 2)
          .attr("marker-end", "url(#arr-lag)");
        annLayer.append("text")
          .attr("x", (x1 + x2) / 2).attr("y", ay - 6)
          .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", C[1])
          .text(`LAG = $${data[idx - 1].rev}`);
      }

      /* LEAD arrow (current → next) */
      if (idx < data.length - 1) {
        const x1 = cx(idx) + 2, x2 = cx(idx + 1) - 2;
        const ay = y(data[idx].rev) + 28;
        annLayer.append("line")
          .attr("x1", x1).attr("y1", ay).attr("x2", x2).attr("y2", ay)
          .attr("stroke", C[2]).attr("stroke-width", 2)
          .attr("marker-end", "url(#arr-lead)");
        annLayer.append("text")
          .attr("x", (x1 + x2) / 2).attr("y", ay - 6)
          .attr("text-anchor", "middle").attr("font-size", 10).attr("fill", C[2])
          .text(`LEAD = $${data[idx + 1].rev}`);
      }

      /* Highlight current bar in purple, others grey */
      bars.attr("fill", (_, i) => i === idx ? C[0] : "#b0bec5");
    }

    /* Arrow markers */
    const defs = svg.append("defs");
    [["arr-lag", C[1]], ["arr-lead", C[2]]].forEach(([id, color]) => {
      defs.append("marker").attr("id", id).attr("viewBox", "0 0 8 8")
        .attr("refX", 6).attr("refY", 4)
        .attr("markerWidth", 6).attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path").attr("d", "M0,0 L8,4 L0,8 Z").attr("fill", color);
    });

    bars.on("click", (_, d) => {
      active = data.indexOf(d);
      drawAnnotations(active);
    });

    drawAnnotations(active);
  }

  /* ══════════════════════════════════════════════════════════════════
   * 7. DOCS OVERVIEW (Landing page)
   *    Clickable bar chart summarizing topic coverage.
   * ══════════════════════════════════════════════════════════════════ */
  function renderDocsOverview(el) {
    const W = Math.min(el.clientWidth || 760, 760), H = 300;
    const m = { t: 28, r: 16, b: 88, l: 48 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const data = [
      { section: "Table", count: 6 },
      { section: "DML", count: 9 },
      { section: "Filter", count: 8 },
      { section: "Join", count: 7 },
      { section: "Aggregation", count: 8 },
      { section: "Window", count: 10 },
      { section: "Optimization", count: 5 },
      { section: "Functions", count: 12 },
      { section: "Types", count: 7 },
    ];

    const x = d3.scaleBand()
      .domain(data.map(d => d.section))
      .range([0, iw])
      .padding(0.24);
    const y = d3.scaleLinear()
      .domain([0, d3.max(data, d => d.count) + 2])
      .range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);

    g.selectAll(".gy").data(y.ticks(5)).join("line")
      .attr("x1", 0).attr("x2", iw).attr("y1", d => y(d)).attr("y2", d => y(d))
      .attr("stroke", "#e8e8e8");

    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x))
      .call(a => a.selectAll(".tick text")
        .attr("transform", "rotate(-30)")
        .style("text-anchor", "end")
        .attr("dx", "-0.6em")
        .attr("dy", "0.3em"))
      .call(a => a.select(".domain").attr("stroke", "#ccc"))
      .call(a => a.selectAll(".tick line").remove());
    g.append("g").call(d3.axisLeft(y).ticks(5))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick line").remove());

    const tip = makeTooltip(el);
    const bars = g.selectAll(".bar").data(data).join("rect")
      .attr("x", d => x(d.section))
      .attr("width", x.bandwidth())
      .attr("y", d => y(d.count))
      .attr("height", d => ih - y(d.count))
      .attr("rx", 4)
      .attr("fill", C[0])
      .attr("opacity", 0.62)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("fill", C[1]).attr("opacity", 0.82);
        tip.show(
          `<b>${d.section}</b><br>Starter examples: <b>${d.count}</b>`,
          ev.offsetX + 12, ev.offsetY - 42
        );
      })
      .on("mouseout", function () {
        d3.select(this).attr("fill", C[0]).attr("opacity", 0.62);
        tip.hide();
      })
      .on("click", (_, d) => {
        bars.attr("fill", b => b.section === d.section ? C[1] : C[0])
          .attr("opacity", b => b.section === d.section ? 0.88 : 0.38);
      });

    g.selectAll(".vl").data(data).join("text")
      .attr("x", d => x(d.section) + x.bandwidth() / 2)
      .attr("y", d => y(d.count) - 5)
      .attr("text-anchor", "middle")
      .attr("font-size", 10)
      .attr("fill", FG)
      .text(d => d.count);

    g.append("text").attr("x", iw / 2).attr("y", -8)
      .attr("text-anchor", "middle").attr("font-size", 12).attr("font-weight", "700")
      .attr("fill", FG).text("Spark SQL docs starter coverage by section");
  }

  /* ── Router ──────────────────────────────────────────────────────── */
  const VIZ_MAP = {
    "viz-tumbling" : renderTumbling,
    "viz-hopping"  : renderHopping,
    "viz-sliding"  : renderSliding,
    "viz-session"  : renderSession,
    "viz-gapfill"  : renderGapFill,
    "viz-laglead"  : renderLagLead,
    "viz-docs-overview": renderDocsOverview,
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
