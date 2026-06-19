/**
 * patterns-viz.js
 * D3 v7 interactive visualizations for Query Patterns pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-pagination       — interactive page selector with row highlighting
 *   #viz-gaps-islands     — timeline showing islands (streaks) and gaps
 *   #viz-period-compare   — line chart: 2023 vs 2024 monthly revenue, region toggle
 *   #viz-string-agg       — two-panel raw rows → collected groups, hover-link
 *   #viz-conditional-agg  — grouped bar chart per region/category
 *   #viz-hierarchy        — D3 tree layout of the org chart
 */
(function () {
  "use strict";

  /* ── Shared palette (matches deep-purple / amber Material theme) ─── */
  const C    = ["#7c4dff", "#ffa726", "#26a69a", "#ef5350", "#ab47bc", "#29b6f6"];
  const GRAY = "#90a4ae";
  const FG   = "#546e7a";

  function isDark() {
    return document.documentElement.getAttribute("data-md-color-scheme") === "slate";
  }

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
        div.style.top  = y + "px";
      },
      hide() { div.style.opacity = 0; },
    };
  }

  /* ══════════════════════════════════════════════════════════════════
   * 1. PAGINATION
   *    12 products sorted by price desc.
   *    Click page buttons (1 / 2 / 3, page size = 4) to highlight
   *    the selected rows.  Shows LIMIT / OFFSET SQL in real time.
   * ══════════════════════════════════════════════════════════════════ */
  function renderPagination(el) {
    const PAGE_SIZE = 4;
    const products  = [
      { id: 1,  name: "Laptop Pro 15",    cat: "electronics", price: 1299.99 },
      { id: 6,  name: "Standing Desk",    cat: "furniture",   price:  549.99 },
      { id: 5,  name: "Ergonomic Chair",  cat: "furniture",   price:  399.99 },
      { id: 9,  name: "Noise Headphones", cat: "electronics", price:  179.99 },
      { id: 11, name: "Bookshelf Oak",    cat: "furniture",   price:  199.99 },
      { id: 12, name: "Kindle Reader",    cat: "electronics", price:  129.99 },
      { id: 10, name: "Webcam HD",        cat: "electronics", price:   89.99 },
      { id: 7,  name: "Python Cookbook",  cat: "books",       price:   44.99 },
      { id: 4,  name: "Desk Lamp",        cat: "furniture",   price:   39.99 },
      { id: 8,  name: "SQL in 10 Steps",  cat: "books",       price:   29.99 },
      { id: 2,  name: "Wireless Mouse",   cat: "electronics", price:   29.99 },
      { id: 3,  name: "USB-C Hub",        cat: "electronics", price:   49.99 },
    ];

    const catC = { electronics: C[0], furniture: C[2], books: C[1] };
    const totalPages = Math.ceil(products.length / PAGE_SIZE);
    let current = 1;

    el.innerHTML = "";
    el.style.padding = "12px 16px";

    /* ── Controls ──────────────────────────────────────────────── */
    const ctrl = document.createElement("div");
    ctrl.style.cssText = "display:flex;align-items:center;gap:8px;margin-bottom:10px;flex-wrap:wrap;";

    const pageLabel = document.createElement("span");
    pageLabel.style.cssText = `font-size:12px;font-weight:600;color:${FG};`;
    pageLabel.textContent = "Page:";
    ctrl.appendChild(pageLabel);

    const btns = [];
    for (let p = 1; p <= totalPages; p++) {
      const b = document.createElement("button");
      b.textContent = p;
      b.style.cssText = `cursor:pointer;padding:3px 10px;border-radius:4px;
        border:1.5px solid ${C[0]};background:white;color:${C[0]};
        font-size:12px;font-weight:700;transition:all 0.15s;`;
      b.addEventListener("click", () => { current = p; render(); });
      ctrl.appendChild(b);
      btns.push(b);
    }

    const infoSpan = document.createElement("span");
    infoSpan.style.cssText = `font-size:11px;color:${GRAY};margin-left:6px;`;
    ctrl.appendChild(infoSpan);

    const sqlCode = document.createElement("code");
    sqlCode.style.cssText = `font-size:11px;background:${isDark()?"#263238":"#f0f0f0"};
      padding:2px 7px;border-radius:3px;margin-left:6px;color:${C[0]};`;
    ctrl.appendChild(sqlCode);
    el.appendChild(ctrl);

    /* ── Row list ───────────────────────────────────────────────── */
    const rowContainer = document.createElement("div");
    el.appendChild(rowContainer);

    function render() {
      const offset = (current - 1) * PAGE_SIZE;
      btns.forEach((b, i) => {
        const active = i + 1 === current;
        b.style.background = active ? C[0] : (isDark() ? "#263238" : "white");
        b.style.color       = active ? "#fff" : C[0];
      });
      infoSpan.textContent = `rows ${offset + 1}–${Math.min(offset + PAGE_SIZE, products.length)} of ${products.length}`;
      sqlCode.textContent  = `LIMIT ${PAGE_SIZE} OFFSET ${offset}`;
      rowContainer.innerHTML = "";

      products.forEach((p, i) => {
        const inPage = i >= offset && i < offset + PAGE_SIZE;
        const row = document.createElement("div");
        row.style.cssText = `display:flex;align-items:center;gap:8px;padding:4px 8px;
          border-radius:4px;margin-bottom:2px;font-size:12px;transition:all 0.18s;
          background:${inPage ? C[0] + "18" : "transparent"};
          border-left:3px solid ${inPage ? C[0] : "transparent"};
          opacity:${inPage ? "1" : "0.45"};`;

        const rankEl = document.createElement("span");
        rankEl.style.cssText = `width:22px;text-align:right;font-size:10px;
          color:${inPage ? C[0] : GRAY};font-weight:${inPage ? "700" : "400"};`;
        rankEl.textContent = i + 1;

        const nameEl = document.createElement("span");
        nameEl.style.cssText = "flex:1;";
        nameEl.textContent = p.name;

        const catEl = document.createElement("span");
        catEl.style.cssText = `width:82px;font-size:10px;padding:1px 5px;border-radius:9px;
          background:${catC[p.cat]}22;color:${catC[p.cat]};text-align:center;font-weight:600;`;
        catEl.textContent = p.cat;

        const priceEl = document.createElement("span");
        priceEl.style.cssText = `width:60px;text-align:right;font-weight:700;
          color:${inPage ? C[0] : GRAY};`;
        priceEl.textContent = "$" + p.price.toFixed(2);

        row.appendChild(rankEl);
        row.appendChild(nameEl);
        row.appendChild(catEl);
        row.appendChild(priceEl);
        rowContainer.appendChild(row);
      });
    }

    render();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 2. GAPS & ISLANDS
   *    Two-user timeline (Jan 1–12).  Filled dots = login days, colored
   *    by island group.  Dashed rings = gap days.  Hover for details.
   * ══════════════════════════════════════════════════════════════════ */
  function renderGapsIslands(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 200;
    const m = { t: 44, r: 24, b: 42, l: 68 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const days   = d3.range(1, 13);
    const users  = [
      {
        name:    "alice",
        logins:  new Set([1, 2, 3, 5, 6, 7, 10]),
        islands: { 1: 0, 2: 0, 3: 0, 5: 1, 6: 1, 7: 1, 10: 2 },
      },
      {
        name:    "bob",
        logins:  new Set([1, 4, 5, 6]),
        islands: { 1: 0, 4: 1, 5: 1, 6: 1 },
      },
    ];

    const x   = d3.scaleLinear().domain([1, 12]).range([0, iw]);
    const rowH = ih / users.length;

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g   = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    /* X axis */
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).ticks(12).tickFormat(d => `Jan ${d}`))
      .call(a => a.select(".domain").attr("stroke", "#e0e0e0"))
      .call(a => a.selectAll(".tick text").attr("font-size", 8.5).attr("fill", FG))
      .call(a => a.selectAll(".tick line").attr("stroke", "#e8e8e8"));

    users.forEach((user, ui) => {
      const cy = ui * rowH + rowH / 2;

      g.append("text").attr("x", -8).attr("y", cy + 4)
        .attr("text-anchor", "end").attr("font-size", 11).attr("font-weight", "600")
        .attr("fill", FG).text(user.name);

      /* Track line */
      g.append("line")
        .attr("x1", x(1)).attr("x2", x(12)).attr("y1", cy).attr("y2", cy)
        .attr("stroke", "#e8e8e8").attr("stroke-width", 1.5);

      days.forEach(d => {
        const hasLogin  = user.logins.has(d);
        const islandIdx = user.islands[d];
        const cx        = x(d);

        if (hasLogin) {
          g.append("circle").attr("cx", cx).attr("cy", cy).attr("r", 9)
            .attr("fill", C[islandIdx % C.length]).attr("opacity", 0.85)
            .attr("stroke", "#fff").attr("stroke-width", 1.5)
            .style("cursor", "pointer")
            .on("mouseover", (ev) => tip.show(
              `<b>${user.name}</b> — Jan ${d}<br>` +
              `<span style="color:${C[islandIdx%C.length]}">&#9679; Island ${islandIdx + 1} (streak)</span>`,
              ev.offsetX + 12, ev.offsetY - 36
            ))
            .on("mouseout", () => tip.hide());

          /* Island label on first day of island */
          const prevDay = d - 1;
          if (!user.logins.has(prevDay) || user.islands[prevDay] !== islandIdx) {
            g.append("text").attr("x", cx).attr("y", cy - 13)
              .attr("text-anchor", "middle").attr("font-size", 9)
              .attr("fill", C[islandIdx % C.length]).attr("font-weight", "700")
              .text(`I${islandIdx + 1}`);
          }
        } else {
          g.append("circle").attr("cx", cx).attr("cy", cy).attr("r", 6)
            .attr("fill", "none").attr("stroke", "#ccc").attr("stroke-dasharray", "2,2")
            .style("cursor", "pointer")
            .on("mouseover", (ev) => tip.show(
              `<b>${user.name}</b> — Jan ${d}<br>` +
              `<span style="color:#ef5350">&#9675; Gap (no login)</span>`,
              ev.offsetX + 12, ev.offsetY - 32
            ))
            .on("mouseout", () => tip.hide());
        }
      });
    });

    /* Legend */
    const lgY = -28;
    ["Island 1", "Island 2", "Island 3"].forEach((lbl, i) => {
      const lx = i * 96;
      g.append("circle").attr("cx", lx + 5).attr("cy", lgY).attr("r", 5).attr("fill", C[i]);
      g.append("text").attr("x", lx + 13).attr("y", lgY + 4)
        .attr("font-size", 9).attr("fill", FG).text(lbl);
    });
    g.append("circle").attr("cx", 293).attr("cy", lgY).attr("r", 5)
      .attr("fill", "none").attr("stroke", "#ccc").attr("stroke-dasharray", "2,2");
    g.append("text").attr("x", 301).attr("y", lgY + 4)
      .attr("font-size", 9).attr("fill", GRAY).text("Gap");
  }

  /* ══════════════════════════════════════════════════════════════════
   * 3. PERIOD COMPARISON
   *    Line chart — Jan–Apr revenue for 2023 (dashed) and 2024 (solid).
   *    Region toggle: APAC / EMEA.
   *    Hover dots to see YoY %.
   * ══════════════════════════════════════════════════════════════════ */
  function renderPeriodCompare(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 270;
    const m = { t: 52, r: 120, b: 52, l: 62 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const months = ["Jan", "Feb", "Mar", "Apr"];
    const datasets = {
      APAC: { "2023": [42000, 38500, 51200, 47800], "2024": [55000, 43200, 61500, 52900] },
      EMEA: { "2023": [31000, 29500, 35000, 33000], "2024": [38500, 31000, 40200, 36800] },
    };
    const regionColors = { APAC: C[0], EMEA: C[2] };
    let activeRegion = "APAC";

    const allVals = Object.values(datasets).flatMap(r => Object.values(r).flat());
    const x  = d3.scalePoint().domain(months).range([0, iw]).padding(0.4);
    const y  = d3.scaleLinear().domain([0, d3.max(allVals) * 1.12]).range([ih, 0]);
    const lg = d3.line().x((_, i) => x(months[i])).y(d => y(d)).curve(d3.curveMonotoneX);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g   = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    /* Axes */
    g.append("g").call(d3.axisLeft(y).ticks(5).tickFormat(d => `$${d / 1000}k`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick text").attr("font-size", 10).attr("fill", FG))
      .call(a => a.selectAll(".tick line")
        .clone().attr("x2", iw).attr("stroke", "#f0f0f0").attr("stroke-dasharray", "3,2"));
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x).tickSize(0))
      .call(a => a.select(".domain").attr("stroke", "#e0e0e0"))
      .call(a => a.selectAll(".tick text").attr("font-size", 11).attr("fill", FG).attr("dy", "1.4em"));

    /* Legend */
    const lgY = -36;
    [["#aaa", "2023 (prior year)", "5,3"], ["#333", "2024 (current year)", ""]].forEach(([col, lbl, dash], i) => {
      const lx = i * 160;
      g.append("line").attr("x1", lx).attr("x2", lx + 24)
        .attr("y1", lgY + 4).attr("y2", lgY + 4)
        .attr("stroke", col).attr("stroke-width", 2).attr("stroke-dasharray", dash);
      g.append("text").attr("x", lx + 28).attr("y", lgY + 8)
        .attr("font-size", 10).attr("fill", FG).text(lbl);
    });

    /* Region buttons (right side) */
    const btnArea = svg.append("g").attr("transform", `translate(${m.l + iw + 12},${m.t})`);
    Object.keys(datasets).forEach((region, ri) => {
      const by     = ri * 30;
      const col    = regionColors[region];
      const brect  = btnArea.append("rect").attr("y", by).attr("width", 72)
        .attr("height", 24).attr("rx", 4).attr("fill", col).style("cursor", "pointer");
      const btxt   = btnArea.append("text").attr("x", 36).attr("y", by + 15.5)
        .attr("text-anchor", "middle").attr("font-size", 11).attr("font-weight", "700")
        .attr("fill", "#fff").style("cursor", "pointer").text(region);
      [brect, btxt].forEach(sel => sel.on("click", () => {
        activeRegion = region;
        update();
        btnArea.selectAll("rect").attr("opacity", (_, j) =>
          Object.keys(datasets)[j] === activeRegion ? 0.9 : 0.3);
      }));
      brect.attr("opacity", ri === 0 ? 0.9 : 0.3);
    });

    /* Lines + dots groups */
    const path23  = g.append("path").attr("fill", "none").attr("stroke-width", 2).attr("stroke-dasharray", "5,3");
    const path24  = g.append("path").attr("fill", "none").attr("stroke-width", 2.5);
    const dotsG23 = g.append("g");
    const dotsG24 = g.append("g");

    function update() {
      const col  = regionColors[activeRegion];
      const v23  = datasets[activeRegion]["2023"];
      const v24  = datasets[activeRegion]["2024"];
      const pts  = months.map((mo, i) => ({ mo, i, v23: v23[i], v24: v24[i] }));

      path23.attr("d", lg(v23)).attr("stroke", col).attr("opacity", 0.45);
      path24.attr("d", lg(v24)).attr("stroke", col);

      dotsG23.selectAll("circle").data(pts).join("circle")
        .attr("cx", d => x(d.mo)).attr("cy", d => y(d.v23)).attr("r", 5)
        .attr("fill", col).attr("opacity", 0.5).attr("stroke", "#fff").attr("stroke-width", 1.5)
        .style("cursor", "pointer")
        .on("mouseover", (ev, d) => {
          const pct = ((d.v24 - d.v23) / d.v23 * 100).toFixed(1);
          const up  = d.v24 >= d.v23;
          tip.show(
            `<b>${d.mo} 2023</b><br>$${d.v23.toLocaleString()}<br>` +
            `YoY → 2024: <b style="color:${up ? "#26a69a" : "#ef5350"}">${up ? "+" : ""}${pct}%</b>`,
            ev.offsetX + 12, ev.offsetY - 44
          );
        })
        .on("mouseout", () => tip.hide());

      dotsG24.selectAll("circle").data(pts).join("circle")
        .attr("cx", d => x(d.mo)).attr("cy", d => y(d.v24)).attr("r", 7)
        .attr("fill", col).attr("stroke", "#fff").attr("stroke-width", 2)
        .style("cursor", "pointer")
        .on("mouseover", (ev, d) => {
          const pct = ((d.v24 - d.v23) / d.v23 * 100).toFixed(1);
          const up  = d.v24 >= d.v23;
          tip.show(
            `<b>${d.mo} 2024</b><br>$${d.v24.toLocaleString()}<br>` +
            `YoY vs 2023: <b style="color:${up ? "#26a69a" : "#ef5350"}">${up ? "+" : ""}${pct}%</b>`,
            ev.offsetX + 12, ev.offsetY - 44
          );
        })
        .on("mouseout", () => tip.hide());
    }

    update();
  }

  /* ══════════════════════════════════════════════════════════════════
   * 4. STRING AGGREGATION
   *    Left panel: raw rows (one row per tag).
   *    Right panel: COLLECT_SET grouped result.
   *    Hover either side to cross-highlight the matching order.
   * ══════════════════════════════════════════════════════════════════ */
  function renderStringAgg(el) {
    const rawRows = [
      { order_id: 101, tag: "express"  },
      { order_id: 101, tag: "gift"     },
      { order_id: 101, tag: "gift"     },  /* dup */
      { order_id: 102, tag: "express"  },
      { order_id: 103, tag: "standard" },
      { order_id: 103, tag: "fragile"  },
      { order_id: 104, tag: "express"  },
      { order_id: 104, tag: "fragile"  },
      { order_id: 104, tag: "priority" },
      { order_id: 105, tag: "standard" },
    ];
    const grouped = [
      { order_id: 101, tags: ["express", "gift"]                    },
      { order_id: 102, tags: ["express"]                            },
      { order_id: 103, tags: ["fragile", "standard"]                },
      { order_id: 104, tags: ["express", "fragile", "priority"]     },
      { order_id: 105, tags: ["standard"]                           },
    ];
    const orderC = { 101: C[0], 102: C[1], 103: C[2], 104: C[3], 105: C[4] };
    const tagC   = { express: C[0], gift: C[1], standard: C[2], fragile: C[3], priority: C[4] };

    let highlighted = null;

    el.innerHTML = "";
    el.style.cssText += "display:flex;gap:0;padding:12px 16px;";

    /* Left panel */
    const left = document.createElement("div");
    left.style.cssText = "flex:1;min-width:0;border-right:1px dashed #e0e0e0;padding-right:10px;";

    const leftTitle = document.createElement("div");
    leftTitle.style.cssText = `font-size:10px;font-weight:700;text-transform:uppercase;
      letter-spacing:0.6px;color:${GRAY};margin-bottom:8px;`;
    leftTitle.textContent = "Raw rows  (10 rows, one per tag)";
    left.appendChild(leftTitle);

    const rawEls = rawRows.map(row => {
      const div = document.createElement("div");
      div.style.cssText = `display:flex;gap:6px;align-items:center;padding:3px 6px;
        border-radius:3px;margin-bottom:2px;font-size:11px;cursor:pointer;
        transition:background 0.13s,opacity 0.13s;
        border-left:3px solid ${orderC[row.order_id]}40;`;
      div.dataset.order = row.order_id;

      const ordSpan = document.createElement("span");
      ordSpan.style.cssText = `width:26px;font-size:10px;font-weight:700;
        color:${orderC[row.order_id]};text-align:right;`;
      ordSpan.textContent = "#" + row.order_id;

      const tagSpan = document.createElement("span");
      tagSpan.style.cssText = `padding:1px 7px;border-radius:9px;font-size:10px;
        background:${(tagC[row.tag] || GRAY) + "22"};
        color:${tagC[row.tag] || GRAY};font-weight:600;`;
      tagSpan.textContent = row.tag;

      div.appendChild(ordSpan);
      div.appendChild(tagSpan);
      left.appendChild(div);

      div.addEventListener("mouseenter", () => { highlighted = row.order_id; applyHL(); });
      div.addEventListener("mouseleave", () => { highlighted = null;         applyHL(); });
      return div;
    });

    /* Arrow */
    const arrow = document.createElement("div");
    arrow.style.cssText = `display:flex;align-items:center;justify-content:center;
      padding:0 10px;font-size:20px;color:${C[0]};opacity:0.65;align-self:center;`;
    arrow.innerHTML = "&#8594;";

    /* Right panel */
    const right = document.createElement("div");
    right.style.cssText = "flex:1.2;min-width:0;padding-left:10px;";

    const rightTitle = document.createElement("div");
    rightTitle.style.cssText = `font-size:10px;font-weight:700;text-transform:uppercase;
      letter-spacing:0.6px;color:${GRAY};margin-bottom:8px;`;
    rightTitle.textContent = "COLLECT_SET  (5 rows, deduped)";
    right.appendChild(rightTitle);

    const groupEls = grouped.map(row => {
      const div = document.createElement("div");
      div.style.cssText = `display:flex;gap:6px;align-items:center;padding:5px 8px;
        border-radius:4px;margin-bottom:4px;font-size:11px;cursor:pointer;
        transition:all 0.13s;
        border-left:3px solid ${orderC[row.order_id]};
        background:${orderC[row.order_id]}10;`;
      div.dataset.order = row.order_id;

      const ordSpan = document.createElement("span");
      ordSpan.style.cssText = `width:26px;font-size:10px;font-weight:700;
        color:${orderC[row.order_id]};text-align:right;flex-shrink:0;`;
      ordSpan.textContent = "#" + row.order_id;

      const tagsWrap = document.createElement("span");
      tagsWrap.style.cssText = "display:flex;flex-wrap:wrap;gap:3px;";
      row.tags.forEach(t => {
        const sp = document.createElement("span");
        sp.style.cssText = `padding:1px 6px;border-radius:9px;font-size:10px;
          background:${(tagC[t] || GRAY) + "22"};color:${tagC[t] || GRAY};font-weight:600;`;
        sp.textContent = t;
        tagsWrap.appendChild(sp);
      });

      div.appendChild(ordSpan);
      div.appendChild(tagsWrap);
      right.appendChild(div);

      div.addEventListener("mouseenter", () => { highlighted = row.order_id; applyHL(); });
      div.addEventListener("mouseleave", () => { highlighted = null;         applyHL(); });
      return { el: div, id: row.order_id };
    });

    function applyHL() {
      rawEls.forEach(div => {
        const match = !highlighted || div.dataset.order == highlighted;
        div.style.opacity  = match ? "1" : "0.25";
        div.style.background = (match && highlighted)
          ? orderC[highlighted] + "18" : "";
      });
      groupEls.forEach(g => {
        const match = !highlighted || g.id == highlighted;
        g.el.style.opacity   = match ? "1" : "0.25";
        g.el.style.transform = (match && highlighted) ? "translateX(3px)" : "";
      });
    }

    el.appendChild(left);
    el.appendChild(arrow);
    el.appendChild(right);
  }

  /* ══════════════════════════════════════════════════════════════════
   * 5. CONDITIONAL AGGREGATION
   *    Grouped bar chart: x = region, grouped by category.
   *    Hover bar → revenue + % share tooltip.
   * ══════════════════════════════════════════════════════════════════ */
  function renderConditionalAgg(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 260;
    const m = { t: 50, r: 20, b: 52, l: 62 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const regions    = ["APAC", "EMEA", "AMER"];
    const categories = ["electronics", "clothing", "books"];
    const catC       = { electronics: C[0], clothing: C[1], books: C[2] };
    const tableData  = {
      APAC: { electronics: 2348, clothing: 125,  books:  35 },
      EMEA: { electronics:  249, clothing: 156,  books:  20 },
      AMER: { electronics: 1498, clothing:   0,  books:  45 },
    };

    const maxTotal = d3.max(regions.map(r => d3.sum(categories.map(c => tableData[r][c]))));
    const x0 = d3.scaleBand().domain(regions).range([0, iw]).paddingInner(0.28);
    const x1 = d3.scaleBand().domain(categories).range([0, x0.bandwidth()]).padding(0.06);
    const y  = d3.scaleLinear().domain([0, maxTotal * 1.1]).range([ih, 0]);

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g   = svg.append("g").attr("transform", `translate(${m.l},${m.t})`);
    const tip = makeTooltip(el);

    /* Axes */
    g.append("g").call(d3.axisLeft(y).ticks(5).tickFormat(d => `$${(d / 1000).toFixed(0)}k`))
      .call(a => a.select(".domain").remove())
      .call(a => a.selectAll(".tick text").attr("font-size", 10).attr("fill", FG))
      .call(a => a.selectAll(".tick line")
        .clone().attr("x2", iw).attr("stroke", "#f0f0f0").attr("stroke-dasharray", "3,2"));
    g.append("g").attr("transform", `translate(0,${ih})`)
      .call(d3.axisBottom(x0).tickSize(0))
      .call(a => a.select(".domain").attr("stroke", "#e0e0e0"))
      .call(a => a.selectAll(".tick text")
        .attr("font-size", 12).attr("font-weight", "700").attr("fill", FG).attr("dy", "1.4em"));

    /* Bars */
    regions.forEach(region => {
      const total = d3.sum(categories.map(c => tableData[region][c]));
      categories.forEach(cat => {
        const val = tableData[region][cat];
        const col = catC[cat];
        const bx  = x0(region) + x1(cat);
        const bw  = x1.bandwidth();

        g.append("rect")
          .attr("x", bx).attr("width", bw)
          .attr("y", y(val)).attr("height", ih - y(val))
          .attr("rx", 3).attr("fill", col).attr("opacity", 0.72)
          .style("cursor", "pointer")
          .on("mouseover", function (ev) {
            d3.select(this).attr("opacity", 0.95);
            const pct = total > 0 ? ((val / total) * 100).toFixed(1) : "0.0";
            tip.show(
              `<b>${region} — ${cat}</b><br>` +
              `Revenue: <b>$${val.toLocaleString()}</b><br>` +
              `Share of ${region} total: <b>${pct}%</b>`,
              ev.offsetX + 12, ev.offsetY - 48
            );
          })
          .on("mouseout", function () { d3.select(this).attr("opacity", 0.72); tip.hide(); });

        if (val > 50) {
          g.append("text")
            .attr("x", bx + bw / 2).attr("y", y(val) - 3)
            .attr("text-anchor", "middle").attr("font-size", 9).attr("fill", col)
            .text(`$${(val / 1000).toFixed(1)}k`);
        }
      });
    });

    /* Legend */
    const lgY = -32;
    categories.forEach((cat, i) => {
      const lx = i * 120;
      g.append("rect").attr("x", lx).attr("y", lgY - 6).attr("width", 12).attr("height", 12)
        .attr("rx", 2).attr("fill", catC[cat]).attr("opacity", 0.8);
      g.append("text").attr("x", lx + 16).attr("y", lgY + 4)
        .attr("font-size", 10).attr("fill", FG).text(cat);
    });
  }

  /* ══════════════════════════════════════════════════════════════════
   * 6. HIERARCHY — D3 tree layout
   *    Org chart with Eve as root.  Hover node → name / title / salary.
   * ══════════════════════════════════════════════════════════════════ */
  function renderHierarchy(el) {
    const W = Math.min(el.clientWidth || 700, 700), H = 290;
    const m = { t: 30, r: 20, b: 30, l: 20 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;

    const treeData = {
      id: 1, name: "Eve", title: "CEO", salary: 200000,
      children: [
        {
          id: 2, name: "Alice", title: "VP Eng", salary: 150000,
          children: [
            { id: 4, name: "Carol", title: "Sr Eng", salary: 95000,
              children: [{ id: 8, name: "Hank", title: "Jr Eng", salary: 60000 }] },
            { id: 5, name: "Dave",  title: "Eng",    salary: 92000 },
          ],
        },
        {
          id: 3, name: "Bob", title: "VP Sales", salary: 140000,
          children: [
            { id: 6, name: "Frank", title: "Acct Exec", salary: 70000 },
            { id: 7, name: "Grace", title: "Sales Rep",  salary: 68000 },
          ],
        },
      ],
    };

    const root   = d3.hierarchy(treeData);
    const layout = d3.tree().size([iw, ih - 40]);
    layout(root);

    const depthColors = [C[0], C[2], C[1], C[3]];

    const svg = d3.select(el).append("svg")
      .attr("width", "100%").attr("height", H)
      .attr("viewBox", `0 0 ${W} ${H}`).style("overflow", "visible");
    const g   = svg.append("g").attr("transform", `translate(${m.l},${m.t + 20})`);
    const tip = makeTooltip(el);

    /* Links */
    g.selectAll(".link").data(root.links()).join("path")
      .attr("class", "link").attr("fill", "none")
      .attr("stroke", "#dde1e7").attr("stroke-width", 1.8)
      .attr("d", d3.linkVertical().x(d => d.x).y(d => d.y));

    /* Node groups */
    const nodeGs = g.selectAll(".node").data(root.descendants()).join("g")
      .attr("class", "node")
      .attr("transform", d => `translate(${d.x},${d.y})`);

    /* Circle */
    nodeGs.append("circle").attr("r", 19)
      .attr("fill", d => depthColors[d.depth] || GRAY)
      .attr("opacity", 0.82)
      .attr("stroke", "#fff").attr("stroke-width", 2.5)
      .style("cursor", "pointer")
      .on("mouseover", function (ev, d) {
        d3.select(this).attr("r", 22).attr("opacity", 1);
        tip.show(
          `<b>${d.data.name}</b><br>${d.data.title}<br>` +
          `Salary: <b>$${d.data.salary.toLocaleString()}</b><br>` +
          `Depth: <b>${d.depth}</b>  |  Reports: <b>${(d.children || []).length}</b>`,
          ev.offsetX + 12, ev.offsetY - 58
        );
      })
      .on("mouseout", function () {
        d3.select(this).attr("r", 19).attr("opacity", 0.82);
        tip.hide();
      });

    /* Name inside circle */
    nodeGs.append("text")
      .attr("text-anchor", "middle").attr("dy", "0.35em")
      .attr("font-size", 10).attr("font-weight", "700").attr("fill", "#fff")
      .attr("pointer-events", "none")
      .text(d => d.data.name);

    /* Title below circle */
    nodeGs.append("text")
      .attr("text-anchor", "middle").attr("dy", "2.4em")
      .attr("font-size", 9).attr("fill", FG)
      .attr("pointer-events", "none")
      .text(d => d.data.title);

    /* Depth legend */
    const lgG = svg.append("g").attr("transform", `translate(${m.l}, ${H - 14})`);
    ["CEO / Root", "VPs (depth 1)", "ICs (depth 2)", "Juniors (depth 3)"].forEach((lbl, i) => {
      const lx = i * 158;
      lgG.append("circle").attr("cx", lx + 5).attr("cy", 0).attr("r", 5)
        .attr("fill", depthColors[i] || GRAY).attr("opacity", 0.82);
      lgG.append("text").attr("x", lx + 14).attr("y", 4)
        .attr("font-size", 9).attr("fill", FG).text(lbl);
    });
  }

  /* ── Router ──────────────────────────────────────────────────────── */
  const VIZ_MAP = {
    "viz-pagination":      renderPagination,
    "viz-gaps-islands":    renderGapsIslands,
    "viz-period-compare":  renderPeriodCompare,
    "viz-string-agg":      renderStringAgg,
    "viz-conditional-agg": renderConditionalAgg,
    "viz-hierarchy":       renderHierarchy,
  };

  function init() {
    for (const [id, fn] of Object.entries(VIZ_MAP)) {
      const el = document.getElementById(id);
      if (el) { el.innerHTML = ""; fn(el); }
    }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
