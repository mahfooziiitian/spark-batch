/**
 * join-viz.js
 * D3 v7 interactive visualization for the Join Types pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Renders two labeled row-lists (left/right table) color-coded by whether each
 * row survives into the join output, plus the resulting output table below.
 * This is intentionally DOM/table based (not SVG line-drawing) so it stays
 * robust across theme switches, resizes, and instant-navigation re-renders.
 *
 * Row status legend (used for both left/right rows):
 *   "match"    — row has a counterpart on the other side AND is in the output (solid teal)
 *   "kept"     — row has NO counterpart but is still in the output, NULL-filled (solid amber)
 *   "dropped"  — row is not present in the output at all (faded/dashed gray)
 *   "probe"    — row participates in matching but its own columns never appear
 *                in the output (used for the right side of SEMI/ANTI joins) (dashed purple)
 *
 * Viz catalogue: see VIZ_MAP at the bottom of this file.
 */
(function () {
  "use strict";

  const TEAL   = "#26a69a";
  const AMBER  = "#ffa726";
  const GRAY   = "#90a4ae";
  const PURPLE = "#ab47bc";
  const RED    = "#ef5350";

  const STATUS_COLOR = { match: TEAL, kept: AMBER, dropped: GRAY, probe: PURPLE, discard: RED };
  const STATUS_LABEL = {
    match: "in output — matched",
    kept: "in output — no match (NULL-filled)",
    dropped: "not in output",
    probe: "match-checked only — never projected",
    discard: "matched \u2192 discarded (anti join)",
  };

  function isDarkMode() {
    return document.documentElement.getAttribute("data-md-color-scheme") === "slate";
  }

  function renderJoinViz(el, cfg) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";
    const CARD_BG = dark ? "#263238" : "#ffffff";
    const PANEL_BG = dark ? "#1c272b" : "#f5f7f8";
    const BORDER = dark ? "#37474f" : "#dde3e6";

    const root = d3.select(el)
      .style("font-family", "inherit")
      .style("font-size", "13px")
      .style("color", TEXT);

    if (cfg.title) {
      root.append("div")
        .style("font-weight", "700")
        .style("margin-bottom", "8px")
        .style("color", cfg.color || TEAL)
        .text(cfg.title);
    }

    const tablesRow = root.append("div")
      .style("display", "flex")
      .style("gap", "16px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "14px");

    function renderSide(title, rows) {
      const col = tablesRow.append("div")
        .style("flex", "1 1 260px")
        .style("min-width", "220px");

      col.append("div")
        .style("font-weight", "600")
        .style("margin-bottom", "6px")
        .text(title);

      const list = col.append("div")
        .style("display", "flex")
        .style("flex-direction", "column")
        .style("gap", "4px");

      list.selectAll("div.jv-row").data(rows).join("div")
        .attr("class", "jv-row")
        .style("border", d => `2px ${d.status === "dropped" || d.status === "probe" ? "dashed" : "solid"} ${STATUS_COLOR[d.status] || GRAY}`)
        .style("background", CARD_BG)
        .style("border-radius", "6px")
        .style("padding", "5px 10px")
        .style("opacity", d => (d.status === "dropped" ? 0.55 : 1))
        .style("display", "flex")
        .style("justify-content", "space-between")
        .style("align-items", "center")
        .style("gap", "8px")
        .html(d => `<span>${d.label}</span>`)
        .append("span")
        .style("font-size", "10.5px")
        .style("font-weight", "700")
        .style("color", d => STATUS_COLOR[d.status] || GRAY)
        .style("white-space", "nowrap")
        .text(d => d.badge !== undefined ? d.badge : "");
    }

    renderSide(cfg.leftTitle, cfg.leftRows);
    renderSide(cfg.rightTitle, cfg.rightRows);

    /* ── Legend ─────────────────────────────────────────────────── */
    const usedStatuses = Array.from(new Set([...cfg.leftRows, ...cfg.rightRows].map(r => r.status)));
    const legend = root.append("div")
      .style("display", "flex")
      .style("flex-wrap", "wrap")
      .style("gap", "12px")
      .style("font-size", "11px")
      .style("margin-bottom", "14px")
      .style("color", TEXT);

    legend.selectAll("span.jv-legend").data(usedStatuses).join("span")
      .attr("class", "jv-legend")
      .style("display", "inline-flex")
      .style("align-items", "center")
      .style("gap", "5px")
      .html(s => `<span style="width:10px;height:10px;border-radius:3px;background:${STATUS_COLOR[s] || GRAY};display:inline-block;"></span>${STATUS_LABEL[s] || s}`);

    /* ── Output table ───────────────────────────────────────────── */
    const outWrap = root.append("div")
      .style("background", PANEL_BG)
      .style("border", `1px solid ${BORDER}`)
      .style("border-radius", "6px")
      .style("padding", "10px 12px");

    outWrap.append("div")
      .style("font-weight", "700")
      .style("margin-bottom", "6px")
      .style("color", cfg.color || TEAL)
      .text(`Output (${cfg.resultRows.length} row${cfg.resultRows.length === 1 ? "" : "s"})`);

    if (cfg.resultRows.length === 0) {
      outWrap.append("div").style("font-style", "italic").style("color", GRAY).text("— empty result set —");
    } else {
      const tbl = outWrap.append("div").style("overflow-x", "auto").append("table")
        .style("border-collapse", "collapse")
        .style("width", "100%")
        .style("font-size", "12px");

      tbl.append("thead").append("tr")
        .selectAll("th").data(cfg.resultCols).join("th")
        .style("text-align", "left")
        .style("padding", "4px 10px")
        .style("border-bottom", `2px solid ${BORDER}`)
        .style("color", cfg.color || TEAL)
        .text(c => c);

      tbl.append("tbody")
        .selectAll("tr").data(cfg.resultRows).join("tr")
        .selectAll("td").data(row => row).join("td")
        .style("padding", "4px 10px")
        .style("border-bottom", `1px solid ${BORDER}`)
        .html(v => (v === null || v === "NULL") ? '<span style="color:#ef5350;font-style:italic;">NULL</span>' : String(v));
    }

    if (cfg.note) {
      root.append("div")
        .style("margin-top", "10px")
        .style("font-size", "12px")
        .style("color", TEXT)
        .html(cfg.note);
    }
  }

  /* ══════════════════════════════════════════════════════════════
   * Per-page configurations
   * ══════════════════════════════════════════════════════════════ */

  function vizInner(el) {
    renderJoinViz(el, {
      title: "orders JOIN customers ON customer_id",
      color: TEAL,
      leftTitle: "orders (left)",
      rightTitle: "customers (right)",
      leftRows: [
        { label: "order_id=1, customer_id=101", status: "match" },
        { label: "order_id=2, customer_id=102", status: "match" },
        { label: "order_id=3, customer_id=105 <em>(no customer 105)</em>", status: "dropped" },
      ],
      rightRows: [
        { label: "customer_id=101, Alice", status: "match" },
        { label: "customer_id=102, Bob", status: "match" },
        { label: "customer_id=103, Charlie <em>(no orders)</em>", status: "dropped" },
      ],
      resultCols: ["order_id", "customer_id", "name"],
      resultRows: [
        [1, 101, "Alice"],
        [2, 102, "Bob"],
      ],
      note: "Only rows with a match on <strong>both</strong> sides survive — order 3 (unknown customer) and customer Charlie (no orders) are both dropped.",
    });
  }

  function vizLeft(el) {
    renderJoinViz(el, {
      title: "Customers LEFT JOIN Orders ON id",
      color: AMBER,
      leftTitle: "Customers (left, ALL rows kept)",
      rightTitle: "Orders (right)",
      leftRows: [
        { label: "id=1, Alice", status: "kept" },
        { label: "id=2, Bob", status: "match" },
        { label: "id=3, Carol", status: "match" },
      ],
      rightRows: [
        { label: "id=2, Laptop", status: "match" },
        { label: "id=3, Phone", status: "match" },
      ],
      resultCols: ["id", "name", "product"],
      resultRows: [
        [1, "Alice", "NULL"],
        [2, "Bob", "Laptop"],
        [3, "Carol", "Phone"],
      ],
      note: "Every left row is kept. Alice has no order, so <code>product</code> is <strong>NULL</strong> instead of the row being dropped.",
    });
  }

  function vizRight(el) {
    renderJoinViz(el, {
      title: "Customers RIGHT JOIN Orders ON id (mirror of LEFT JOIN)",
      color: AMBER,
      leftTitle: "Customers (left)",
      rightTitle: "Orders (right, ALL rows kept)",
      leftRows: [
        { label: "id=1, Alice <em>(no order)</em>", status: "dropped" },
        { label: "id=2, Bob", status: "match" },
        { label: "id=3, Carol", status: "match" },
      ],
      rightRows: [
        { label: "id=2, Laptop", status: "match" },
        { label: "id=3, Phone", status: "match" },
        { label: "id=4, Tablet <em>(no customer)</em>", status: "kept" },
      ],
      resultCols: ["id", "name", "product"],
      resultRows: [
        [2, "Bob", "Laptop"],
        [3, "Carol", "Phone"],
        [4, "NULL", "Tablet"],
      ],
      note: "Every right row is kept. The Tablet order (id=4) has no matching customer, so <code>name</code> is <strong>NULL</strong> — Alice (no order) is simply absent, not NULL-filled.",
    });
  }

  function vizFull(el) {
    renderJoinViz(el, {
      title: "orders FULL OUTER JOIN customers ON customer_id",
      color: "#7c4dff",
      leftTitle: "orders (left, ALL rows kept)",
      rightTitle: "customers (right, ALL rows kept)",
      leftRows: [
        { label: "order_id=1, customer_id=101", status: "match" },
        { label: "order_id=4, customer_id=105 <em>(unknown)</em>", status: "kept" },
      ],
      rightRows: [
        { label: "customer_id=101, Alice", status: "match" },
        { label: "customer_id=103, Charlie <em>(no orders)</em>", status: "kept" },
      ],
      resultCols: ["customer_id", "name", "order_id"],
      resultRows: [
        [101, "Alice", 1],
        [103, "Charlie", "NULL"],
        [105, "NULL", 4],
      ],
      note: "Nothing is dropped from either side — unmatched rows from <strong>both</strong> tables appear, NULL-filled on whichever side has no match.",
    });
  }

  function vizSemi(el) {
    renderJoinViz(el, {
      title: "customers LEFT SEMI JOIN orders ON customer_id",
      color: TEAL,
      leftTitle: "customers (left)",
      rightTitle: "orders (right — probe only, never projected)",
      leftRows: [
        { label: "101, Alice — has orders", status: "match" },
        { label: "102, Bob — has orders", status: "match" },
        { label: "104, Diana — no orders", status: "dropped" },
      ],
      rightRows: [
        { label: "order 1, customer_id=101", status: "probe" },
        { label: "order 2, customer_id=102", status: "probe" },
        { label: "order 3, customer_id=101 <em>(2nd order, no dup output)</em>", status: "probe" },
      ],
      resultCols: ["customer_id", "name"],
      resultRows: [
        [101, "Alice"],
        [102, "Bob"],
      ],
      note: "Alice has <strong>two</strong> matching orders but still appears only <strong>once</strong> — semi join is an existence filter, not a data merge, and never returns right-side columns.",
    });
  }

  function vizAnti(el) {
    renderJoinViz(el, {
      title: "department LEFT ANTI JOIN employee ON department_name",
      color: RED,
      leftTitle: "department (left)",
      rightTitle: "employee (right — probe only, never projected)",
      leftRows: [
        { label: "IT — has employees", status: "discard" },
        { label: "HR — has employees", status: "discard" },
        { label: "Admin — no employees", status: "kept" },
      ],
      rightRows: [
        { label: "John Doe, department=IT", status: "probe" },
        { label: "Jane Smith, department=HR", status: "probe" },
      ],
      resultCols: ["department_id", "department_name"],
      resultRows: [[4, "Admin"]],
      note: "The mirror image of semi join: rows <strong>with</strong> a match are discarded, only unmatched left rows (Admin) survive.",
    });
  }

  function vizCross(el) {
    const rows = [];
    for (const p of ["Widget", "Gadget"]) {
      for (const r of ["US", "EU", "APAC"]) rows.push([p, r]);
    }
    renderJoinViz(el, {
      title: "products CROSS JOIN regions — every combination",
      color: "#8d6e63",
      leftTitle: "products (2 rows)",
      rightTitle: "regions (3 rows)",
      leftRows: [
        { label: "Widget", status: "match" },
        { label: "Gadget", status: "match" },
      ],
      rightRows: [
        { label: "US", status: "match" },
        { label: "EU", status: "match" },
        { label: "APAC", status: "match" },
      ],
      resultCols: ["product", "region"],
      resultRows: rows,
      note: "No <code>ON</code> clause — every left row is paired with every right row: 2 × 3 = <strong>6</strong> output rows. Grows multiplicatively, not additively.",
    });
  }

  function vizNonEqui(el) {
    renderJoinViz(el, {
      title: "txns JOIN pricing_tiers ON amount BETWEEN min_amount AND max_amount",
      color: "#5c6bc0",
      leftTitle: "txns (left)",
      rightTitle: "pricing_tiers (right)",
      leftRows: [
        { label: "txn 1, amount=45.00", status: "match" },
        { label: "txn 2, amount=150.00", status: "match" },
        { label: "txn 5, amount=1200.00", status: "match" },
      ],
      rightRows: [
        { label: "Bronze [0, 99.99]", status: "match" },
        { label: "Silver [100, 499.99]", status: "match" },
        { label: "Platinum [1000, 9999.99]", status: "match" },
      ],
      resultCols: ["txn_id", "amount", "tier", "discount_pct"],
      resultRows: [
        [1, 45.00, "Bronze", 0.0],
        [2, 150.00, "Silver", 5.0],
        [5, 1200.00, "Platinum", 15.0],
      ],
      note: "The match condition is a <strong>range</strong> (<code>BETWEEN</code>), not equality — Spark cannot hash-partition on it and typically falls back to sort-merge or nested-loop execution.",
    });
  }

  function vizPointInInterval(el) {
    renderJoinViz(el, {
      title: "students_rj JOIN grade_range ON score BETWEEN min_score AND max_score",
      color: "#5c6bc0",
      leftTitle: "students_rj (points)",
      rightTitle: "grade_range (intervals)",
      leftRows: [
        { label: "Alice, score=55", status: "match" },
        { label: "Bob, score=75", status: "match" },
        { label: "Charlie, score=85", status: "match" },
      ],
      rightRows: [
        { label: "C [50, 69]", status: "match" },
        { label: "B [70, 84]", status: "match" },
        { label: "A [85, 100]", status: "match" },
      ],
      resultCols: ["name", "score", "grade"],
      resultRows: [
        ["Alice", 55, "C"],
        ["Bob", 75, "B"],
        ["Charlie", 85, "A"],
      ],
      note: "Each <strong>point</strong> value (score) is matched against the <strong>interval</strong> it falls inside — a classic point-in-interval range join.",
    });
  }

  function vizIntervalOverlap(el) {
    renderJoinViz(el, {
      title: "events_tbl JOIN availability_tbl ON start &lt; other.end AND end &gt; other.start",
      color: "#5c6bc0",
      leftTitle: "events_tbl",
      rightTitle: "availability_tbl",
      leftRows: [
        { label: "Event A, 08:00\u201310:00", status: "match" },
        { label: "Event B, 09:00\u201311:00", status: "match" },
        { label: "Event C, 12:00\u201314:00 <em>(touches, doesn't overlap)</em>", status: "dropped" },
        { label: "Event D, 13:00\u201315:00", status: "match" },
      ],
      rightRows: [
        { label: "avail 1, 07:00\u201309:30", status: "match" },
        { label: "avail 2, 09:00\u201311:30", status: "match" },
        { label: "avail 3, 10:00\u201312:00", status: "match" },
        { label: "avail 4, 14:00\u201316:00", status: "match" },
      ],
      resultCols: ["event_name", "event_window", "avail_id", "avail_window"],
      resultRows: [
        ["Event A", "08:00-10:00", 1, "07:00-09:30"],
        ["Event A", "08:00-10:00", 2, "09:00-11:30"],
        ["Event B", "09:00-11:00", 1, "07:00-09:30"],
        ["Event B", "09:00-11:00", 2, "09:00-11:30"],
        ["Event B", "09:00-11:00", 3, "10:00-12:00"],
        ["Event D", "13:00-15:00", 4, "14:00-16:00"],
      ],
      note: "Two intervals match whenever they <strong>overlap at all</strong> (strict <code>&lt;</code>/<code>&gt;</code>) — a single event can produce multiple output rows, and intervals that merely <em>touch</em> at an endpoint (Event C vs avail 3) do <strong>not</strong> count as overlapping.",
    });
  }

  function vizLateral(el) {
    renderJoinViz(el, {
      title: "customers JOIN LATERAL (SELECT ... WHERE customer_id = c.customer_id ORDER BY order_date DESC LIMIT 1)",
      color: "#26c6da",
      leftTitle: "customers (left, drives the subquery)",
      rightTitle: "correlated subquery result (per left row)",
      leftRows: [
        { label: "101, Alice \u2192 runs subquery WHERE customer_id=101", status: "match" },
        { label: "102, Bob \u2192 runs subquery WHERE customer_id=102", status: "match" },
      ],
      rightRows: [
        { label: "Alice's latest order only (LIMIT 1 of N)", status: "match" },
        { label: "Bob's latest order only (LIMIT 1 of N)", status: "match" },
      ],
      resultCols: ["customer_id", "name", "order_id", "order_date"],
      resultRows: [
        [101, "Alice", 7, "2024-06-01"],
        [102, "Bob", 4, "2024-05-20"],
      ],
      note: "Unlike a normal join, the right side is a <strong>subquery re-evaluated per left row</strong> — here it's correlated on <code>customer_id</code> and capped with <code>LIMIT 1</code>.",
    });
  }

  function vizTypesOverview(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";
    const rows = [
      { type: "Inner", left: "match", right: "match", cols: "Left + Right" },
      { type: "Left Outer", left: "kept", right: "match", cols: "Left + Right (NULLs)" },
      { type: "Right Outer", left: "match", right: "kept", cols: "Left + Right (NULLs)" },
      { type: "Full Outer", left: "kept", right: "kept", cols: "Left + Right (NULLs)" },
      { type: "Left Semi", left: "match", right: "probe", cols: "Left only" },
      { type: "Left Anti", left: "kept", right: "probe", cols: "Left only" },
      { type: "Cross", left: "match", right: "match", cols: "Left + Right (all combos)" },
    ];
    const root = d3.select(el).style("font-family", "inherit").style("font-size", "13px").style("color", TEXT);
    const tbl = root.append("div").style("overflow-x", "auto").append("table")
      .style("border-collapse", "collapse").style("width", "100%").style("font-size", "12.5px");
    const border = dark ? "#37474f" : "#dde3e6";
    const head = tbl.append("thead").append("tr");
    ["Join Type", "Left rows", "Right rows", "Output columns"].forEach(h => {
      head.append("th").style("text-align", "left").style("padding", "6px 12px")
        .style("border-bottom", `2px solid ${border}`).style("color", TEAL).text(h);
    });
    const body = tbl.append("tbody").selectAll("tr").data(rows).join("tr");
    body.append("td").style("padding", "6px 12px").style("border-bottom", `1px solid ${border}`).style("font-weight", "600").text(d => d.type);
    body.append("td").style("padding", "6px 12px").style("border-bottom", `1px solid ${border}`)
      .html(d => `<span style="color:${STATUS_COLOR[d.left]};font-weight:700;">\u25CF</span> ${d.left === "kept" ? "all rows" : d.left === "probe" ? "n/a" : "matched only"}`);
    body.append("td").style("padding", "6px 12px").style("border-bottom", `1px solid ${border}`)
      .html(d => `<span style="color:${STATUS_COLOR[d.right]};font-weight:700;">\u25CF</span> ${d.right === "kept" ? "all rows" : d.right === "probe" ? "match-checked only" : "matched only"}`);
    body.append("td").style("padding", "6px 12px").style("border-bottom", `1px solid ${border}`).text(d => d.cols);
  }

  function vizPointInIntervalScenarios(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";
    const BORDER = dark ? "#37474f" : "#dde3e6";
    const AXIS = dark ? "#607d8b" : "#90a4ae";
    const NOMATCH = RED;

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 46, PAD_R = 16;
    const scaleX = d3.scaleLinear().domain([-10, 110]).range([PAD_L, W - PAD_R]);

    const root = d3.select(el)
      .style("font-family", "inherit")
      .style("font-size", "13px")
      .style("color", TEXT);

    /* ── Scenario 1: single-band coverage — inside / boundary / gap ────── */
    root.append("div")
      .style("font-weight", "700")
      .style("margin-bottom", "6px")
      .style("color", TEAL)
      .text("Scenario 1–3: point INSIDE a band, ON a boundary, and in a GAP (no match)");

    const bands = [
      { grade: "F", min: 0, max: 34, color: "#90a4ae" },
      { grade: "D", min: 35, max: 49, color: "#ff7043" },
      { grade: "C", min: 50, max: 69, color: TEAL },
      { grade: "B", min: 70, max: 84, color: "#42a5f5" },
      { grade: "A", min: 85, max: 100, color: "#7c4dff" },
    ];
    // "no match" points deliberately fall in gaps/out-of-domain relative to grade_range.
    const points1 = [
      { label: "Alice 55", score: 55, match: true },
      { label: "Diana 65", score: 65, match: true },
      { label: "Eva 70 (on B lower bound)", score: 70, match: true },
      { label: "Charlie 85 (on A lower bound)", score: 85, match: true },
      { label: "Frank 90", score: 90, match: true },
      { label: "Gap-below \u22125 (no match)", score: -5, match: false },
      { label: "Gap-above 105 (no match)", score: 105, match: false },
    ];

    const H1 = 150;
    const svg1 = root.append("svg").attr("width", W).attr("height", H1).style("display", "block").style("margin-bottom", "18px");

    const bandY = 46, bandH = 26;
    svg1.selectAll("rect.band").data(bands).join("rect")
      .attr("class", "band")
      .attr("x", d => scaleX(d.min))
      .attr("y", bandY)
      .attr("width", d => scaleX(d.max) - scaleX(d.min))
      .attr("height", bandH)
      .attr("fill", d => d.color)
      .attr("opacity", 0.85)
      .attr("rx", 3);

    svg1.selectAll("text.bandLabel").data(bands).join("text")
      .attr("class", "bandLabel")
      .attr("x", d => (scaleX(d.min) + scaleX(d.max)) / 2)
      .attr("y", bandY + bandH / 2 + 4)
      .attr("text-anchor", "middle")
      .style("font-size", "11px")
      .style("font-weight", "700")
      .style("fill", "#fff")
      .text(d => `${d.grade} [${d.min}\u2013${d.max}]`);

    // Axis line + boundary ticks (0,34,35,49,50,69,70,84,85,100)
    svg1.append("line")
      .attr("x1", scaleX(-10)).attr("x2", scaleX(110))
      .attr("y1", bandY + bandH + 10).attr("y2", bandY + bandH + 10)
      .attr("stroke", AXIS).attr("stroke-width", 1);
    const ticks = [0, 34, 35, 49, 50, 69, 70, 84, 85, 100];
    svg1.selectAll("text.tick").data(ticks).join("text")
      .attr("class", "tick")
      .attr("x", d => scaleX(d))
      .attr("y", bandY + bandH + 24)
      .attr("text-anchor", "middle")
      .style("font-size", "9px")
      .style("fill", AXIS)
      .text(d => d);

    // Point markers with connector line + label.
    const pointY = bandY - 26;
    const g1 = svg1.selectAll("g.pt").data(points1).join("g").attr("class", "pt");
    g1.append("line")
      .attr("x1", d => scaleX(d.score)).attr("x2", d => scaleX(d.score))
      .attr("y1", pointY + 8).attr("y2", d => d.match ? bandY : bandY + bandH)
      .attr("stroke", d => d.match ? TEAL : NOMATCH)
      .attr("stroke-dasharray", d => d.match ? "0" : "3,2")
      .attr("stroke-width", 1.5);
    g1.append("circle")
      .attr("cx", d => scaleX(d.score)).attr("cy", pointY)
      .attr("r", 5)
      .attr("fill", d => d.match ? TEAL : NOMATCH)
      .attr("stroke", "#fff").attr("stroke-width", 1.2);
    g1.append("text")
      .attr("x", d => scaleX(d.score)).attr("y", pointY - 10)
      .attr("text-anchor", "middle")
      .style("font-size", "10px")
      .style("font-weight", d => d.match ? "400" : "700")
      .style("fill", d => d.match ? TEXT : NOMATCH)
      .text(d => d.label);

    /* ── Scenario 4: overlapping bands — a point matching MULTIPLE ranges ─ */
    root.append("div")
      .style("font-weight", "700")
      .style("margin", "4px 0 6px")
      .style("color", "#ab47bc")
      .text("Scenario 4: OVERLAPPING ranges — one point matches multiple bands (fan-out)");

    const overlapBands = [
      { label: "Promo A [40\u201380]", min: 40, max: 80, color: "#7c4dff", row: 0 },
      { label: "Promo B [60\u2013100]", min: 60, max: 100, color: "#ffa726", row: 1 },
    ];
    const H2 = 140;
    const svg2 = root.append("svg").attr("width", W).attr("height", H2).style("display", "block");

    const rowY = i => 30 + i * 30;
    const rowH = 22;

    // Shade the overlap region.
    const ovMin = Math.max(overlapBands[0].min, overlapBands[1].min);
    const ovMax = Math.min(overlapBands[0].max, overlapBands[1].max);
    svg2.append("rect")
      .attr("x", scaleX(ovMin)).attr("y", rowY(0))
      .attr("width", scaleX(ovMax) - scaleX(ovMin))
      .attr("height", rowY(1) + rowH - rowY(0))
      .attr("fill", dark ? "#4a3b5c" : "#f3e5f5")
      .attr("stroke", "#ab47bc").attr("stroke-dasharray", "3,2");

    svg2.selectAll("rect.ovband").data(overlapBands).join("rect")
      .attr("class", "ovband")
      .attr("x", d => scaleX(d.min)).attr("y", d => rowY(d.row))
      .attr("width", d => scaleX(d.max) - scaleX(d.min)).attr("height", rowH)
      .attr("fill", d => d.color).attr("opacity", 0.85).attr("rx", 3);

    svg2.selectAll("text.ovlabel").data(overlapBands).join("text")
      .attr("class", "ovlabel")
      .attr("x", d => scaleX(d.min) + 6).attr("y", d => rowY(d.row) + rowH / 2 + 4)
      .style("font-size", "10.5px").style("font-weight", "700").style("fill", "#fff")
      .text(d => d.label);

    // The demo point sits inside the shaded overlap -> matches both bands.
    const ovScore = 70;
    const ovPointY = rowY(1) + rowH + 30;
    svg2.append("line")
      .attr("x1", scaleX(ovScore)).attr("x2", scaleX(ovScore))
      .attr("y1", rowY(0)).attr("y2", ovPointY - 8)
      .attr("stroke", "#ab47bc").attr("stroke-width", 1.5).attr("stroke-dasharray", "2,2");
    svg2.append("circle")
      .attr("cx", scaleX(ovScore)).attr("cy", ovPointY).attr("r", 5)
      .attr("fill", "#ab47bc").attr("stroke", "#fff").attr("stroke-width", 1.2);
    svg2.append("text")
      .attr("x", scaleX(ovScore)).attr("y", ovPointY + 16)
      .attr("text-anchor", "middle")
      .style("font-size", "10px").style("fill", TEXT)
      .text(`Product X, price=${ovScore} \u2192 2 output rows`);

    root.append("div")
      .style("margin-top", "10px")
      .style("font-size", "12px")
      .style("color", TEXT)
      .html(
        "<strong>Scenario reference:</strong> " +
        "<span style=\"color:" + TEAL + "\">\u25CF inside a band</span> and " +
        "<span style=\"color:" + TEAL + "\">\u25CF on an inclusive boundary</span> both produce exactly " +
        "<strong>1</strong> output row; a point in a <span style=\"color:" + NOMATCH + "\">\u25CF gap</span> " +
        "(below the lowest band or above the highest, or between two non-adjacent bands) produces " +
        "<strong>0</strong> rows — the row is silently dropped by an <code>INNER</code> range join; " +
        "and a point inside <span style=\"color:#ab47bc\">\u25CF overlapping bands</span> produces " +
        "<strong>N</strong> output rows (one per matching band) — a fan-out, exactly like the " +
        "<a href=\"../../../../issues/data_explosion/\">data explosion</a> issue on an equi-join."
      );
  }

  function vizPointInIntervalInequality(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";
    const BORDER = dark ? "#37474f" : "#dde3e6";
    const HEAD_BG = dark ? "#26323a" : "#eef3f5";
    const NOMATCH = RED;

    const students = [
      { id: 1, name: "Alice", score: 55, inclusive: "C", exclusive: "C", halfOpen: "C" },
      { id: 2, name: "Bob", score: 75, inclusive: "B", exclusive: "B", halfOpen: "B" },
      { id: 3, name: "Charlie", score: 85, inclusive: "A", exclusive: null, halfOpen: "A" },
      { id: 4, name: "Diana", score: 65, inclusive: "C", exclusive: "C", halfOpen: "C" },
      { id: 5, name: "Eva", score: 70, inclusive: "B", exclusive: null, halfOpen: "B" },
      { id: 6, name: "Frank", score: 90, inclusive: "A", exclusive: "A", halfOpen: "A" },
    ];
    const cols = [
      { key: "inclusive", label: "BETWEEN / >= AND <=", sub: "closed–closed" },
      { key: "exclusive", label: "> AND <", sub: "open–open" },
      { key: "halfOpen", label: ">= AND <", sub: "closed–open" },
    ];

    const root = d3.select(el)
      .style("font-family", "inherit")
      .style("font-size", "13px")
      .style("color", TEXT);

    const table = root.append("table")
      .style("width", "100%")
      .style("border-collapse", "collapse");

    const thead = table.append("thead").append("tr");
    thead.append("th").style("text-align", "left").style("padding", "6px 8px")
      .style("border-bottom", `2px solid ${BORDER}`).style("background", HEAD_BG).text("Student (score)");
    thead.selectAll("th.op").data(cols).join("th")
      .attr("class", "op")
      .style("text-align", "center").style("padding", "6px 8px")
      .style("border-bottom", `2px solid ${BORDER}`).style("background", HEAD_BG)
      .html(d => `<div style="font-weight:700;">${d.label}</div><div style="font-weight:400;font-size:10.5px;opacity:.75;">${d.sub}</div>`);

    const rows = table.append("tbody").selectAll("tr").data(students).join("tr");
    rows.append("td")
      .style("padding", "6px 8px").style("border-bottom", `1px solid ${BORDER}`)
      .text(d => `${d.name} (${d.score})`);
    cols.forEach(col => {
      rows.append("td")
        .style("text-align", "center").style("padding", "6px 8px")
        .style("border-bottom", `1px solid ${BORDER}`)
        .style("font-weight", "700")
        .style("color", d => d[col.key] ? TEAL : NOMATCH)
        .text(d => d[col.key] ? d[col.key] : "\u2014 no match");
    });

    root.append("div")
      .style("margin-top", "8px")
      .style("font-size", "11.5px")
      .style("opacity", "0.85")
      .text("Boundary scores (70, 85) flip from matched to unmatched only under the open–open (exclusive) form.");
  }

  function vizPointInIntervalFixedLength(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";
    const AXIS = dark ? "#607d8b" : "#90a4ae";
    const BUCKET_COLORS = ["#26a69a", "#42a5f5", "#7c4dff", "#ffa726", "#ef5350"];

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 46, PAD_R = 16;
    // Domain: minutes elapsed since 09:00, covering five 15-minute buckets (09:00-10:15).
    const scaleX = d3.scaleLinear().domain([0, 75]).range([PAD_L, W - PAD_R]);
    const fmt = m => {
      const h = 9 + Math.floor(m / 60), mm = m % 60;
      return `${String(h).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
    };

    const buckets = [0, 15, 30, 45, 60].map((start, i) => ({
      id: i + 1, start, end: start + 15, color: BUCKET_COLORS[i],
    }));
    const readings = [
      { min: 3, value: 21.4 }, { min: 12, value: 21.6 }, { min: 29, value: 22.1 },
      { min: 31, value: 22.3 }, { min: 47, value: 21.9 }, { min: 65, value: 20.8 },
    ].map(r => ({ ...r, bucket: buckets.find(b => r.min >= b.start && r.min < b.end) }));

    const root = d3.select(el)
      .style("font-family", "inherit")
      .style("font-size", "13px")
      .style("color", TEXT);

    const H = 190;
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    const bandY = 70, bandH = 26;
    svg.selectAll("rect.bucket").data(buckets).join("rect")
      .attr("class", "bucket")
      .attr("x", d => scaleX(d.start)).attr("y", bandY)
      .attr("width", d => scaleX(d.end) - scaleX(d.start)).attr("height", bandH)
      .attr("fill", d => d.color).attr("opacity", 0.85)
      .attr("stroke", dark ? "#0b1215" : "#fff").attr("stroke-width", 1);

    svg.selectAll("text.bucketLabel").data(buckets).join("text")
      .attr("class", "bucketLabel")
      .attr("x", d => (scaleX(d.start) + scaleX(d.end)) / 2)
      .attr("y", bandY + bandH / 2 + 4)
      .attr("text-anchor", "middle")
      .style("font-size", "10.5px").style("font-weight", "700").style("fill", "#fff")
      .text(d => `bucket ${d.id}`);

    svg.selectAll("text.bucketRange").data(buckets).join("text")
      .attr("class", "bucketRange")
      .attr("x", d => (scaleX(d.start) + scaleX(d.end)) / 2)
      .attr("y", bandY + bandH + 16)
      .attr("text-anchor", "middle")
      .style("font-size", "9.5px").style("fill", AXIS)
      .text(d => `[${fmt(d.start)}, ${fmt(d.end)})`);

    // Axis line (fixed-width buckets — every tick evenly spaced by construction).
    svg.append("line")
      .attr("x1", scaleX(0)).attr("x2", scaleX(75))
      .attr("y1", bandY + bandH).attr("y2", bandY + bandH)
      .attr("stroke", "none");

    // Reading markers with connector line down into their bucket + avg summary above bucket.
    const pointY = bandY - 28;
    const g = svg.selectAll("g.pt").data(readings).join("g").attr("class", "pt");
    g.append("line")
      .attr("x1", d => scaleX(d.min)).attr("x2", d => scaleX(d.min))
      .attr("y1", pointY + 8).attr("y2", bandY)
      .attr("stroke", d => d.bucket.color).attr("stroke-width", 1.5);
    g.append("circle")
      .attr("cx", d => scaleX(d.min)).attr("cy", pointY).attr("r", 5)
      .attr("fill", d => d.bucket.color).attr("stroke", "#fff").attr("stroke-width", 1.2);
    g.append("text")
      .attr("x", d => scaleX(d.min)).attr("y", pointY - 10)
      .attr("text-anchor", "middle")
      .style("font-size", "10px").style("fill", TEXT)
      .text(d => `${fmt(d.min)} (${d.value})`);

    // Per-bucket aggregate (count + avg) below the range label.
    const agg = buckets.map(b => {
      const rs = readings.filter(r => r.bucket.id === b.id);
      const avg = rs.length ? (rs.reduce((s, r) => s + r.value, 0) / rs.length).toFixed(1) : "\u2014";
      return { ...b, count: rs.length, avg };
    });
    svg.selectAll("text.agg").data(agg).join("text")
      .attr("class", "agg")
      .attr("x", d => (scaleX(d.start) + scaleX(d.end)) / 2)
      .attr("y", bandY + bandH + 34)
      .attr("text-anchor", "middle")
      .style("font-size", "10px").style("font-weight", "700").style("fill", d => d.color)
      .text(d => `n=${d.count}, avg=${d.avg}`);

    root.append("div")
      .style("margin-top", "6px")
      .style("font-size", "11.5px")
      .style("opacity", "0.85")
      .text("Every bucket has the exact same 15-minute width — only the anchor (bucket_start) is stored; " +
            "bucket_end = bucket_start + INTERVAL 15 MINUTES is computed at query time.");
  }

  function vizPointInIntervalFixedDistance(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";
    const AXIS = dark ? "#607d8b" : "#90a4ae";

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 20, PAD_R = 20;
    // Domain: minutes elapsed since 09:00, with room for the +/-10 min windows either side.
    const scaleX = d3.scaleLinear().domain([-15, 85]).range([PAD_L, W - PAD_R]);
    const fmt = m => {
      const total = 9 * 60 + m, h = Math.floor(total / 60), mm = total % 60;
      return `${String(h).padStart(2, "0")}:${String(mm).padStart(2, "0")}`;
    };

    const clicks = [
      { id: 1, min: 0, campaign: "summer_sale" },
      { id: 2, min: 8, campaign: "summer_sale" },
      { id: 3, min: 25, campaign: "winter_promo" },
      { id: 4, min: 60, campaign: "flash_deal" },
    ];
    const purchases = [
      { id: 1, customer: "cust_1", min: 5, matches: [1, 2], color: "#ab47bc" },   // fan-out
      { id: 2, customer: "cust_2", min: 26, matches: [3], color: TEAL },
      { id: 3, customer: "cust_3", min: 45, matches: [], color: RED },            // gap
      { id: 4, customer: "cust_4", min: 65, matches: [4], color: TEAL },
    ];

    const root = d3.select(el)
      .style("font-family", "inherit")
      .style("font-size", "13px")
      .style("color", TEXT);

    const H = 210;
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    const clickY = 50, purchY = 160, winY = 90, winH = 50;

    // +/-10 minute window band behind each purchase (semi-transparent, colored by outcome).
    svg.selectAll("rect.win").data(purchases).join("rect")
      .attr("class", "win")
      .attr("x", d => scaleX(d.min - 10)).attr("y", winY)
      .attr("width", d => scaleX(d.min + 10) - scaleX(d.min - 10)).attr("height", winH)
      .attr("fill", d => d.color).attr("opacity", 0.14)
      .attr("stroke", d => d.color)
      .attr("stroke-dasharray", d => d.matches.length ? "0" : "4,3")
      .attr("stroke-width", 1.2).attr("rx", 4);

    // Click points (top row).
    const gc = svg.selectAll("g.click").data(clicks).join("g").attr("class", "click");
    gc.append("circle")
      .attr("cx", d => scaleX(d.min)).attr("cy", clickY).attr("r", 5)
      .attr("fill", "#42a5f5").attr("stroke", "#fff").attr("stroke-width", 1.2);
    gc.append("text")
      .attr("x", d => scaleX(d.min)).attr("y", clickY - 12)
      .attr("text-anchor", "middle")
      .style("font-size", "9.5px").style("fill", TEXT)
      .text(d => `click ${d.id} ${fmt(d.min)}`);

    // Connector lines from purchase to each matched click.
    const clickById = Object.fromEntries(clicks.map(c => [c.id, c]));
    purchases.forEach(p => {
      p.matches.forEach(cid => {
        const c = clickById[cid];
        svg.append("line")
          .attr("x1", scaleX(p.min)).attr("x2", scaleX(c.min))
          .attr("y1", purchY).attr("y2", clickY)
          .attr("stroke", p.color).attr("stroke-width", 1.5);
      });
    });

    // Purchase points (bottom row).
    const gp = svg.selectAll("g.purch").data(purchases).join("g").attr("class", "purch");
    gp.append("circle")
      .attr("cx", d => scaleX(d.min)).attr("cy", purchY).attr("r", 5)
      .attr("fill", d => d.color).attr("stroke", "#fff").attr("stroke-width", 1.2);
    gp.append("text")
      .attr("x", d => scaleX(d.min)).attr("y", purchY + 18)
      .attr("text-anchor", "middle")
      .style("font-size", "9.5px").style("font-weight", "700").style("fill", d => d.color)
      .text(d => `${d.customer} ${fmt(d.min)}${d.matches.length > 1 ? " (fan-out x" + d.matches.length + ")" : d.matches.length === 0 ? " (no match)" : ""}`);

    root.append("div")
      .style("margin-top", "6px")
      .style("font-size", "11.5px")
      .style("opacity", "0.85")
      .html(
        "Each shaded band is one purchase's own \u00B110-minute window (it moves with the point, " +
        "unlike the fixed grid in the previous section). <span style=\"color:#ab47bc;font-weight:700;\">cust_1</span> " +
        "\u2192 2 clicks fall inside its window (fan-out); <span style=\"color:" + RED + ";font-weight:700;\">cust_3</span> " +
        "\u2192 no click falls inside its window (gap, dashed outline)."
      );
  }

  function vizPointInIntervalExtraCondition(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";
    const MATH_COLOR = "#42a5f5";
    const ENGLISH_COLOR = "#ffa726";

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 34, PAD_R = 14;
    const scaleX = d3.scaleLinear().domain([0, 100]).range([PAD_L, W - PAD_R]);

    const mathBands = [
      { grade: "F", min: 0, max: 44 }, { grade: "D", min: 45, max: 59 }, { grade: "C", min: 60, max: 74 },
      { grade: "B", min: 75, max: 89 }, { grade: "A", min: 90, max: 100 },
    ];
    const englishBands = [
      { grade: "F", min: 0, max: 34 }, { grade: "D", min: 35, max: 49 }, { grade: "C", min: 50, max: 69 },
      { grade: "B", min: 70, max: 84 }, { grade: "A", min: 85, max: 100 },
    ];
    // matchOther = the band the student would spuriously also match if the `course =` condition were dropped.
    const students = [
      { name: "Alice", course: "Math", score: 55 },
      { name: "Bob", course: "English", score: 75 },
      { name: "Charlie", course: "Math", score: 85 },
      { name: "Diana", course: "English", score: 65 },
      { name: "Eva", course: "Math", score: 70 },
      { name: "Frank", course: "English", score: 90 },
    ];

    const root = d3.select(el)
      .style("font-family", "inherit")
      .style("font-size", "13px")
      .style("color", TEXT);

    const H = 210;
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    const mathY = 30, englishY = 150, bandH = 26, studentY = 90;

    function drawBands(bands, y, color, label) {
      svg.selectAll(`rect.band-${label}`).data(bands).join("rect")
        .attr("class", `band-${label}`)
        .attr("x", d => scaleX(d.min)).attr("y", y)
        .attr("width", d => scaleX(d.max) - scaleX(d.min)).attr("height", bandH)
        .attr("fill", color).attr("opacity", d => d.grade === "A" || d.grade === "F" ? 0.55 : 0.85)
        .attr("rx", 3);
      svg.selectAll(`text.bandLabel-${label}`).data(bands).join("text")
        .attr("class", `bandLabel-${label}`)
        .attr("x", d => (scaleX(d.min) + scaleX(d.max)) / 2).attr("y", y + bandH / 2 + 4)
        .attr("text-anchor", "middle")
        .style("font-size", "10px").style("font-weight", "700").style("fill", "#fff")
        .text(d => d.grade);
      svg.append("text").attr("x", 4).attr("y", y + bandH / 2 + 4)
        .style("font-size", "10px").style("font-weight", "700").style("fill", color)
        .text(label === "math" ? "Math" : "Eng.");
    }
    drawBands(mathBands, mathY, MATH_COLOR, "math");
    drawBands(englishBands, englishY, ENGLISH_COLOR, "english");

    function bandFor(bands, score) { return bands.find(b => score >= b.min && score <= b.max); }

    students.forEach(s => {
      const ownColor = s.course === "Math" ? MATH_COLOR : ENGLISH_COLOR;
      const own = bandFor(s.course === "Math" ? mathBands : englishBands, s.score);
      const other = bandFor(s.course === "Math" ? englishBands : mathBands, s.score);
      const x = scaleX(s.score);

      // Solid line = correct match (with the `course =` condition).
      svg.append("line")
        .attr("x1", x).attr("x2", x)
        .attr("y1", studentY).attr("y2", s.course === "Math" ? mathY + bandH : englishY)
        .attr("stroke", ownColor).attr("stroke-width", 1.8);

      // Dashed red line = spurious extra match if `course =` were dropped.
      if (other) {
        svg.append("line")
          .attr("x1", x).attr("x2", x)
          .attr("y1", studentY).attr("y2", s.course === "Math" ? englishY : mathY + bandH)
          .attr("stroke", RED).attr("stroke-width", 1.5).attr("stroke-dasharray", "3,2");
      }

      svg.append("circle")
        .attr("cx", x).attr("cy", studentY).attr("r", 5)
        .attr("fill", ownColor).attr("stroke", "#fff").attr("stroke-width", 1.2);
      svg.append("text")
        .attr("x", x).attr("y", studentY - 10)
        .attr("text-anchor", "middle")
        .style("font-size", "9.5px").style("fill", TEXT)
        .text(`${s.name} (${s.score})`);
      svg.append("text")
        .attr("x", x).attr("y", studentY + 20)
        .attr("text-anchor", "middle")
        .style("font-size", "9px").style("fill", RED)
        .text(`+${other ? other.grade : "\u2014"} (${s.course === "Math" ? "Eng." : "Math"})`);
    });

    root.append("div")
      .style("margin-top", "8px")
      .style("font-size", "11.5px")
      .style("opacity", "0.85")
      .html(
        "Solid line = correct match under <code>course = course AND score BETWEEN ...</code>. " +
        "Dashed red line = the <strong>extra</strong> match every student would also get if the " +
        "<code>course =</code> equality were dropped — because Math's and English's bands overlap " +
        "across the two tracks."
      );
  }

  /* ── Shared helper: draw a row of horizontal interval bars on a time axis ── */
  function drawIntervalRow(svg, scaleX, y, h, items, dark) {
    const g = svg.selectAll(`g.row-${y}`).data(items).join("g").attr("class", `row-${y}`);
    g.append("rect")
      .attr("x", d => scaleX(d.start)).attr("y", y)
      .attr("width", d => Math.max(2, scaleX(d.end) - scaleX(d.start))).attr("height", h)
      .attr("fill", d => d.color).attr("opacity", d => d.dashed ? 0.25 : 0.85)
      .attr("stroke", d => d.color).attr("stroke-dasharray", d => d.dashed ? "3,2" : "0")
      .attr("stroke-width", 1.2).attr("rx", 3);
    g.append("text")
      .attr("x", d => (scaleX(d.start) + scaleX(d.end)) / 2).attr("y", y + h / 2 + 4)
      .attr("text-anchor", "middle")
      .style("font-size", "9.5px").style("font-weight", "700")
      .style("fill", d => d.dashed ? d.color : "#fff")
      .text(d => d.label || "");
  }

  function vizIntervalOverlapScenarios(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 20, PAD_R = 20;
    const scaleX = d3.scaleLinear().domain([0, 9]).range([PAD_L, W - PAD_R]);

    const others = [
      { label: "A", start: 0, end: 2, overlap: false },
      { label: "B", start: 1, end: 3, overlap: false },
      { label: "C", start: 2, end: 4, overlap: true },
      { label: "F", start: 2, end: 8, overlap: true },
      { label: "D", start: 3, end: 6, overlap: true },
      { label: "E", start: 4, end: 5, overlap: true },
      { label: "G", start: 5, end: 9, overlap: true },
      { label: "H", start: 6, end: 9, overlap: false },
      { label: "I", start: 7, end: 9, overlap: false },
    ];

    const root = d3.select(el)
      .style("font-family", "inherit").style("font-size", "13px").style("color", TEXT);
    const rowH = 20, gap = 6;
    const H = 40 + others.length * (rowH + gap);
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    // Reference interval R drawn first as a slim guide band across the full height.
    svg.append("rect")
      .attr("x", scaleX(3)).attr("y", 4)
      .attr("width", scaleX(6) - scaleX(3)).attr("height", H - 8)
      .attr("fill", "#ab47bc").attr("opacity", 0.12);
    svg.append("text").attr("x", (scaleX(3) + scaleX(6)) / 2).attr("y", 16)
      .attr("text-anchor", "middle")
      .style("font-size", "10px").style("font-weight", "700").style("fill", "#ab47bc")
      .text("R = [3, 6]");

    others.forEach((d, i) => {
      const y = 26 + i * (rowH + gap);
      const color = d.overlap ? TEAL : RED;
      svg.append("rect")
        .attr("x", scaleX(d.start)).attr("y", y)
        .attr("width", scaleX(d.end) - scaleX(d.start)).attr("height", rowH)
        .attr("fill", color).attr("opacity", 0.85).attr("rx", 3);
      svg.append("text")
        .attr("x", scaleX(d.start) - 6).attr("y", y + rowH / 2 + 4)
        .attr("text-anchor", "end")
        .style("font-size", "10px").style("font-weight", "700").style("fill", TEXT)
        .text(d.label);
      svg.append("text")
        .attr("x", (scaleX(d.start) + scaleX(d.end)) / 2).attr("y", y + rowH / 2 + 4)
        .attr("text-anchor", "middle")
        .style("font-size", "9px").style("font-weight", "700").style("fill", "#fff")
        .text(d.overlap ? "overlap" : "no overlap");
    });

    root.append("div")
      .style("margin-top", "6px").style("font-size", "11.5px").style("opacity", "0.85")
      .html(`<span style="color:${TEAL};font-weight:700;">\u25CF overlap</span> vs. <span style="color:${RED};font-weight:700;">\u25CF no overlap</span> against the shaded reference interval R=[3,6]. B and H touch R's edge exactly but don't overlap it.`);
  }

  function vizIntervalOverlapInequality(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 40, PAD_R = 16;
    const scaleX = d3.scaleLinear().domain([7, 16]).range([PAD_L, W - PAD_R]);
    const fmt = h => `${String(Math.floor(h)).padStart(2, "0")}:${h % 1 ? "30" : "00"}`;

    const events = [
      { label: "Event A", start: 8, end: 10, color: TEAL },
      { label: "Event B", start: 9, end: 11, color: TEAL },
      { label: "Event C", start: 12, end: 14, color: "#ffa726" },
      { label: "Event D", start: 13, end: 15, color: TEAL },
    ];
    const avail = [
      { label: "avail 1", start: 7, end: 9.5, color: "#42a5f5" },
      { label: "avail 2", start: 9, end: 11.5, color: "#42a5f5" },
      { label: "avail 3", start: 10, end: 12, color: "#42a5f5" },
      { label: "avail 4", start: 14, end: 16, color: "#42a5f5" },
    ];

    const root = d3.select(el)
      .style("font-family", "inherit").style("font-size", "13px").style("color", TEXT);
    const H = 150;
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    drawIntervalRow(svg, scaleX, 24, 26, events, dark);
    drawIntervalRow(svg, scaleX, 96, 26, avail, dark);

    // Mark the two touch points that only match under inclusive (<=/>=) semantics.
    const touches = [
      { at: 10, note: "A ends = avail3 starts" },
      { at: 12, note: "C starts = avail3 ends" },
      { at: 14, note: "C ends = avail4 starts" },
    ];
    touches.forEach(t => {
      svg.append("line")
        .attr("x1", scaleX(t.at)).attr("x2", scaleX(t.at))
        .attr("y1", 20).attr("y2", 126)
        .attr("stroke", RED).attr("stroke-width", 1.3).attr("stroke-dasharray", "2,2");
      svg.append("circle").attr("cx", scaleX(t.at)).attr("cy", 60).attr("r", 3.5).attr("fill", RED);
    });

    root.append("div")
      .style("margin-top", "6px").style("font-size", "11.5px").style("opacity", "0.85")
      .html(`Dashed red lines mark the 3 boundary touch-points. Under strict <code>&lt;/&gt;</code> they don't count as overlap (6 rows); under inclusive <code>&lt;=/&gt;=</code> all 3 do (9 rows).`);
  }

  function vizIntervalOverlapFixedLength(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 40, PAD_R = 16;
    const scaleX = d3.scaleLinear().domain([7, 17]).range([PAD_L, W - PAD_R]);

    const requests = [
      { label: "Amy", start: 7.5, end: 8.5, color: "#7c4dff" },
      { label: "Ben", start: 10.5, end: 11.5, color: "#7c4dff" },
      { label: "Cid", start: 13.5, end: 14.5, color: "#7c4dff" },
      { label: "Dee", start: 15.5, end: 16.5, color: "#7c4dff" },
    ];
    const avail = [
      { label: "avail 1", start: 7, end: 9.5, color: "#42a5f5" },
      { label: "avail 2", start: 9, end: 11.5, color: "#42a5f5" },
      { label: "avail 3", start: 10, end: 12, color: "#42a5f5" },
      { label: "avail 4", start: 14, end: 16, color: "#42a5f5" },
    ];

    const root = d3.select(el)
      .style("font-family", "inherit").style("font-size", "13px").style("color", TEXT);
    const H = 140;
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    svg.append("text").attr("x", PAD_L).attr("y", 14)
      .style("font-size", "10px").style("font-weight", "700").style("fill", "#7c4dff")
      .text("meeting_requests (fixed 60-min duration)");
    drawIntervalRow(svg, scaleX, 20, 26, requests, dark);

    svg.append("text").attr("x", PAD_L).attr("y", 92)
      .style("font-size", "10px").style("font-weight", "700").style("fill", "#42a5f5")
      .text("availability_tbl");
    drawIntervalRow(svg, scaleX, 98, 26, avail, dark);

    root.append("div")
      .style("margin-top", "6px").style("font-size", "11.5px").style("opacity", "0.85")
      .text("Every request bar has the same 60-minute width — only start_time is stored; request_end = start_time + INTERVAL 60 MINUTES is computed at query time.");
  }

  function vizIntervalOverlapFixedDistance(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 40, PAD_R = 16;
    const scaleX = d3.scaleLinear().domain([7, 16]).range([PAD_L, W - PAD_R]);

    const events = [
      { label: "Event A", start: 8, end: 10, color: TEAL },
      { label: "Event B", start: 9, end: 11, color: TEAL },
      { label: "Event C", start: 12, end: 14, color: TEAL },
      { label: "Event D", start: 13, end: 15, color: TEAL },
    ];
    const avail = [
      { label: "avail 1", start: 7, end: 9.5, color: "#42a5f5" },
      { label: "avail 2", start: 9, end: 11.5, color: "#42a5f5" },
      { label: "avail 3", start: 10, end: 12, color: "#42a5f5" },
      { label: "avail 4", start: 14, end: 16, color: "#42a5f5" },
    ];
    const buffer = 0.25; // 15 minutes in hours

    const root = d3.select(el)
      .style("font-family", "inherit").style("font-size", "13px").style("color", TEXT);
    const H = 150;
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    // Faint padded "buffer zone" behind each availability window.
    avail.forEach(a => {
      svg.append("rect")
        .attr("x", scaleX(a.start - buffer)).attr("y", 96)
        .attr("width", scaleX(a.end + buffer) - scaleX(a.start - buffer)).attr("height", 26)
        .attr("fill", "#42a5f5").attr("opacity", 0.15)
        .attr("stroke", "#42a5f5").attr("stroke-dasharray", "3,2").attr("stroke-width", 1);
    });

    drawIntervalRow(svg, scaleX, 24, 26, events, dark);
    drawIntervalRow(svg, scaleX, 96, 26, avail, dark);

    root.append("div")
      .style("margin-top", "6px").style("font-size", "11.5px").style("opacity", "0.85")
      .text("The dashed light-blue zone around each availability window is the +/-15 minute buffer — any event bar touching it (not just the solid window) now counts as a conflict.");
  }

  function vizIntervalOverlapExtraCondition(el) {
    const dark = isDarkMode();
    const TEXT = dark ? "#eceff1" : "#37474f";

    const W = Math.max(320, Math.min(el.clientWidth || 760, 760));
    const PAD_L = 20, PAD_R = 20;
    const scaleX = d3.scaleLinear().domain([9, 10.25]).range([PAD_L, W - PAD_R]);

    const roomA = [
      { label: "Standup", start: 9, end: 9.5, color: "#42a5f5" },
      { label: "Retro", start: 9.25, end: 9.75, color: "#42a5f5" },
    ];
    const roomB = [
      { label: "Planning", start: 9.25, end: 10, color: "#ffa726" },
    ];

    const root = d3.select(el)
      .style("font-family", "inherit").style("font-size", "13px").style("color", TEXT);
    const H = 140;
    const svg = root.append("svg").attr("width", W).attr("height", H).style("display", "block");

    svg.append("text").attr("x", PAD_L).attr("y", 14)
      .style("font-size", "10px").style("font-weight", "700").style("fill", "#42a5f5").text("Room A");
    drawIntervalRow(svg, scaleX, 20, 26, roomA, dark);

    svg.append("text").attr("x", PAD_L).attr("y", 92)
      .style("font-size", "10px").style("font-weight", "700").style("fill", "#ffa726").text("Room B");
    drawIntervalRow(svg, scaleX, 98, 26, roomB, dark);

    // Real conflict: Standup <-> Retro (same room, overlapping) - solid teal connector.
    svg.append("line")
      .attr("x1", scaleX(9.375)).attr("x2", scaleX(9.375))
      .attr("y1", 33).attr("y2", 46)
      .attr("stroke", TEAL).attr("stroke-width", 2.5);
    svg.append("text").attr("x", scaleX(9.375)).attr("y", 60)
      .attr("text-anchor", "middle")
      .style("font-size", "9px").style("font-weight", "700").style("fill", TEAL)
      .text("real conflict (same room)");

    // False conflict: Planning overlaps both Standup and Retro in time, but different room.
    svg.append("line")
      .attr("x1", scaleX(9.4)).attr("x2", scaleX(9.4))
      .attr("y1", 46).attr("y2", 98)
      .attr("stroke", RED).attr("stroke-width", 1.5).attr("stroke-dasharray", "3,2");
    svg.append("text").attr("x", scaleX(9.7)).attr("y", 78)
      .attr("text-anchor", "middle")
      .style("font-size", "9px").style("font-weight", "700").style("fill", RED)
      .text("time overlaps, different room \u2192 false positive without room=room");

    root.append("div")
      .style("margin-top", "8px").style("font-size", "11.5px").style("opacity", "0.85")
      .html(`<span style="color:${TEAL};font-weight:700;">Solid</span> = genuine same-room conflict; <span style="color:${RED};font-weight:700;">dashed red</span> = would be incorrectly reported as a conflict if the <code>room = room</code> condition were omitted.`);
  }

  /* ── Router ──────────────────────────────────────────────────────── */
  const VIZ_MAP = {
    "viz-join-types-overview": vizTypesOverview,
    "viz-join-inner": vizInner,
    "viz-join-left": vizLeft,
    "viz-join-right": vizRight,
    "viz-join-full": vizFull,
    "viz-join-semi": vizSemi,
    "viz-join-anti": vizAnti,
    "viz-join-cross": vizCross,
    "viz-join-non-equi": vizNonEqui,
    "viz-join-range-overview": vizPointInInterval,
    "viz-join-point-in-interval": vizPointInInterval,
    "viz-join-point-in-interval-scenarios": vizPointInIntervalScenarios,
    "viz-join-point-in-interval-inequality": vizPointInIntervalInequality,
    "viz-join-point-in-interval-fixed-length": vizPointInIntervalFixedLength,
    "viz-join-point-in-interval-fixed-distance": vizPointInIntervalFixedDistance,
    "viz-join-point-in-interval-extra-condition": vizPointInIntervalExtraCondition,
    "viz-join-interval-overlap": vizIntervalOverlap,
    "viz-join-interval-overlap-scenarios": vizIntervalOverlapScenarios,
    "viz-join-interval-overlap-inequality": vizIntervalOverlapInequality,
    "viz-join-interval-overlap-fixed-length": vizIntervalOverlapFixedLength,
    "viz-join-interval-overlap-fixed-distance": vizIntervalOverlapFixedDistance,
    "viz-join-interval-overlap-extra-condition": vizIntervalOverlapExtraCondition,
    "viz-join-lateral": vizLateral,
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
