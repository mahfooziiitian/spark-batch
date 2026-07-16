/**
 * scd-viz.js
 * D3 v7 interactive visualization for SCD (Slowly Changing Dimensions) pages.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-scd-overview  — SCD type selector: shows dimension table rows before/after a change
 */
(function () {
  "use strict";

  const C    = ["#7c4dff", "#ffa726", "#26a69a", "#ef5350", "#ab47bc", "#29b6f6"];
  const GRAY = "#90a4ae";
  const FG   = "#546e7a";
  const LIGHT_BG = "#f5f5f5";

  /* ══════════════════════════════════════════════════════════════════
   * SCD OVERVIEW — Interactive Type Selector
   *
   * Scenario: Customer Alice starts in NY. A change event arrives
   *   setting her city to TX. Each button shows how the dim table
   *   looks AFTER applying that SCD type.
   *
   * Layout:
   *   [Change event banner]
   *   [Type buttons: T1 T2 T3 T4 T5 T6]
   *   [Dimension table rows as colored SVG rows]
   *   [Description footer]
   * ══════════════════════════════════════════════════════════════════ */
  function renderSCDOverview(el) {
    const W = Math.min(el.clientWidth || 820, 820);
    const isDark = document.documentElement.getAttribute("data-md-color-scheme") === "slate";
    const TEXT   = isDark ? "#eceff1" : FG;
    const ROWBG  = isDark ? "#263238" : "#fff";
    const HEADBG = isDark ? "#37474f" : "#eceff1";
    const STRIPE = isDark ? "#2e3c43" : "#fafafa";

    /* ── Data model ─────────────────────────────────────────────── */
    const TYPES = [
      {
        id: 1, label: "Type 1", color: C[0],
        desc: "Overwrite in place — no history retained. Previous NY value is lost.",
        rows: [
          { sk:"cust1", id:"cust1", name:"Alice", city:"TX", extra:{}, badge:"updated", badgeColor: C[1] },
        ],
        cols: ["customer_sk","customer_id","name","city"],
        history: null,
      },
      {
        id: 2, label: "Type 2", color: C[2],
        desc: "Full history — expire old row, insert new row. Point-in-time queries possible.",
        rows: [
          { sk:"cust1-v1", id:"cust1", name:"Alice", city:"NY", extra:{ is_current:"false", end_date:"2024-06-15" }, badge:"expired", badgeColor: GRAY },
          { sk:"cust1-v2", id:"cust1", name:"Alice", city:"TX", extra:{ is_current:"true",  end_date:"NULL"       }, badge:"new row", badgeColor: C[2] },
        ],
        cols: ["surr_key","customer_id","name","city","is_current","end_date"],
        history: null,
      },
      {
        id: 3, label: "Type 3", color: C[5],
        desc: "Add columns — one row per customer, previous_city column stores prior value.",
        rows: [
          { sk:"cust1", id:"cust1", name:"Alice", city:"TX", extra:{ previous_city:"NY", changed_at:"2024-06-15" }, badge:"updated", badgeColor: C[1] },
        ],
        cols: ["customer_id","name","current_city","previous_city","changed_at"],
        history: null,
      },
      {
        id: 4, label: "Type 4", color: C[3],
        desc: "Separate history table — current dim stays lean; all versions live in dim_history.",
        rows: [
          { sk:"cust1", id:"cust1", name:"Alice", city:"TX", extra:{}, badge:"current dim", badgeColor: C[0] },
        ],
        cols: ["customer_id","name","city"],
        history: [
          { sk:"1", id:"cust1", name:"Alice", city:"NY", extra:{ batch_date:"2024-01-01" }, badge:"v1", badgeColor: GRAY },
          { sk:"2", id:"cust1", name:"Alice", city:"TX", extra:{ batch_date:"2024-06-15" }, badge:"v2", badgeColor: C[3] },
        ],
        histCols: ["hist_sk","customer_id","name","city","batch_date"],
      },
      {
        id: 5, label: "Type 5", color: C[4],
        desc: "Hybrid (Type 4 + mini-Type 1) — current dim embeds latest values for fast joins; history stored separately.",
        rows: [
          { sk:"cust1", id:"cust1", name:"Alice", city:"TX", extra:{ current_city:"TX" }, badge:"current dim", badgeColor: C[0] },
        ],
        cols: ["customer_id","name","city","current_city"],
        history: [
          { sk:"1", id:"cust1", name:"Alice", city:"NY", extra:{ batch_date:"2024-01-01" }, badge:"v1", badgeColor: GRAY },
          { sk:"2", id:"cust1", name:"Alice", city:"TX", extra:{ batch_date:"2024-06-15" }, badge:"v2", badgeColor: C[4] },
        ],
        histCols: ["hist_sk","customer_id","name","city","batch_date"],
      },
      {
        id: 6, label: "Type 6", color: "#ff7043",
        desc: "Hybrid (Type 1+2+3) — new rows for each change AND current_city column always reflects the latest value.",
        rows: [
          { sk:"cust1-v1", id:"cust1", name:"Alice", city:"NY", extra:{ current_city:"TX", is_current:"false", end_date:"2024-06-15" }, badge:"expired", badgeColor: GRAY },
          { sk:"cust1-v2", id:"cust1", name:"Alice", city:"TX", extra:{ current_city:"TX", is_current:"true",  end_date:"NULL"       }, badge:"new row", badgeColor:"#ff7043" },
        ],
        cols: ["surr_key","customer_id","name","city","current_city","is_current","end_date"],
        history: null,
      },
    ];

    let active = 0; // Type 1 selected by default

    /* ── Container ──────────────────────────────────────────────── */
    const container = d3.select(el)
      .style("font-family", "inherit")
      .style("font-size", "13px");

    /* ── Change event banner ────────────────────────────────────── */
    container.append("div")
      .style("background", isDark ? "#1b2a30" : "#e8f5e9")
      .style("border-left", `4px solid ${C[2]}`)
      .style("padding", "10px 16px")
      .style("margin-bottom", "14px")
      .style("border-radius", "4px")
      .html(`<strong style="color:${C[2]}">:material-database-arrow-right: Incoming change event</strong><br>
        <span style="color:${TEXT}">Customer <strong>Alice</strong> (ID: cust1) — city changed:
        <strong style="color:${C[3]}">NY</strong>
        &nbsp;→&nbsp;
        <strong style="color:${C[2]}">TX</strong></span>`);

    /* ── Type buttons ───────────────────────────────────────────── */
    const btnRow = container.append("div")
      .style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap")
      .style("margin-bottom", "16px");

    const buttons = btnRow.selectAll("button").data(TYPES).join("button")
      .style("padding", "6px 18px")
      .style("border-radius", "20px")
      .style("border", d => `2px solid ${d.color}`)
      .style("cursor", "pointer")
      .style("font-size", "13px")
      .style("font-weight", "600")
      .style("transition", "all 0.15s")
      .text(d => d.label);

    /* ── Table area ─────────────────────────────────────────────── */
    const tableDiv  = container.append("div");
    const descDiv   = container.append("div")
      .style("margin-top", "10px")
      .style("padding", "8px 14px")
      .style("border-radius", "4px")
      .style("font-size", "12.5px");

    /* ── Render a table ─────────────────────────────────────────── */
    function renderTable(parent, rows, cols, title, titleColor) {
      const wrap = parent.append("div").style("margin-bottom", "10px");
      if (title) {
        wrap.append("div")
          .style("font-weight", "700").style("margin-bottom", "4px")
          .style("color", titleColor || TEXT)
          .text(title);
      }
      const tbl = wrap.append("div")
        .style("overflow-x", "auto")
        .append("table")
        .style("width", "100%")
        .style("border-collapse", "collapse")
        .style("font-size", "12px");

      /* Header */
      const thead = tbl.append("thead");
      const hrow  = thead.append("tr").style("background", HEADBG);
      // badge col
      hrow.append("th").text("").style("width", "80px").style("padding", "5px 8px");
      cols.forEach(c => {
        hrow.append("th")
          .style("padding", "5px 8px")
          .style("text-align", "left")
          .style("border-bottom", `1px solid ${GRAY}`)
          .style("color", TEXT)
          .text(c);
      });

      /* Rows */
      const tbody = tbl.append("tbody");
      rows.forEach((row, ri) => {
        const tr = tbody.append("tr")
          .style("background", ri % 2 === 0 ? ROWBG : STRIPE)
          .style("border-left", `4px solid ${row.badgeColor}`);

        // badge
        tr.append("td")
          .style("padding", "5px 6px")
          .append("span")
          .style("background", row.badgeColor)
          .style("color", "#fff")
          .style("border-radius", "10px")
          .style("padding", "2px 7px")
          .style("font-size", "11px")
          .style("white-space", "nowrap")
          .text(row.badge);

        cols.forEach(c => {
          let val = row[c] !== undefined ? row[c]
                  : row.extra[c] !== undefined ? row.extra[c]
                  : "—";
          // Map display cols
          if (c === "surr_key") val = row.sk;
          if (c === "hist_sk")  val = row.sk;
          if (c === "customer_sk") val = row.sk;
          if (c === "current_city" && row.extra.current_city !== undefined) val = row.extra.current_city;

          const td = tr.append("td")
            .style("padding", "5px 8px")
            .style("color", TEXT);
          if (val === "NULL") td.style("color", GRAY).style("font-style", "italic");
          if (val === "false") td.style("color", C[3]);
          if (val === "true")  td.style("color", C[2]);
          if (c === "city" && row.city === "TX") td.style("font-weight", "700").style("color", C[2]);
          if (c === "current_city" && val === "TX") td.style("font-weight", "700").style("color", C[2]);
          td.text(val);
        });
      });
    }

    /* ── Update ─────────────────────────────────────────────────── */
    function update(idx) {
      const t = TYPES[idx];
      tableDiv.html("");
      descDiv.html("");

      /* Style buttons */
      buttons
        .style("background", (d, i) => i === idx ? d.color : "transparent")
        .style("color", (d, i) => i === idx ? "#fff" : TEXT);

      /* Primary dim table */
      renderTable(tableDiv, t.rows, t.cols,
        t.history ? "dim_customer (current)" : "dim_customer",
        t.color);

      /* History table (Type 4, 5) */
      if (t.history) {
        renderTable(tableDiv, t.history, t.histCols, "dim_customer_history", GRAY);
      }

      /* Description */
      descDiv
        .style("background", isDark ? "#1b2a30" : `${t.color}18`)
        .style("border-left", `4px solid ${t.color}`)
        .style("color", TEXT)
        .html(`<strong style="color:${t.color}">${t.label}:</strong> ${t.desc}`);
    }

    buttons.on("click", (_, d) => {
      active = TYPES.indexOf(d);
      update(active);
    });

    update(active);
  }

  /* ══════════════════════════════════════════════════════════════════
   * PER-TYPE — Before / After interactive transition
   *
   * Shows the dimension table BEFORE and AFTER a change event.
   * A toggle button switches between the two states with a highlight
   * on changed cells.
   * ══════════════════════════════════════════════════════════════════ */

  function renderTypeViz(el, config) {
    const isDark = document.documentElement.getAttribute("data-md-color-scheme") === "slate";
    const TEXT   = isDark ? "#eceff1" : FG;
    const ROWBG  = isDark ? "#263238" : "#fff";
    const HEADBG = isDark ? "#37474f" : "#eceff1";
    const STRIPE = isDark ? "#2e3c43" : "#fafafa";
    const CHANGE_BG = isDark ? "rgba(124, 77, 255, 0.18)" : "rgba(124, 77, 255, 0.08)";
    const NEW_BG    = isDark ? "rgba(38, 166, 154, 0.18)" : "rgba(38, 166, 154, 0.08)";
    const EXPIRED_BG = isDark ? "rgba(144, 164, 174, 0.15)" : "rgba(144, 164, 174, 0.08)";

    let showAfter = false;
    const container = d3.select(el).style("font-family", "inherit").style("font-size", "13px");

    // Change event banner
    container.append("div")
      .style("background", isDark ? "#1b2a30" : "#e8f5e9")
      .style("border-left", "4px solid " + C[2])
      .style("padding", "8px 14px")
      .style("margin-bottom", "12px")
      .style("border-radius", "4px")
      .html('<strong style="color:' + C[2] + '">Incoming change</strong><br>' +
        '<span style="color:' + TEXT + '">Customer <strong>Alice</strong> — city: ' +
        '<strong style="color:' + C[3] + '">NY</strong> → <strong style="color:' + C[2] + '">TX</strong></span>');

    // Toggle button
    const btnWrap = container.append("div")
      .style("display", "flex").style("gap", "8px").style("margin-bottom", "14px");

    const btnBefore = btnWrap.append("button").text("Before")
      .style("padding", "5px 16px").style("border-radius", "16px")
      .style("border", "2px solid " + GRAY).style("cursor", "pointer")
      .style("font-size", "12px").style("font-weight", "600");

    const btnAfter = btnWrap.append("button").text("After")
      .style("padding", "5px 16px").style("border-radius", "16px")
      .style("border", "2px solid " + config.color).style("cursor", "pointer")
      .style("font-size", "12px").style("font-weight", "600");

    const tableArea = container.append("div");
    const noteDiv = container.append("div")
      .style("margin-top", "8px").style("padding", "6px 12px")
      .style("border-radius", "4px").style("font-size", "12px");

    function renderRows(parent, title, titleColor, cols, rows, highlights) {
      const wrap = parent.append("div").style("margin-bottom", "8px");
      if (title) {
        wrap.append("div")
          .style("font-weight", "700").style("margin-bottom", "4px")
          .style("color", titleColor || TEXT).style("font-size", "12px")
          .text(title);
      }
      const tbl = wrap.append("div").style("overflow-x", "auto")
        .append("table")
        .style("width", "100%").style("border-collapse", "collapse").style("font-size", "12px");

      const hrow = tbl.append("thead").append("tr").style("background", HEADBG);
      cols.forEach(function(c) {
        hrow.append("th")
          .style("padding", "4px 8px").style("text-align", "left")
          .style("border-bottom", "1px solid " + GRAY).style("color", TEXT)
          .text(c);
      });

      const tbody = tbl.append("tbody");
      rows.forEach(function(row, ri) {
        var rowMeta = highlights && highlights[ri] ? highlights[ri] : {};
        var bg = rowMeta.type === "new" ? NEW_BG
               : rowMeta.type === "expired" ? EXPIRED_BG
               : rowMeta.type === "changed" ? CHANGE_BG
               : ri % 2 === 0 ? ROWBG : STRIPE;
        var borderColor = rowMeta.type === "new" ? C[2]
                        : rowMeta.type === "expired" ? GRAY
                        : rowMeta.type === "changed" ? C[1]
                        : "transparent";

        var tr = tbody.append("tr")
          .style("background", bg)
          .style("border-left", "4px solid " + borderColor)
          .style("transition", "background 0.3s");

        row.forEach(function(val, ci) {
          var changedCells = rowMeta.cells || [];
          var isChanged = changedCells.indexOf(ci) >= 0;
          var td = tr.append("td")
            .style("padding", "4px 8px")
            .style("color", TEXT);
          if (isChanged) td.style("font-weight", "700").style("color", C[2]);
          if (val === "NULL") td.style("color", GRAY).style("font-style", "italic");
          if (val === "false") td.style("color", C[3]);
          if (val === "true") td.style("color", C[2]);
          td.text(val);
        });
      });
    }

    function update() {
      tableArea.html("");
      noteDiv.html("");

      btnBefore
        .style("background", !showAfter ? GRAY : "transparent")
        .style("color", !showAfter ? "#fff" : TEXT);
      btnAfter
        .style("background", showAfter ? config.color : "transparent")
        .style("color", showAfter ? "#fff" : TEXT);

      var state = showAfter ? config.after : config.before;
      state.tables.forEach(function(t) {
        renderRows(tableArea, t.title, t.titleColor, t.cols, t.rows, showAfter ? t.highlights : null);
      });

      var note = showAfter ? config.afterNote : config.beforeNote;
      noteDiv
        .style("background", isDark ? "#1b2a30" : (showAfter ? config.color + "18" : GRAY + "18"))
        .style("border-left", "4px solid " + (showAfter ? config.color : GRAY))
        .style("color", TEXT)
        .html(note);
    }

    btnBefore.on("click", function() { showAfter = false; update(); });
    btnAfter.on("click", function() { showAfter = true; update(); });

    update();
  }

  /* ── Type 1 config ─────────────────────────────────────────────── */
  function renderSCDType1(el) {
    renderTypeViz(el, {
      color: C[0],
      before: { tables: [{ title: "dim_customer", titleColor: C[0],
        cols: ["customer_id", "name", "city"],
        rows: [["cust1", "Alice", "NY"], ["cust2", "Bob", "SF"]]
      }]},
      after: { tables: [{ title: "dim_customer", titleColor: C[0],
        cols: ["customer_id", "name", "city"],
        rows: [["cust1", "Alice", "TX"], ["cust2", "Bob", "SF"]],
        highlights: { 0: { type: "changed", cells: [2] } }
      }]},
      beforeNote: "Alice is in NY. The dimension has one row per customer.",
      afterNote: '<strong style="color:' + C[0] + '">Overwritten.</strong> Alice\'s city updated from NY → TX. Previous value is lost permanently.',
    });
  }

  /* ── Type 2 config ─────────────────────────────────────────────── */
  function renderSCDType2(el) {
    renderTypeViz(el, {
      color: C[2],
      before: { tables: [{ title: "dim_customer", titleColor: C[2],
        cols: ["surr_key", "customer_id", "name", "city", "start_date", "end_date", "is_current"],
        rows: [["sk1", "cust1", "Alice", "NY", "2024-01-01", "9999-12-31", "true"],
               ["sk2", "cust2", "Bob",   "SF", "2024-01-01", "9999-12-31", "true"]]
      }]},
      after: { tables: [{ title: "dim_customer", titleColor: C[2],
        cols: ["surr_key", "customer_id", "name", "city", "start_date", "end_date", "is_current"],
        rows: [["sk1", "cust1", "Alice", "NY", "2024-01-01", "2024-06-15", "false"],
               ["sk3", "cust1", "Alice", "TX", "2024-06-15", "9999-12-31", "true"],
               ["sk2", "cust2", "Bob",   "SF", "2024-01-01", "9999-12-31", "true"]],
        highlights: {
          0: { type: "expired", cells: [5, 6] },
          1: { type: "new", cells: [0, 3, 4] }
        }
      }]},
      beforeNote: "One active row per customer, all with is_current = true.",
      afterNote: '<strong style="color:' + C[2] + '">Expired + inserted.</strong> Old row closed (end_date set, is_current = false). New row inserted with TX.',
    });
  }

  /* ── Type 3 config ─────────────────────────────────────────────── */
  function renderSCDType3(el) {
    renderTypeViz(el, {
      color: C[5],
      before: { tables: [{ title: "dim_customer", titleColor: C[5],
        cols: ["customer_id", "name", "city", "prev_city", "changed_at"],
        rows: [["cust1", "Alice", "NY", "NULL", "NULL"],
               ["cust2", "Bob",   "SF", "NULL", "NULL"]]
      }]},
      after: { tables: [{ title: "dim_customer", titleColor: C[5],
        cols: ["customer_id", "name", "city", "prev_city", "changed_at"],
        rows: [["cust1", "Alice", "TX", "NY", "2024-06-15"],
               ["cust2", "Bob",   "SF", "NULL", "NULL"]],
        highlights: { 0: { type: "changed", cells: [2, 3, 4] } }
      }]},
      beforeNote: "One row per customer. prev_city and changed_at are NULL (no prior change).",
      afterNote: '<strong style="color:' + C[5] + '">Updated in place.</strong> city → TX, prev_city ← NY, changed_at ← 2024-06-15. Only one level of history.',
    });
  }

  /* ── Type 4 config ─────────────────────────────────────────────── */
  function renderSCDType4(el) {
    renderTypeViz(el, {
      color: C[3],
      before: { tables: [
        { title: "dim_customer (current)", titleColor: C[0],
          cols: ["customer_id", "name", "city"],
          rows: [["cust1", "Alice", "NY"], ["cust2", "Bob", "SF"]]
        },
        { title: "dim_customer_history", titleColor: GRAY,
          cols: ["hist_sk", "customer_id", "name", "city", "valid_from"],
          rows: [["1", "cust1", "Alice", "NY", "2024-01-01"],
                 ["2", "cust2", "Bob",   "SF", "2024-01-01"]]
        }
      ]},
      after: { tables: [
        { title: "dim_customer (current)", titleColor: C[0],
          cols: ["customer_id", "name", "city"],
          rows: [["cust1", "Alice", "TX"], ["cust2", "Bob", "SF"]],
          highlights: { 0: { type: "changed", cells: [2] } }
        },
        { title: "dim_customer_history", titleColor: GRAY,
          cols: ["hist_sk", "customer_id", "name", "city", "valid_from"],
          rows: [["1", "cust1", "Alice", "NY", "2024-01-01"],
                 ["2", "cust2", "Bob",   "SF", "2024-01-01"],
                 ["3", "cust1", "Alice", "TX", "2024-06-15"]],
          highlights: { 2: { type: "new", cells: [0, 3, 4] } }
        }
      ]},
      beforeNote: "Current table has latest values. History table has one row per initial load.",
      afterNote: '<strong style="color:' + C[3] + '">Current overwritten, history appended.</strong> Main dim updated like Type 1. New row archived in history.',
    });
  }

  /* ── Type 5 config ─────────────────────────────────────────────── */
  function renderSCDType5(el) {
    renderTypeViz(el, {
      color: C[4],
      before: { tables: [
        { title: "dim_customer (+ hist_key FK)", titleColor: C[0],
          cols: ["customer_id", "name", "city", "hist_key"],
          rows: [["cust1", "Alice", "NY", "h1"], ["cust2", "Bob", "SF", "h2"]]
        },
        { title: "dim_customer_history", titleColor: GRAY,
          cols: ["hist_key", "customer_id", "name", "city", "valid_from", "valid_to"],
          rows: [["h1", "cust1", "Alice", "NY", "2024-01-01", "9999-12-31"],
                 ["h2", "cust2", "Bob",   "SF", "2024-01-01", "9999-12-31"]]
        }
      ]},
      after: { tables: [
        { title: "dim_customer (+ hist_key FK)", titleColor: C[0],
          cols: ["customer_id", "name", "city", "hist_key"],
          rows: [["cust1", "Alice", "TX", "h3"], ["cust2", "Bob", "SF", "h2"]],
          highlights: { 0: { type: "changed", cells: [2, 3] } }
        },
        { title: "dim_customer_history", titleColor: GRAY,
          cols: ["hist_key", "customer_id", "name", "city", "valid_from", "valid_to"],
          rows: [["h1", "cust1", "Alice", "NY", "2024-01-01", "2024-06-15"],
                 ["h2", "cust2", "Bob",   "SF", "2024-01-01", "9999-12-31"],
                 ["h3", "cust1", "Alice", "TX", "2024-06-15", "9999-12-31"]],
          highlights: {
            0: { type: "expired", cells: [5] },
            2: { type: "new", cells: [0, 3, 4] }
          }
        }
      ]},
      beforeNote: "Current dim has hist_key pointing to the latest history row. History has one row per load.",
      afterNote: '<strong style="color:' + C[4] + '">Current overwritten + FK updated.</strong> hist_key now points to new history row h3. Old history row expired.',
    });
  }

  /* ── Type 6 config ─────────────────────────────────────────────── */
  function renderSCDType6(el) {
    renderTypeViz(el, {
      color: "#ff7043",
      before: { tables: [{ title: "dim_customer (single table)", titleColor: "#ff7043",
        cols: ["surr_key", "customer_id", "name", "city", "current_city", "prev_city", "start_date", "end_date", "is_current"],
        rows: [["sk1", "cust1", "Alice", "NY", "NY", "NULL", "2024-01-01", "9999-12-31", "true"],
               ["sk2", "cust2", "Bob",   "SF", "SF", "NULL", "2024-01-01", "9999-12-31", "true"]]
      }]},
      after: { tables: [{ title: "dim_customer (single table)", titleColor: "#ff7043",
        cols: ["surr_key", "customer_id", "name", "city", "current_city", "prev_city", "start_date", "end_date", "is_current"],
        rows: [["sk1", "cust1", "Alice", "NY", "TX", "NULL", "2024-01-01", "2024-06-15", "false"],
               ["sk3", "cust1", "Alice", "TX", "TX", "NY",   "2024-06-15", "9999-12-31", "true"],
               ["sk2", "cust2", "Bob",   "SF", "SF", "NULL", "2024-01-01", "9999-12-31", "true"]],
        highlights: {
          0: { type: "expired", cells: [4, 7, 8] },
          1: { type: "new", cells: [0, 3, 4, 5, 6] }
        }
      }]},
      beforeNote: "Single table with current_city and prev_city columns. All rows reflect current values.",
      afterNote: '<strong style="color:#ff7043">Expire + insert + backfill.</strong> Old row expired. New row inserted. current_city updated on ALL rows (Type 1). prev_city set on new row (Type 3).',
    });
  }

  /* ── Router ──────────────────────────────────────────────────────── */
  const VIZ_MAP = {
    "viz-scd-overview": renderSCDOverview,
    "viz-scd-type1": renderSCDType1,
    "viz-scd-type2": renderSCDType2,
    "viz-scd-type3": renderSCDType3,
    "viz-scd-type4": renderSCDType4,
    "viz-scd-type5": renderSCDType5,
    "viz-scd-type6": renderSCDType6,
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
