/**
 * application-patterns-viz.js
 * D3 v7 interactive visualization for Application Patterns index page.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-app-pipeline — interactive data pipeline stages with pattern drill-down
 */
(function () {
  "use strict";

  var COLORS = {
    dq: "#ef5350",
    transform: "#ffa726",
    enrich: "#7c4dff",
    types: "#26a69a",
    temporal: "#29b6f6",
    fg: "#546e7a",
    fgLight: "#90a4ae",
    bg: "#fff",
  };

  var STAGES = [
    {
      id: "dq", label: "Data Quality", icon: "1",
      color: COLORS.dq,
      desc: "Find and remove duplicates, validate integrity",
      patterns: ["Finding Duplicates", "Deduplication", "Hash Comparison"],
    },
    {
      id: "transform", label: "Transformation", icon: "2",
      color: COLORS.transform,
      desc: "Filter, aggregate, group, and pivot data",
      patterns: ["Filter (WHERE)", "Aggregation (GROUP BY)", "Grouping Sets (ROLLUP/CUBE)", "Pivot / Unpivot"],
    },
    {
      id: "enrich", label: "Enrichment", icon: "3",
      color: COLORS.enrich,
      desc: "Add rankings, running calculations, derived columns",
      patterns: ["Ranking (ROW_NUMBER)", "Rolling Analysis (SUM OVER)", "Analytics (CASE)", "CTE (WITH)", "Subqueries", "Derived Tables"],
    },
    {
      id: "types", label: "Types & Formats", icon: "4",
      color: COLORS.types,
      desc: "Handle numeric precision, date parsing, struct operations",
      patterns: ["Numeric (ROUND/MOD)", "Date Strings (TO_DATE)", "Keys & Structs (MAP)", "Replace Map Key"],
    },
    {
      id: "temporal", label: "Temporal", icon: "5",
      color: COLORS.temporal,
      desc: "Date hierarchies, time bands, seasonal analysis",
      patterns: ["DATE_TRUNC", "DAYOFWEEK / HOUR", "Period Comparison", "Seasonality"],
    },
  ];

  function renderAppPipeline(el) {
    var isDark = document.documentElement.getAttribute("data-md-color-scheme") === "slate";
    var TEXT = isDark ? "#eceff1" : COLORS.fg;
    var BG = isDark ? "#263238" : COLORS.bg;
    var CARD_BG = isDark ? "#37474f" : "#fafafa";

    var container = d3.select(el).style("font-family", "inherit").style("font-size", "13px");
    container.selectAll("*").remove();

    var selected = null;

    // Stage cards row
    var row = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "14px");

    var cards = row.selectAll(".stage-card").data(STAGES).enter()
      .append("div")
      .attr("class", "stage-card")
      .style("flex", "1 1 140px")
      .style("min-width", "120px")
      .style("border", function(d) { return "2px solid " + d.color; })
      .style("border-radius", "8px")
      .style("padding", "12px 10px")
      .style("cursor", "pointer")
      .style("text-align", "center")
      .style("transition", "all 0.2s")
      .style("background", CARD_BG)
      .on("click", function(event, d) {
        selected = selected === d.id ? null : d.id;
        update();
      })
      .on("mouseenter", function(event, d) {
        d3.select(this).style("transform", "translateY(-2px)")
          .style("box-shadow", "0 4px 12px rgba(0,0,0,0.12)");
      })
      .on("mouseleave", function(event, d) {
        d3.select(this).style("transform", "translateY(0)")
          .style("box-shadow", "none");
      });

    // Stage number circle
    cards.append("div")
      .style("width", "28px").style("height", "28px")
      .style("border-radius", "50%")
      .style("background", function(d) { return d.color; })
      .style("color", "#fff")
      .style("display", "flex").style("align-items", "center").style("justify-content", "center")
      .style("margin", "0 auto 6px")
      .style("font-weight", "700").style("font-size", "13px")
      .text(function(d) { return d.icon; });

    // Stage label
    cards.append("div")
      .style("font-weight", "700")
      .style("font-size", "12px")
      .style("color", TEXT)
      .text(function(d) { return d.label; });

    // Detail panel
    var detail = container.append("div")
      .style("border-radius", "8px")
      .style("padding", "14px 18px")
      .style("min-height", "60px")
      .style("transition", "all 0.3s");

    // Arrow connectors (visual pipeline flow)
    var arrowRow = container.append("div")
      .style("display", "flex")
      .style("justify-content", "center")
      .style("gap", "6px")
      .style("margin-top", "10px")
      .style("font-size", "11px")
      .style("color", COLORS.fgLight);

    STAGES.forEach(function(s, i) {
      arrowRow.append("span")
        .style("color", s.color).style("font-weight", "600")
        .text(s.label);
      if (i < STAGES.length - 1) {
        arrowRow.append("span").text(" → ");
      }
    });

    function update() {
      // Highlight selected card
      cards
        .style("background", function(d) {
          return d.id === selected ? d.color + "18" : CARD_BG;
        })
        .style("border-width", function(d) {
          return d.id === selected ? "3px" : "2px";
        });

      if (selected) {
        var stage = STAGES.find(function(s) { return s.id === selected; });
        detail
          .style("background", isDark ? "#1b2a30" : stage.color + "10")
          .style("border-left", "4px solid " + stage.color)
          .style("opacity", "1");

        var html = '<strong style="color:' + stage.color + '">' + stage.label + '</strong>';
        html += '<div style="margin-top:6px;color:' + TEXT + '">' + stage.desc + '</div>';
        html += '<div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:6px">';
        stage.patterns.forEach(function(p) {
          html += '<span style="background:' + stage.color + '22;color:' + stage.color +
            ';border:1px solid ' + stage.color + '44;border-radius:12px;padding:3px 10px;font-size:11px;font-weight:600">' +
            p + '</span>';
        });
        html += '</div>';
        detail.html(html);
      } else {
        detail
          .style("background", isDark ? "#1b2a30" : "#f5f5f5")
          .style("border-left", "4px solid " + COLORS.fgLight)
          .html('<span style="color:' + COLORS.fgLight + '">Click a stage above to see its patterns</span>');
      }
    }

    update();
  }

  /* ── Bootstrap ─────────────────────────────────────────────────── */
  function init() {
    var el = document.getElementById("viz-app-pipeline");
    if (el && !el.dataset.loaded) {
      el.dataset.loaded = "1";
      renderAppPipeline(el);
    }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(init);
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
