/**
 * patterns-overview-viz.js
 * D3 v7 interactive visualization for the Query Patterns index page.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-patterns-overview — radial category explorer with pattern counts
 */
(function () {
  "use strict";

  var CATEGORIES = [
    {
      id: "aggregation", label: "Aggregation", icon: "\u03A3",
      color: "#1e88e5", patterns: ["String Aggregation", "Conditional Aggregation"],
      complexity: 2,
    },
    {
      id: "ranking", label: "Ranking", icon: "\u25B2",
      color: "#8e24aa", patterns: ["Pagination", "Top-N", "Dense Rank"],
      complexity: 2,
    },
    {
      id: "sequence", label: "Sequence", icon: "\u2194",
      color: "#43a047", patterns: ["Gaps & Islands", "Period Comparison"],
      complexity: 3,
    },
    {
      id: "customer", label: "Customer Analytics", icon: "\u263A",
      color: "#f4511e", patterns: ["Funnel", "Retention", "Churn", "CLV", "Survival", "ABC", "Pareto"],
      complexity: 4,
    },
    {
      id: "data_quality", label: "Data Quality", icon: "\u2713",
      color: "#00897b", patterns: ["Deduplication", "Validation", "Hash Compare"],
      complexity: 2,
    },
    {
      id: "structural", label: "Structural", icon: "\u2302",
      color: "#6d4c41", patterns: ["Hierarchy", "Recursive CTE", "Graph Traversal"],
      complexity: 4,
    },
    {
      id: "scd", label: "SCD", icon: "\u0394",
      color: "#fdd835", patterns: ["Type 1", "Type 2", "Type 3", "Type 4", "Type 5", "Type 6"],
      complexity: 5,
    },
    {
      id: "timeseries", label: "Time Series", icon: "\u23F1",
      color: "#29b6f6", patterns: ["Tumbling", "Hopping", "Sliding", "Session", "Gap Fill", "Lag/Lead"],
      complexity: 4,
    },
    {
      id: "application", label: "Applications", icon: "\u2699",
      color: "#78909c", patterns: ["ETL", "Filtering", "Pivoting", "Dedup", "CTE Chains"],
      complexity: 3,
    },
  ];

  function renderOverview(container) {
    var width = Math.min(container.clientWidth || 700, 800);
    var height = 520;
    var centerX = width / 2;
    var centerY = height / 2;
    var radius = Math.min(width, height) * 0.34;

    var svg = d3.select(container)
      .html("")
      .append("svg")
      .attr("viewBox", "0 0 " + width + " " + height)
      .attr("width", "100%")
      .style("font-family", "system-ui, -apple-system, sans-serif")
      .style("max-height", "520px");

    // Central hub
    svg.append("circle")
      .attr("cx", centerX)
      .attr("cy", centerY)
      .attr("r", 40)
      .attr("fill", "#263238")
      .attr("opacity", 0.9);

    svg.append("text")
      .attr("x", centerX)
      .attr("y", centerY + 5)
      .attr("text-anchor", "middle")
      .attr("fill", "#fff")
      .attr("font-size", "11px")
      .attr("font-weight", "bold")
      .text("Patterns");

    // Tooltip
    var tooltip = d3.select(container)
      .append("div")
      .style("position", "absolute")
      .style("background", "#263238")
      .style("color", "#fff")
      .style("padding", "10px 14px")
      .style("border-radius", "8px")
      .style("font-size", "12px")
      .style("pointer-events", "none")
      .style("opacity", 0)
      .style("transition", "opacity 0.2s")
      .style("max-width", "220px")
      .style("z-index", 10);

    // Category nodes
    var angleStep = (2 * Math.PI) / CATEGORIES.length;
    var nodes = CATEGORIES.map(function (cat, i) {
      var angle = angleStep * i - Math.PI / 2;
      return {
        x: centerX + radius * Math.cos(angle),
        y: centerY + radius * Math.sin(angle),
        cat: cat,
      };
    });

    // Connecting lines
    nodes.forEach(function (node) {
      svg.append("line")
        .attr("x1", centerX)
        .attr("y1", centerY)
        .attr("x2", node.x)
        .attr("y2", node.y)
        .attr("stroke", node.cat.color)
        .attr("stroke-width", 1.5)
        .attr("stroke-dasharray", "4,3")
        .attr("opacity", 0.4);
    });

    // Category circles
    var nodeSize = 32;
    var groups = svg.selectAll(".cat-group")
      .data(nodes)
      .enter()
      .append("g")
      .attr("class", "cat-group")
      .attr("transform", function (d) { return "translate(" + d.x + "," + d.y + ")"; })
      .style("cursor", "pointer");

    // Outer ring showing complexity
    groups.append("circle")
      .attr("r", function (d) { return nodeSize + d.cat.complexity * 2; })
      .attr("fill", "none")
      .attr("stroke", function (d) { return d.cat.color; })
      .attr("stroke-width", function (d) { return d.cat.complexity; })
      .attr("opacity", 0.25);

    // Main circle
    groups.append("circle")
      .attr("r", nodeSize)
      .attr("fill", function (d) { return d.cat.color; })
      .attr("opacity", 0.9)
      .attr("stroke", "#fff")
      .attr("stroke-width", 2);

    // Icon
    groups.append("text")
      .attr("y", -4)
      .attr("text-anchor", "middle")
      .attr("fill", "#fff")
      .attr("font-size", "16px")
      .text(function (d) { return d.cat.icon; });

    // Pattern count badge
    groups.append("text")
      .attr("y", 12)
      .attr("text-anchor", "middle")
      .attr("fill", "#fff")
      .attr("font-size", "10px")
      .attr("font-weight", "bold")
      .text(function (d) { return d.cat.patterns.length; });

    // Label below
    groups.append("text")
      .attr("y", nodeSize + 16)
      .attr("text-anchor", "middle")
      .attr("fill", "#546e7a")
      .attr("font-size", "11px")
      .attr("font-weight", "600")
      .text(function (d) { return d.cat.label; });

    // Interactions
    groups.on("mouseover", function (event, d) {
      d3.select(this).select("circle:nth-child(2)")
        .transition().duration(200)
        .attr("r", nodeSize + 6)
        .attr("opacity", 1);

      var html = "<strong>" + d.cat.label + "</strong><br/>" +
        "<span style='opacity:0.7'>Patterns:</span><br/>" +
        d.cat.patterns.map(function (p) { return "\u2022 " + p; }).join("<br/>") +
        "<br/><br/><span style='opacity:0.7'>Complexity: " +
        "\u2605".repeat(d.cat.complexity) + "\u2606".repeat(5 - d.cat.complexity) + "</span>";

      tooltip.html(html).style("opacity", 1);
    })
    .on("mousemove", function (event) {
      var rect = container.getBoundingClientRect();
      tooltip
        .style("left", (event.clientX - rect.left + 12) + "px")
        .style("top", (event.clientY - rect.top - 10) + "px");
    })
    .on("mouseout", function () {
      d3.select(this).select("circle:nth-child(2)")
        .transition().duration(200)
        .attr("r", nodeSize)
        .attr("opacity", 0.9);

      tooltip.style("opacity", 0);
    });

    // Legend
    var legend = svg.append("g")
      .attr("transform", "translate(10," + (height - 30) + ")");

    legend.append("text")
      .attr("fill", "#90a4ae")
      .attr("font-size", "10px")
      .text("Ring thickness = complexity \u2022 Number = pattern count \u2022 Hover for details");
  }

  function init() {
    var el = document.getElementById("viz-patterns-overview");
    if (!el) return;
    if (el.dataset.rendered === "1") return;
    el.dataset.rendered = "1";
    el.style.position = "relative";
    renderOverview(el);
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(function () {
      setTimeout(init, 100);
    });
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
