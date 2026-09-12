/**
 * generator-viz.js
 * D3 v7 interactive visualizations for the generator function pages
 * (EXPLODE / EXPLODE_OUTER / INLINE). Compatible with MkDocs Material
 * instant navigation (document$ observable).
 *
 * Viz catalogue:
 *   #viz-explode       — input rows (with array/NULL/empty) fanning out
 *                        into their EXPLODE output rows; NULL/empty rows
 *                        are dropped. Click a row to trace it.
 *   #viz-explode-outer — same input rows, but NULL/empty arrays produce
 *                        one output row with col = NULL instead of being
 *                        dropped. Click a row to trace it.
 *   #viz-inline        — input rows with an array of structs fanning out
 *                        into rows where each struct FIELD becomes its
 *                        own output column (product, qty), contrasted
 *                        with EXPLODE's single struct-shaped column.
 *   #viz-inline-outer  — same as #viz-inline, but empty/NULL arrays are
 *                        kept as one row with product = NULL, qty = NULL
 *                        instead of being dropped.
 *   #viz-stack         — a flat K-expression list regrouped into n rows
 *                        of K/n columns (row-major order); toggle n to
 *                        see the same values land in different cells.
 */
(function () {
  "use strict";

  const PURPLE = "#7c4dff";
  const TEAL = "#26a69a";
  const AMBER = "#ffa726";
  const RED = "#ef5350";
  const GRAY = "#90a4ae";
  const FG = "#546e7a";

  const COLORS = [PURPLE, TEAL, AMBER];

  function makeTooltip(parent) {
    const div = document.createElement("div");
    div.className = "ts-tooltip";
    div.style.cssText = "opacity:0;position:absolute;pointer-events:none;";
    parent.style.position = "relative";
    parent.appendChild(div);
    return {
      show(html, x, y) { div.innerHTML = html; div.style.opacity = 1; div.style.left = x + "px"; div.style.top = y + "px"; },
      hide() { div.style.opacity = 0; },
    };
  }

  function renderExplode(el, outer) {
    const inputs = [
      { id: 1, label: "1", arr: ["a", "b", "c"] },
      { id: 2, label: "2", arr: ["x", "y"] },
      { id: 3, label: "3", arr: null }, // NULL array → dropped by EXPLODE, kept by EXPLODE_OUTER
    ];

    let selected = null;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const legend = container.append("div")
      .style("font-size", "0.72rem").style("color", GRAY)
      .style("margin-bottom", "0.5rem")
      .text("Click a row on the left to trace where it goes.");

    const wrap = container.append("div")
      .style("display", "flex").style("align-items", "stretch")
      .style("gap", "0").style("flex-wrap", "wrap");

    const leftCol = wrap.append("div").style("flex", "0 0 220px");
    leftCol.append("div")
      .style("font-weight", "700").style("font-size", "0.78rem")
      .style("color", FG).style("margin-bottom", "0.3rem")
      .text("Input rows (id, arr)");
    const leftList = leftCol.append("div");

    const midCol = wrap.append("div")
      .style("flex", "0 0 90px").style("position", "relative")
      .style("min-height", "220px");
    const svg = midCol.append("svg")
      .attr("width", "100%").attr("height", "100%")
      .style("position", "absolute").style("top", "0").style("left", "0")
      .style("overflow", "visible");

    const rightCol = wrap.append("div").style("flex", "0 0 260px");
    rightCol.append("div")
      .style("font-weight", "700").style("font-size", "0.78rem")
      .style("color", FG).style("margin-bottom", "0.3rem")
      .text(outer ? "EXPLODE_OUTER output rows (id, col)" : "EXPLODE output rows (id, col)");
    const rightList = rightCol.append("div");

    const tip = makeTooltip(el);

    const rowH = 34;
    const rowGap = 6;

    function outputRows() {
      const rows = [];
      inputs.forEach((d, i) => {
        if (!d.arr || d.arr.length === 0) {
          if (outer) rows.push({ srcId: d.id, srcIdx: i, val: null }); // kept as NULL row
          return; // dropped (non-outer)
        }
        d.arr.forEach((v) => rows.push({ srcId: d.id, srcIdx: i, val: v }));
      });
      return rows;
    }

    function update() {
      leftList.selectAll("*").remove();
      rightList.selectAll("*").remove();
      svg.selectAll("*").remove();

      const outs = outputRows();

      // Left rows
      const leftRows = leftList.selectAll(".in-row")
        .data(inputs)
        .join("div")
        .attr("class", "in-row")
        .style("height", rowH + "px")
        .style("margin-bottom", rowGap + "px")
        .style("display", "flex").style("align-items", "center")
        .style("gap", "8px").style("padding", "0 10px")
        .style("border-radius", "6px").style("cursor", "pointer")
        .style("font-size", "0.75rem").style("font-family", "monospace")
        .style("background", (d, i) => selected === i ? "rgba(124,77,255,0.12)" : "#f5f5f7")
        .style("border", (d, i) => selected === i ? `1.5px solid ${COLORS[i % COLORS.length]}` : "1px solid #e0e0e0")
        .on("click", function (ev, d) {
          const idx = inputs.indexOf(d);
          selected = selected === idx ? null : idx;
          update();
        })
        .on("mouseenter", function (ev, d) {
          const empty = !d.arr || d.arr.length === 0;
          let note = "";
          if (empty) {
            note = outer
              ? `<br><span style="color:${AMBER}">kept by EXPLODE_OUTER → 1 row with col = NULL</span>`
              : `<br><span style="color:${RED}">⊘ dropped by EXPLODE (NULL/empty)</span>`;
          }
          tip.show(
            `<strong>id = ${d.id}</strong><br>arr = ${d.arr ? "[" + d.arr.join(", ") + "]" : "NULL"}` + note,
            ev.offsetX + 14, ev.offsetY - 20
          );
        })
        .on("mouseleave", () => tip.hide());

      leftRows.each(function (d, i) {
        const row = d3.select(this);
        row.append("span").style("color", GRAY).text(`id=${d.id}`);
        if (!d.arr || d.arr.length === 0) {
          row.append("span").style("color", RED).style("font-style", "italic")
            .text(d.arr === null ? "arr=NULL" : "arr=[]");
        } else {
          row.append("span").style("color", FG).text("arr=[" + d.arr.join(",") + "]");
        }
      });

      // Right rows
      const rightRows = rightList.selectAll(".out-row")
        .data(outs)
        .join("div")
        .attr("class", "out-row")
        .style("height", rowH + "px")
        .style("margin-bottom", rowGap + "px")
        .style("display", "flex").style("align-items", "center")
        .style("gap", "8px").style("padding", "0 10px")
        .style("border-radius", "6px")
        .style("font-size", "0.75rem").style("font-family", "monospace")
        .style("background", (d) => selected === d.srcIdx ? "rgba(124,77,255,0.12)" : "#f5f5f7")
        .style("border", (d) => selected === d.srcIdx
          ? `1.5px solid ${COLORS[d.srcIdx % COLORS.length]}` : "1px solid #e0e0e0")
        .style("opacity", (d) => selected === null || selected === d.srcIdx ? 1 : 0.35);

      rightRows.each(function (d) {
        const row = d3.select(this);
        row.append("span").style("color", GRAY).text(`id=${d.srcId}`);
        if (d.val === null) {
          row.append("span").style("color", AMBER).style("font-style", "italic").text("col=NULL");
        } else {
          row.append("span").style("color", TEAL).text(`col=${d.val}`);
        }
      });

      // Connector lines (input row center → each of its output rows)
      const leftH = inputs.length * (rowH + rowGap);
      const rightH = Math.max(outs.length * (rowH + rowGap), rowH);
      const totalH = Math.max(leftH, rightH, 40);
      svg.attr("viewBox", `0 0 90 ${totalH}`);

      inputs.forEach((d, i) => {
        const y0 = i * (rowH + rowGap) + rowH / 2;
        const rowOuts = outs.map((o, oi) => ({ o, oi })).filter(({ o }) => o.srcIdx === i);
        if (rowOuts.length === 0) {
          // dropped row (non-outer only): short dead-end stub
          svg.append("line")
            .attr("x1", 0).attr("y1", y0).attr("x2", 24).attr("y2", y0)
            .attr("stroke", RED).attr("stroke-width", selected === i ? 2 : 1)
            .attr("stroke-dasharray", "3,3")
            .attr("opacity", selected === null || selected === i ? 1 : 0.25);
          svg.append("text")
            .attr("x", 30).attr("y", y0 + 3)
            .attr("font-size", "0.6rem").attr("fill", RED)
            .text("✕");
          return;
        }
        rowOuts.forEach(({ oi }) => {
          const y1 = oi * (rowH + rowGap) + rowH / 2;
          svg.append("path")
            .attr("d", `M0,${y0} C 45,${y0} 45,${y1} 90,${y1}`)
            .attr("fill", "none")
            .attr("stroke", outs[rowOuts[0].oi].val === null ? AMBER : COLORS[i % COLORS.length])
            .attr("stroke-width", selected === i ? 2.2 : 1.2)
            .attr("opacity", selected === null || selected === i ? 0.9 : 0.15);
        });
      });
    }

    update();
  }

  function renderInline(el, outer) {
    const inputs = [
      { id: 1001, items: [{ product: "book", qty: 2 }, { product: "pen", qty: 5 }] },
      { id: 1002, items: [{ product: "notebook", qty: 1 }] },
      { id: 1003, items: [] }, // empty array → dropped by INLINE, kept by INLINE_OUTER
    ];

    let selected = null;

    const container = d3.select(el);
    container.selectAll("*").remove();

    container.append("div")
      .style("font-size", "0.72rem").style("color", GRAY)
      .style("margin-bottom", "0.5rem")
      .text("Click a row on the left to trace where it goes.");

    const wrap = container.append("div")
      .style("display", "flex").style("align-items", "stretch")
      .style("gap", "0").style("flex-wrap", "wrap");

    const leftCol = wrap.append("div").style("flex", "0 0 240px");
    leftCol.append("div")
      .style("font-weight", "700").style("font-size", "0.78rem")
      .style("color", FG).style("margin-bottom", "0.3rem")
      .text("Input rows (order_id, items[])");
    const leftList = leftCol.append("div");

    const midCol = wrap.append("div")
      .style("flex", "0 0 90px").style("position", "relative")
      .style("min-height", "220px");
    const svg = midCol.append("svg")
      .attr("width", "100%").attr("height", "100%")
      .style("position", "absolute").style("top", "0").style("left", "0")
      .style("overflow", "visible");

    const rightCol = wrap.append("div").style("flex", "0 0 300px");
    rightCol.append("div")
      .style("font-weight", "700").style("font-size", "0.78rem")
      .style("color", FG).style("margin-bottom", "0.3rem")
      .text(outer ? "INLINE_OUTER output rows — 2 separate columns" : "INLINE output rows — 2 separate columns");
    const rightHeader = rightCol.append("div")
      .style("display", "flex").style("gap", "16px")
      .style("font-size", "0.66rem").style("color", GRAY)
      .style("margin-bottom", "2px");
    rightHeader.append("span").style("width", "40px").text("order_id");
    rightHeader.append("span").style("width", "90px").text("product");
    rightHeader.append("span").style("width", "40px").text("qty");
    const rightList = rightCol.append("div");

    const tip = makeTooltip(el);

    const rowH = 34;
    const rowGap = 6;

    function outputRows() {
      const rows = [];
      inputs.forEach((d, i) => {
        if (!d.items || d.items.length === 0) {
          if (outer) rows.push({ srcId: d.id, srcIdx: i, product: null, qty: null }); // kept as NULL row
          return; // dropped (non-outer)
        }
        d.items.forEach((it) => rows.push({ srcId: d.id, srcIdx: i, product: it.product, qty: it.qty }));
      });
      return rows;
    }

    function update() {
      leftList.selectAll("*").remove();
      rightList.selectAll("*").remove();
      svg.selectAll("*").remove();

      const outs = outputRows();

      const leftRows = leftList.selectAll(".in-row")
        .data(inputs)
        .join("div")
        .attr("class", "in-row")
        .style("height", rowH + "px")
        .style("margin-bottom", rowGap + "px")
        .style("display", "flex").style("align-items", "center")
        .style("gap", "8px").style("padding", "0 10px")
        .style("border-radius", "6px").style("cursor", "pointer")
        .style("font-size", "0.72rem").style("font-family", "monospace")
        .style("background", (d, i) => selected === i ? "rgba(124,77,255,0.12)" : "#f5f5f7")
        .style("border", (d, i) => selected === i ? `1.5px solid ${COLORS[i % COLORS.length]}` : "1px solid #e0e0e0")
        .on("click", function (ev, d) {
          const idx = inputs.indexOf(d);
          selected = selected === idx ? null : idx;
          update();
        })
        .on("mouseenter", function (ev, d) {
          const empty = !d.items || d.items.length === 0;
          let note = "";
          if (empty) {
            note = outer
              ? `<br><span style="color:${AMBER}">kept by INLINE_OUTER → 1 row with product = NULL, qty = NULL</span>`
              : `<br><span style="color:${RED}">⊘ dropped by INLINE (empty array)</span>`;
          }
          const itemsStr = d.items.map((it) => `{${it.product}, ${it.qty}}`).join(", ");
          tip.show(
            `<strong>order_id = ${d.id}</strong><br>items = [${itemsStr}]` + note,
            ev.offsetX + 14, ev.offsetY - 20
          );
        })
        .on("mouseleave", () => tip.hide());

      leftRows.each(function (d) {
        const row = d3.select(this);
        row.append("span").style("color", GRAY).text(`id=${d.id}`);
        if (!d.items || d.items.length === 0) {
          row.append("span").style("color", RED).style("font-style", "italic").text("items=[]");
        } else {
          row.append("span").style("color", FG)
            .text("items=[" + d.items.map((it) => `${it.product}:${it.qty}`).join(",") + "]");
        }
      });

      const rightRows = rightList.selectAll(".out-row")
        .data(outs)
        .join("div")
        .attr("class", "out-row")
        .style("height", rowH + "px")
        .style("margin-bottom", rowGap + "px")
        .style("display", "flex").style("align-items", "center")
        .style("gap", "16px").style("padding", "0 10px")
        .style("border-radius", "6px")
        .style("font-size", "0.72rem").style("font-family", "monospace")
        .style("background", (d) => selected === d.srcIdx ? "rgba(124,77,255,0.12)" : "#f5f5f7")
        .style("border", (d) => selected === d.srcIdx
          ? `1.5px solid ${COLORS[d.srcIdx % COLORS.length]}` : "1px solid #e0e0e0")
        .style("opacity", (d) => selected === null || selected === d.srcIdx ? 1 : 0.35);

      rightRows.each(function (d) {
        const row = d3.select(this);
        row.append("span").style("width", "40px").style("color", GRAY).text(d.srcId);
        if (d.product === null) {
          row.append("span").style("width", "90px").style("color", AMBER).style("font-style", "italic").text("NULL");
          row.append("span").style("width", "40px").style("color", AMBER).style("font-style", "italic").text("NULL");
        } else {
          row.append("span").style("width", "90px").style("color", TEAL).text(d.product);
          row.append("span").style("width", "40px").style("color", TEAL).text(d.qty);
        }
      });

      const leftH = inputs.length * (rowH + rowGap);
      const rightH = Math.max(outs.length * (rowH + rowGap), rowH);
      const totalH = Math.max(leftH, rightH, 40);
      svg.attr("viewBox", `0 0 90 ${totalH}`);

      inputs.forEach((d, i) => {
        const y0 = i * (rowH + rowGap) + rowH / 2;
        const rowOuts = outs.map((o, oi) => ({ o, oi })).filter(({ o }) => o.srcIdx === i);
        if (rowOuts.length === 0) {
          svg.append("line")
            .attr("x1", 0).attr("y1", y0).attr("x2", 24).attr("y2", y0)
            .attr("stroke", RED).attr("stroke-width", selected === i ? 2 : 1)
            .attr("stroke-dasharray", "3,3")
            .attr("opacity", selected === null || selected === i ? 1 : 0.25);
          svg.append("text")
            .attr("x", 30).attr("y", y0 + 3)
            .attr("font-size", "0.6rem").attr("fill", RED)
            .text("✕");
          return;
        }
        rowOuts.forEach(({ oi }) => {
          const y1 = oi * (rowH + rowGap) + rowH / 2;
          svg.append("path")
            .attr("d", `M0,${y0} C 45,${y0} 45,${y1} 90,${y1}`)
            .attr("fill", "none")
            .attr("stroke", outs[rowOuts[0].oi].product === null ? AMBER : COLORS[i % COLORS.length])
            .attr("stroke-width", selected === i ? 2.2 : 1.2)
            .attr("opacity", selected === null || selected === i ? 0.9 : 0.15);
        });
      });
    }

    update();
  }

  function renderStack(el) {
    // K = 6 flat expressions, row-major order — same list, regrouped by n
    const values = ["math", 85, "science", 90, "history", 78];
    const K = values.length;
    const divisors = [1, 2, 3, 6].filter((n) => K % n === 0);

    let n = 3;
    let selectedVal = null; // index into `values`

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div")
      .style("display", "flex").style("align-items", "center")
      .style("gap", "0.5rem").style("margin-bottom", "0.6rem")
      .style("font-size", "0.78rem");
    ctrl.append("span").style("color", FG).style("font-weight", "600").text("n =");
    const btns = divisors.map((d) =>
      ctrl.append("button")
        .style("padding", "3px 10px").style("border-radius", "4px")
        .style("border", "1px solid #ccc").style("font-size", "0.74rem")
        .style("cursor", "pointer").style("font-family", "monospace")
        .text(d)
        .on("click", () => { n = d; update(); })
    );
    ctrl.append("span").style("color", GRAY).style("font-size", "0.7rem")
      .text(`(K=${K} expressions → K/n = ${K}/n columns per row)`);

    container.append("div")
      .style("font-weight", "700").style("font-size", "0.76rem")
      .style("color", FG).style("margin", "0.4rem 0 0.2rem")
      .text("Flat expression list (row-major order)");
    const chipRow = container.append("div")
      .style("display", "flex").style("gap", "6px").style("margin-bottom", "0.8rem").style("flex-wrap", "wrap");

    container.append("div")
      .style("font-weight", "700").style("font-size", "0.76rem")
      .style("color", FG).style("margin-bottom", "0.3rem")
      .text("STACK(n, ...) output rows");
    const outputGrid = container.append("div");

    const tip = makeTooltip(el);

    function update() {
      btns.forEach((b, i) => {
        const active = divisors[i] === n;
        b.style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border-color", active ? PURPLE : "#ccc");
      });

      const cols = K / n;

      chipRow.selectAll("*").remove();
      values.forEach((v, i) => {
        const row = Math.floor(i / cols);
        const col = i % cols;
        chipRow.append("div")
          .style("padding", "4px 10px").style("border-radius", "4px")
          .style("font-size", "0.72rem").style("font-family", "monospace")
          .style("cursor", "pointer")
          .style("background", selectedVal === i ? COLORS[row % COLORS.length] : "#f5f5f7")
          .style("color", selectedVal === i ? "#fff" : FG)
          .style("border", `1px solid ${selectedVal === i ? COLORS[row % COLORS.length] : "#ddd"}`)
          .text(String(v))
          .on("click", () => { selectedVal = selectedVal === i ? null : i; update(); })
          .on("mouseenter", function (ev) {
            tip.show(`index ${i} → row ${row}, col ${col}`, ev.offsetX + 10, ev.offsetY - 24);
          })
          .on("mouseleave", () => tip.hide());
      });

      outputGrid.selectAll("*").remove();
      const table = outputGrid.append("div").style("display", "inline-block");
      for (let r = 0; r < n; r++) {
        const rowDiv = table.append("div").style("display", "flex").style("gap", "6px").style("margin-bottom", "4px");
        for (let c = 0; c < cols; c++) {
          const idx = r * cols + c;
          const v = values[idx];
          const isSel = selectedVal === idx;
          rowDiv.append("div")
            .style("min-width", "70px").style("padding", "4px 10px")
            .style("border-radius", "4px").style("text-align", "center")
            .style("font-size", "0.72rem").style("font-family", "monospace")
            .style("background", isSel ? COLORS[r % COLORS.length] : "rgba(0,0,0,0.04)")
            .style("color", isSel ? "#fff" : FG)
            .style("border", `1px solid ${isSel ? COLORS[r % COLORS.length] : "#ddd"}`)
            .text(String(v));
        }
      }
    }

    update();
  }

  function init() {
    const el = document.getElementById("viz-explode");
    if (el && !el.dataset.rendered) { renderExplode(el, false); el.dataset.rendered = "1"; }
    const elOuter = document.getElementById("viz-explode-outer");
    if (elOuter && !elOuter.dataset.rendered) { renderExplode(elOuter, true); elOuter.dataset.rendered = "1"; }
    const elInline = document.getElementById("viz-inline");
    if (elInline && !elInline.dataset.rendered) { renderInline(elInline, false); elInline.dataset.rendered = "1"; }
    const elInlineOuter = document.getElementById("viz-inline-outer");
    if (elInlineOuter && !elInlineOuter.dataset.rendered) { renderInline(elInlineOuter, true); elInlineOuter.dataset.rendered = "1"; }
    const elStack = document.getElementById("viz-stack");
    if (elStack && !elStack.dataset.rendered) { renderStack(elStack); elStack.dataset.rendered = "1"; }
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
