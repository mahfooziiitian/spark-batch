(function () {
  "use strict";

  const PURPLE = "#7c4dff";
  const TEAL = "#26a69a";
  const AMBER = "#ffa726";
  const RED = "#ef5350";
  const GRAY = "#90a4ae";
  const FG = "#546e7a";
  const HIT_COLOR = "#26a69a";

  function chip(parent, text, opts) {
    opts = opts || {};
    return parent.append("div")
      .style("display", "inline-flex")
      .style("align-items", "center")
      .style("padding", "5px 12px")
      .style("border-radius", "5px")
      .style("font-size", "0.74rem")
      .style("font-family", "monospace")
      .style("cursor", opts.clickable ? "pointer" : "default")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .style("opacity", opts.opacity != null ? opts.opacity : 1)
      .text(text);
  }

  function box(parent, title, opts) {
    opts = opts || {};
    const wrap = parent.append("div")
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .style("background", opts.bg || "#fff")
      .style("border-radius", "8px")
      .style("padding", "10px 12px");
    wrap.append("div")
      .style("font-size", "0.72rem")
      .style("font-weight", "700")
      .style("color", opts.titleColor || FG)
      .style("margin-bottom", "0.35rem")
      .text(title);
    return wrap;
  }

  function renderEncryptionOverview(el) {
    const source = "555-12-3456";
    const modes = [
      {
        name: "Hash",
        color: PURPLE,
        output: "175dbb7c6c96...bd6b3a8c",
        note: "One-way digest for joins, dedup, and pseudonymous IDs.",
        chips: ["deterministic", "not reversible", "stable key candidate"],
      },
      {
        name: "Mask",
        color: TEAL,
        output: "nnn-nn-nnnn",
        note: "Human-safe redaction that keeps the visible format.",
        chips: ["display-safe", "many-to-one", "shape preserved"],
      },
      {
        name: "Hex",
        color: AMBER,
        output: "3535352D31322D33343536",
        note: "Reversible byte encoding; useful for transport and debugging.",
        chips: ["reversible", "byte-oriented", "not anonymization"],
      },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("margin-bottom", "0.7rem");
    modes.forEach((m, i) => {
      tabs.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .text(m.name)
        .on("click", () => { idx = i; update(); });
    });

    const io = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(220px, 1fr))")
      .style("gap", "10px")
      .style("margin-bottom", "0.6rem");
    const traits = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap");

    function update() {
      const mode = modes[idx];
      tabs.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? mode.color : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? mode.color : "#ccc"}`);
      });

      io.selectAll("*").remove();
      traits.selectAll("*").remove();

      box(io, "Source value", { border: "#ddd", bg: "#fff" })
        .append("div")
        .style("font-family", "monospace")
        .style("font-size", "0.88rem")
        .style("color", FG)
        .text(source);

      const out = box(io, `${mode.name} output`, {
        border: mode.color,
        bg: mode.name === "Hash" ? "rgba(124,77,255,0.06)" : mode.name === "Mask" ? "rgba(38,166,154,0.06)" : "rgba(255,167,38,0.08)",
        titleColor: mode.color,
      });
      out.append("div")
        .style("font-family", "monospace")
        .style("font-size", "0.82rem")
        .style("word-break", "break-all")
        .style("color", FG)
        .text(mode.output);
      out.append("div")
        .style("font-size", "0.72rem")
        .style("color", GRAY)
        .style("margin-top", "0.4rem")
        .text(mode.note);

      mode.chips.forEach((label) => {
        chip(traits, label, {
          bg: mode.name === "Hash" ? "rgba(124,77,255,0.08)" : mode.name === "Mask" ? "rgba(38,166,154,0.08)" : "rgba(255,167,38,0.10)",
          fg: mode.color,
          border: mode.color,
        });
      });
    }
    update();
  }

  function renderEncryptionContrast(el) {
    const modes = [
      {
        name: "MASK",
        color: TEAL,
        a: "xxxxx@xxxxxxx.xxx",
        b: "xxxxx@xxxxxxx.xxx",
        summary: "Both emails collapse to the same masked pattern.",
      },
      {
        name: "SHA2",
        color: PURPLE,
        a: "ff8d9819fc0e...a8c6d976",
        b: "fb3222ce381b...90555296",
        summary: "The digests stay distinct, so they remain useful for joins or dedup.",
      },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("margin-bottom", "0.7rem");
    modes.forEach((m, i) => {
      ctrl.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .text(m.name)
        .on("click", () => { idx = i; update(); });
    });

    const grid = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(220px, 1fr))")
      .style("gap", "10px")
      .style("margin-bottom", "0.6rem");
    const result = container.append("div")
      .style("font-size", "0.76rem")
      .style("font-weight", "700");

    function update() {
      const mode = modes[idx];
      ctrl.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? mode.color : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? mode.color : "#ccc"}`);
      });

      grid.selectAll("*").remove();
      result.text("");

      [
        ["alice@example.com", mode.a],
        ["bruce@example.com", mode.b],
      ].forEach(([src, out]) => {
        const b = box(grid, src, {
          border: mode.color,
          bg: mode.name === "MASK" ? "rgba(38,166,154,0.06)" : "rgba(124,77,255,0.06)",
          titleColor: FG,
        });
        b.append("div")
          .style("font-family", "monospace")
          .style("font-size", "0.8rem")
          .style("word-break", "break-all")
          .style("color", mode.color)
          .text(out);
      });

      const same = mode.a === mode.b;
      result
        .style("color", same ? RED : HIT_COLOR)
        .text(`${mode.name}: outputs are ${same ? "the same" : "different"}. ${mode.summary}`);
    }
    update();
  }

  function renderCrc32VsSha(el) {
    const states = [
      {
        name: "Original",
        payload: "invoice-1001|99.95",
        crc: "2849482112",
        sha: "cf45ac55cff0...0c191666",
      },
      {
        name: "Modified",
        payload: "invoice-1001|99.96",
        crc: "819917882",
        sha: "c170a5237629...8e783430",
      },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("margin-bottom", "0.7rem");
    states.forEach((s, i) => {
      ctrl.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .text(s.name)
        .on("click", () => { idx = i; update(); });
    });

    const grid = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(220px, 1fr))")
      .style("gap", "10px");

    function update() {
      ctrl.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? PURPLE : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });

      const state = states[idx];
      grid.selectAll("*").remove();

      box(grid, "Payload", { border: "#ddd" })
        .append("div")
        .style("font-family", "monospace")
        .style("font-size", "0.82rem")
        .style("color", FG)
        .text(state.payload);

      const crc = box(grid, "CRC32", { border: TEAL, bg: "rgba(38,166,154,0.06)", titleColor: TEAL });
      crc.append("div")
        .style("font-family", "monospace")
        .style("font-size", "0.92rem")
        .style("color", TEAL)
        .text(state.crc);
      crc.append("div")
        .style("font-size", "0.7rem")
        .style("color", GRAY)
        .style("margin-top", "0.35rem")
        .text("32-bit numeric checksum");

      const sha = box(grid, "SHA2-256", { border: PURPLE, bg: "rgba(124,77,255,0.06)", titleColor: PURPLE });
      sha.append("div")
        .style("font-family", "monospace")
        .style("font-size", "0.8rem")
        .style("word-break", "break-all")
        .style("color", PURPLE)
        .text(state.sha);
      sha.append("div")
        .style("font-size", "0.7rem")
        .style("color", GRAY)
        .style("margin-top", "0.35rem")
        .text("256-bit cryptographic digest");
    }
    update();
  }

  function renderHexRoundtrip(el) {
    const samples = [
      { name: "Spark", bytes: ["53", "70", "61", "72", "6B"], text: "Spark", color: PURPLE },
      { name: "é", bytes: ["C3", "A9"], text: "é", color: AMBER },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("margin-bottom", "0.7rem");
    samples.forEach((s, i) => {
      tabs.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .text(s.name)
        .on("click", () => { idx = i; update(); });
    });

    function row(label) {
      const wrap = container.append("div").style("margin-bottom", "0.45rem");
      wrap.append("div")
        .style("font-size", "0.68rem")
        .style("font-weight", "700")
        .style("color", FG)
        .style("margin-bottom", "3px")
        .text(label);
      return wrap.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap");
    }

    const inRow = row("Input text");
    container.append("div")
      .style("text-align", "center")
      .style("color", GRAY)
      .style("font-size", "0.85rem")
      .style("margin", "2px 0")
      .text("↓ UTF-8 bytes ↓");
    const byteRow = row("Byte sequence");
    container.append("div")
      .style("text-align", "center")
      .style("color", GRAY)
      .style("font-size", "0.85rem")
      .style("margin", "2px 0")
      .text("↓ HEX(...) ↓");
    const hexRow = row("Hex pairs");
    const footer = container.append("div").style("font-size", "0.74rem").style("color", GRAY);

    function update() {
      const sample = samples[idx];
      tabs.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? sample.color : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? sample.color : "#ccc"}`);
      });

      inRow.selectAll("*").remove();
      byteRow.selectAll("*").remove();
      hexRow.selectAll("*").remove();

      chip(inRow, sample.text, { bg: "#f5f5f7", fg: FG, border: "#ddd" });
      sample.bytes.forEach((b) => chip(byteRow, b, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }));
      sample.bytes.forEach((b) => chip(hexRow, b, { bg: "rgba(255,167,38,0.10)", fg: AMBER, border: AMBER }));
      footer.text(sample.bytes.length === 1 ? "One byte becomes one hex pair." : `This value uses ${sample.bytes.length} UTF-8 bytes, so HEX emits ${sample.bytes.length} byte pairs.`);
    }
    update();
  }

  function renderMaskClasses(el) {
    const source = ["A", "b", "-", "1", "9"];
    const modes = [
      {
        name: "Default",
        output: ["X", "x", "-", "n", "n"],
        color: TEAL,
        note: "Uppercase, lowercase, and digits are replaced; punctuation stays visible.",
      },
      {
        name: "Keep digits",
        output: ["U", "l", "*", "1", "9"],
        color: AMBER,
        note: "Passing NULL for the digit rule leaves digits unchanged.",
      },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const ctrl = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("margin-bottom", "0.7rem");
    modes.forEach((m, i) => {
      ctrl.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .text(m.name)
        .on("click", () => { idx = i; update(); });
    });

    function row(label) {
      const wrap = container.append("div").style("margin-bottom", "0.45rem");
      wrap.append("div")
        .style("font-size", "0.68rem")
        .style("font-weight", "700")
        .style("color", FG)
        .style("margin-bottom", "3px")
        .text(label);
      return wrap.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap");
    }

    const srcRow = row("Source characters");
    const outRow = row("Masked characters");
    const note = container.append("div").style("font-size", "0.74rem").style("color", GRAY);

    function update() {
      const mode = modes[idx];
      ctrl.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? mode.color : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? mode.color : "#ccc"}`);
      });

      srcRow.selectAll("*").remove();
      outRow.selectAll("*").remove();

      source.forEach((ch) => chip(srcRow, ch, { bg: "#f5f5f7", fg: FG, border: "#ddd" }));
      mode.output.forEach((ch) => chip(outRow, ch, {
        bg: idx === 0 ? "rgba(38,166,154,0.10)" : "rgba(255,167,38,0.10)",
        fg: mode.color,
        border: mode.color,
      }));
      note.text(mode.note);
    }
    update();
  }

  function renderMd5Fit(el) {
    const modes = [
      {
        name: "Dedup key",
        color: HIT_COLOR,
        verdict: "Reasonable fit",
        note: "Deterministic, compact, and fast enough for non-adversarial row fingerprints.",
      },
      {
        name: "Password hash",
        color: RED,
        verdict: "Bad fit",
        note: "Modern security needs stronger, attack-resistant password hashing than MD5 provides.",
      },
    ];
    let idx = 0;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("margin-bottom", "0.7rem");
    modes.forEach((m, i) => {
      tabs.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-family", "monospace")
        .style("font-size", "0.74rem")
        .text(m.name)
        .on("click", () => { idx = i; update(); });
    });

    const panel = container.append("div");

    function update() {
      const mode = modes[idx];
      tabs.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? mode.color : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? mode.color : "#ccc"}`);
      });

      panel.selectAll("*").remove();
      const b = box(panel, mode.verdict, {
        border: mode.color,
        bg: idx === 0 ? "rgba(38,166,154,0.08)" : "rgba(239,83,80,0.08)",
        titleColor: mode.color,
      });
      b.append("div")
        .style("font-family", "monospace")
        .style("font-size", "0.8rem")
        .style("margin-bottom", "0.4rem")
        .style("color", FG)
        .text("md5('customer-42|2026-09-11') -> c59f5a1f...");
      b.append("div")
        .style("font-size", "0.74rem")
        .style("color", FG)
        .text(mode.note);
    }
    update();
  }

  function renderShaFamily(el) {
    const variants = [
      { name: "SHA1", bits: 160, chars: 40, color: RED, note: "Legacy compatibility only" },
      { name: "SHA2-224", bits: 224, chars: 56, color: AMBER, note: "Smaller cryptographic digest" },
      { name: "SHA2-256", bits: 256, chars: 64, color: TEAL, note: "Recommended default" },
      { name: "SHA2-384", bits: 384, chars: 96, color: PURPLE, note: "Higher-assurance digest" },
      { name: "SHA2-512", bits: 512, chars: 128, color: FG, note: "Largest SHA2 digest in Spark" },
    ];
    let idx = 2;

    const container = d3.select(el);
    container.selectAll("*").remove();

    const tabs = container.append("div")
      .style("display", "flex")
      .style("gap", "8px")
      .style("flex-wrap", "wrap")
      .style("margin-bottom", "0.7rem");
    variants.forEach((v, i) => {
      tabs.append("button")
        .attr("data-i", i)
        .style("padding", "5px 12px")
        .style("border-radius", "5px")
        .style("cursor", "pointer")
        .style("font-family", "monospace")
        .style("font-size", "0.72rem")
        .text(v.name)
        .on("click", () => { idx = i; update(); });
    });

    const grid = container.append("div")
      .style("display", "grid")
      .style("grid-template-columns", "repeat(auto-fit, minmax(180px, 1fr))")
      .style("gap", "10px");

    function update() {
      const v = variants[idx];
      tabs.selectAll("button").each(function (_, i) {
        const active = i === idx;
        d3.select(this)
          .style("background", active ? v.color : "#fff")
          .style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? v.color : "#ccc"}`);
      });

      grid.selectAll("*").remove();
      const b1 = box(grid, "Digest size", { border: v.color, bg: "rgba(124,77,255,0.05)", titleColor: v.color });
      b1.append("div").style("font-size", "1rem").style("font-family", "monospace").style("color", v.color).text(`${v.bits} bits`);

      const b2 = box(grid, "Hex length", { border: v.color, bg: "#fff", titleColor: v.color });
      b2.append("div").style("font-size", "1rem").style("font-family", "monospace").style("color", FG).text(`${v.chars} chars`);

      const b3 = box(grid, "Recommendation", { border: v.color, bg: "#fff", titleColor: v.color });
      b3.append("div").style("font-size", "0.78rem").style("color", FG).text(v.note);
    }
    update();
  }

  function init() {
    const specs = [
      ["viz-encryption-overview", renderEncryptionOverview],
      ["viz-encryption-contrast", renderEncryptionContrast],
      ["viz-crc32-vs-sha", renderCrc32VsSha],
      ["viz-hex-roundtrip", renderHexRoundtrip],
      ["viz-mask-classes", renderMaskClasses],
      ["viz-md5-fit", renderMd5Fit],
      ["viz-sha-family", renderShaFamily],
    ];
    specs.forEach(([id, fn]) => {
      const elm = document.getElementById(id);
      if (elm && !elm.dataset.rendered) { fn(elm); elm.dataset.rendered = "1"; }
    });
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(() => requestAnimationFrame(init));
  } else {
    document.addEventListener("DOMContentLoaded", init);
  }
})();
