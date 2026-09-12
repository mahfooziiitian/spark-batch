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
      .style("display", "inline-flex").style("align-items", "center")
      .style("padding", "5px 12px").style("border-radius", "5px")
      .style("font-size", "0.74rem").style("font-family", "monospace")
      .style("background", opts.bg || "#f5f5f7")
      .style("color", opts.fg || FG)
      .style("border", `1.5px solid ${opts.border || "#ddd"}`)
      .text(text);
  }

  function btnGroup(container, options, onSelect) {
    const ctrl = container.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "0.7rem").style("flex-wrap", "wrap");
    let current = options[0][0];
    function refresh() {
      ctrl.selectAll("button").each(function () {
        const active = d3.select(this).attr("data-v") === current;
        d3.select(this).style("background", active ? PURPLE : "#fff").style("color", active ? "#fff" : FG)
          .style("border", `1px solid ${active ? PURPLE : "#ccc"}`);
      });
    }
    options.forEach(([v, label]) => {
      ctrl.append("button").attr("data-v", v)
        .style("padding", "5px 14px").style("border-radius", "5px")
        .style("cursor", "pointer").style("font-size", "0.76rem")
        .text(label)
        .on("click", () => { current = v; refresh(); onSelect(v); });
    });
    refresh();
    return { get: () => current };
  }

  function box(parent, text, opts) {
    opts = opts || {};
    return parent.append("pre")
      .style("margin", "0").style("padding", "8px 10px")
      .style("background", opts.bg || "#f5f5f7")
      .style("border", `1px solid ${opts.border || "#ddd"}`)
      .style("border-radius", "5px")
      .style("font-size", "0.72rem").style("font-family", "monospace")
      .style("white-space", "pre-wrap").style("color", opts.fg || FG)
      .text(text);
  }

  function stageCard(parent, label, lines, color) {
    const card = parent.append("div")
      .style("flex", "1 1 130px")
      .style("min-width", "130px")
      .style("border", `1.5px solid ${color}`)
      .style("border-radius", "8px")
      .style("padding", "10px")
      .style("background", "rgba(255,255,255,0.9)");
    card.append("div")
      .style("font-size", "0.78rem")
      .style("font-weight", "700")
      .style("color", color)
      .style("margin-bottom", "6px")
      .text(label);
    lines.forEach((line) => {
      card.append("div")
        .style("font-size", "0.72rem")
        .style("color", FG)
        .style("font-family", "monospace")
        .text(line);
    });
    return card;
  }

  function arrow(parent) {
    parent.append("div")
      .style("align-self", "center")
      .style("color", GRAY)
      .style("font-size", "1rem")
      .style("padding", "0 2px")
      .text("→");
  }

  // ---- index.md: script building-block pipeline ----
  function renderOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const traces = {
      pending: {
        steps: [
          ["DECLARE", ["total_rows BIGINT := 0"], GRAY],
          ["SET", ["total_rows := 3", "(3 pending orders)"], PURPLE],
          ["IF", ["total_rows > 0 -> TRUE"], AMBER],
          ["INSERT", ["audit_log row written"], HIT_COLOR],
        ],
        note: "When pending orders exist, the IF branch runs and the block writes an audit row."
      },
      empty: {
        steps: [
          ["DECLARE", ["total_rows BIGINT := 0"], GRAY],
          ["SET", ["total_rows := 0", "(no pending orders)"], PURPLE],
          ["IF", ["total_rows > 0 -> FALSE"], AMBER],
          ["END IF", ["block ends, no INSERT"], RED],
        ],
        note: "When the condition is false, Spark skips straight past END IF — no audit row is written."
      }
    };
    const group = btnGroup(container, [["pending", "Pending orders exist"], ["empty", "No pending orders"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("align-items", "stretch").style("margin-bottom", "8px");
      traces[mode].steps.forEach((spec, idx) => {
        stageCard(row, spec[0], spec[1], spec[2]);
        if (idx < traces[mode].steps.length - 1) arrow(row);
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(traces[mode].note);
    }
    render(group.get());
  }

  // ---- control.md: IF / ELSEIF / ELSE branch explorer ----
  function renderControl(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const branches = {
      full: {
        label: "load_mode = 'full'",
        taken: ["IF load_mode = 'full' THEN", "TRUNCATE TABLE dim_customer", "INSERT INTO dim_customer SELECT * FROM staging_customer"],
        skipped: ["ELSEIF load_mode = 'incremental'", "ELSE"]
      },
      incremental: {
        label: "load_mode = 'incremental'",
        taken: ["ELSEIF load_mode = 'incremental' THEN", "MERGE INTO dim_customer ..."],
        skipped: ["IF load_mode = 'full'", "ELSE"]
      },
      other: {
        label: "load_mode = 'unexpected'",
        taken: ["ELSE", "SIGNAL SQLSTATE '45000' ...", "'Unknown load_mode'"],
        skipped: ["IF load_mode = 'full'", "ELSEIF load_mode = 'incremental'"]
      }
    };
    const group = btnGroup(container, [["full", "full"], ["incremental", "incremental"], ["other", "anything else"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, branches[mode].label, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "12px").style("flex-wrap", "wrap");
      stageCard(row, "Branch taken", branches[mode].taken, HIT_COLOR);
      stageCard(row, "Branches skipped", branches[mode].skipped, GRAY);
      stage.append("div").style("margin-top", "8px").style("font-size", "0.72rem").style("color", GRAY)
        .text("Spark evaluates IF, then each ELSEIF in order, and stops at the first TRUE branch — only one branch ever executes.");
    }
    render(group.get());
  }

  // ---- loops.md: loop construct comparison with iteration trace ----
  function renderLoops(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const loops = {
      while: {
        title: "WHILE i < 3 DO ... END WHILE",
        trace: ["check i<3 (0<3) -> run", "check i<3 (1<3) -> run", "check i<3 (2<3) -> run", "check i<3 (3<3) -> stop"],
        note: "Condition is checked before every iteration, including the first."
      },
      repeat: {
        title: "REPEAT ... UNTIL i >= 3 END REPEAT",
        trace: ["run body (i=0)", "check i>=3 -> false, run again", "check i>=3 -> false, run again", "check i>=3 -> true, stop"],
        note: "The body always runs at least once — the condition is only checked afterward."
      },
      loop: {
        title: "label: LOOP ... LEAVE label ... END LOOP",
        trace: ["run body", "run body", "IF exit_condition THEN LEAVE label", "loop exits"],
        note: "LOOP has no built-in condition; without a LEAVE, it never terminates."
      },
      for: {
        title: "FOR row_var IN (SELECT ...) DO ... END FOR",
        trace: ["fetch row 1 -> run body", "fetch row 2 -> run body", "fetch row 3 -> run body", "no more rows -> stop"],
        note: "FOR iterates once per row of the query result — no manual counter needed."
      }
    };
    const group = btnGroup(container, [["while", "WHILE"], ["repeat", "REPEAT"], ["loop", "LOOP"], ["for", "FOR"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, loops[mode].title, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }).style("margin-bottom", "8px");
      const list = stage.append("div").style("display", "flex").style("flex-direction", "column").style("gap", "4px").style("margin-bottom", "8px");
      loops[mode].trace.forEach((line, idx) => {
        const isLast = idx === loops[mode].trace.length - 1;
        list.append("div")
          .style("font-size", "0.72rem").style("font-family", "monospace")
          .style("padding", "4px 8px").style("border-radius", "4px")
          .style("background", isLast ? "rgba(239,83,80,0.10)" : "rgba(38,166,154,0.08)")
          .style("color", isLast ? RED : FG)
          .text(`${idx + 1}. ${line}`);
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(loops[mode].note);
    }
    render(group.get());
  }

  // ---- variables.md: block-scope shadowing ----
  function renderVariables(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const points = {
      declare_outer: {
        label: "Outer BEGIN: DECLARE x = 'outer'",
        scopeStack: [["outer block", "x = 'outer'", PURPLE]],
        selectResult: null
      },
      enter_inner: {
        label: "Inner BEGIN: DECLARE x = 'inner'",
        scopeStack: [["outer block", "x = 'outer' (shadowed)", GRAY], ["inner block", "x = 'inner'", TEAL]],
        selectResult: "SELECT x  -->  'inner'"
      },
      exit_inner: {
        label: "Inner block ends (END;)",
        scopeStack: [["outer block", "x = 'outer'", PURPLE]],
        selectResult: "SELECT x  -->  'outer'"
      }
    };
    const group = btnGroup(container, [["declare_outer", "1. Declare outer"], ["enter_inner", "2. Enter inner block"], ["exit_inner", "3. Inner block ends"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, points[mode].label, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      points[mode].scopeStack.forEach(([label, val, color]) => stageCard(row, label, [val], color));
      if (points[mode].selectResult) {
        box(stage, points[mode].selectResult, { bg: "#faf8ff", border: TEAL });
      }
      stage.append("div").style("margin-top", "8px").style("font-size", "0.72rem").style("color", GRAY)
        .text("A nested block's DECLARE of the same name shadows the outer variable only until its own END is reached.");
    }
    render(group.get());
  }

  // ---- exceptions.md: CONTINUE vs EXIT handler flow ----
  function renderExceptions(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const flows = {
      continue: {
        title: "DECLARE CONTINUE HANDLER FOR SQLEXCEPTION",
        steps: [
          ["Statement 1", "OK"],
          ["Statement 2", "raises error"],
          ["Handler runs", "skip_count += 1"],
          ["Statement 3", "resumes normally"],
        ],
        note: "Execution resumes at the statement AFTER the one that failed — the block keeps running."
      },
      exit: {
        title: "DECLARE EXIT HANDLER FOR SQLEXCEPTION",
        steps: [
          ["Statement 1", "OK"],
          ["Statement 2", "raises error"],
          ["Handler runs", "log to error_log"],
          ["Statement 3", "never runs"],
        ],
        note: "Execution leaves the enclosing BEGIN … END block immediately after the handler body finishes."
      }
    };
    const group = btnGroup(container, [["continue", "CONTINUE handler"], ["exit", "EXIT handler"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      chip(stage, flows[mode].title, { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      flows[mode].steps.forEach(([label, status], idx) => {
        const skipped = mode === "exit" && idx === flows[mode].steps.length - 1;
        stageCard(row, label, [status], skipped ? RED : (status.includes("raises") ? AMBER : HIT_COLOR));
        if (idx < flows[mode].steps.length - 1) arrow(row);
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(flows[mode].note);
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-scripting-overview", renderOverview],
      ["viz-scripting-control", renderControl],
      ["viz-scripting-loops", renderLoops],
      ["viz-scripting-variables", renderVariables],
      ["viz-scripting-exceptions", renderExceptions],
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
