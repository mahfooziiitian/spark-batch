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
      .style("flex", "1 1 120px")
      .style("min-width", "120px")
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

  function renderOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      cli: {
        cards: [
          ["User", ["terminal prompt", "types spark-sql"], GRAY],
          ["Session", ["Spark driver starts", "owns its own SparkSession"], PURPLE],
          ["Data path", ["catalog + files", "runs SQL directly"], HIT_COLOR],
        ],
        note: "Use spark-sql when one user wants a fresh session for ad-hoc SQL or batch .sql execution."
      },
      beeline: {
        cards: [
          ["User", ["terminal or BI tool", "types beeline -u ..."], GRAY],
          ["Session", ["remote Thrift Server", "SparkSession lives on server"], AMBER],
          ["Data path", ["shared catalogs", "multi-user JDBC access"], HIT_COLOR],
        ],
        note: "Use Beeline when SQL must cross a network boundary and connect to a shared remote service."
      },
      api: {
        cards: [
          ["User", ["application code", "calls spark.sql(...)"], GRAY],
          ["Session", ["existing Spark app", "shares app configs and temp views"], TEAL],
          ["Data path", ["same catalog", "mixed API + SQL workflow"], HIT_COLOR],
        ],
        note: "Use spark.sql() when SQL is one part of a larger program that also uses DataFrames or procedural code."
      }
    };
    const group = btnGroup(container, [["cli", "spark-sql"], ["beeline", "Beeline"], ["api", "spark.sql()"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("align-items", "stretch").style("margin-bottom", "8px");
      modes[mode].cards.forEach((spec, idx) => {
        stageCard(row, spec[0], spec[1], spec[2]);
        if (idx < modes[mode].cards.length - 1) arrow(row);
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function renderGettingStarted(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    let step = 1;
    const steps = {
      1: {
        title: "1. Launch",
        cards: [["Command", ["spark-sql", "launcher parses options"], PURPLE]],
        note: "Spark is still starting the driver, reading defaults, and discovering catalog settings."
      },
      2: {
        title: "2. Prompt ready",
        cards: [["Prompt", ["Spark SQL>", "session is ready"], TEAL], ["Catalog", ["current database", "startup configs loaded"], AMBER]],
        note: "Once the prompt appears, startup-time configuration and initial catalog access are already in place."
      },
      3: {
        title: "3. Query runs",
        cards: [["Input", ["SHOW TABLES;", "semicolon ends line"], GRAY], ["Execution", ["Spark plans query", "results print to stdout"], HIT_COLOR]],
        note: "Interactive statements execute only after a trailing semicolon terminates the current command."
      },
      4: {
        title: "4. Exit",
        cards: [["Command", ["quit; or exit;", "process ends"], RED], ["Cleanup", ["temp views vanish", "session SET values vanish"], PURPLE]],
        note: "Session-scoped objects disappear when the shell exits because spark-sql owned that SparkSession."
      }
    };

    const ctrl = container.append("div").style("display", "flex").style("align-items", "center").style("gap", "8px").style("margin-bottom", "8px").style("flex-wrap", "wrap");
    ctrl.append("span").style("font-size", "0.74rem").text("Session step");
    const label = ctrl.append("code").style("padding", "2px 8px").style("background", "#eee").style("border-radius", "4px");
    ctrl.append("input").attr("type", "range").attr("min", 1).attr("max", 4).attr("step", 1).attr("value", step).style("width", "180px")
      .on("input", function () { step = +this.value; update(); });
    const stage = container.append("div");

    function update() {
      label.text(steps[step].title);
      stage.selectAll("*").remove();
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      steps[step].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(steps[step].note);
    }
    update();
  }

  function renderOptions(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      interactive: {
        command: "spark-sql --master local[*]",
        cards: [["Input source", ["typed at prompt", "multiple statements"], PURPLE], ["Prompt", ["appears", "waits for quit;"], TEAL], ["Process end", ["user exits manually"], AMBER]],
        note: "Interactive mode is the default when neither -e nor -f is supplied."
      },
      inline: {
        command: "spark-sql -S -e \"SHOW TABLES IN default\"",
        cards: [["Input source", ["one inline string", "no prompt"], PURPLE], ["Prompt", ["skipped"], RED], ["Process end", ["after statement completes"], HIT_COLOR]],
        note: "Use -e for single-shot checks, CI probes, and shell-generated SQL."
      },
      file: {
        command: "spark-sql -f ./sql/daily_report.sql",
        cards: [["Input source", ["SQL file", "semicolon-delimited"], PURPLE], ["Prompt", ["skipped"], RED], ["Process end", ["after file or first error"], HIT_COLOR]],
        note: "Use -f for versioned batch scripts or shared SQL artifacts."
      },
      init: {
        command: "spark-sql --database analytics -i ./sql/init.sql",
        cards: [["Input source", ["init file first", "then interactive prompt"], PURPLE], ["Prompt", ["appears after init"], TEAL], ["Process end", ["user exits manually"], AMBER]],
        note: "Use -i when you want reusable setup before a prompt or before a -f batch file."
      }
    };
    const group = btnGroup(container, [["interactive", "Interactive"], ["inline", "-e inline"], ["file", "-f file"], ["init", "-i init"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, modes[mode].command, { bg: "#faf8ff", border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      modes[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function renderConfiguration(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const configs = {
      defaults: {
        chips: [
          ["built-in = 200", { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY }],
          ["spark-defaults.conf = 256", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
          ["active = 256", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }]
        ],
        note: "Without a command-line override, the startup value from spark-defaults.conf becomes the session value."
      },
      cli: {
        chips: [
          ["built-in = 200", { bg: "rgba(144,164,174,0.12)", fg: FG, border: GRAY }],
          ["spark-defaults.conf = 256", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
          ["--conf = 64", { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }],
          ["active = 64", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }]
        ],
        note: "At startup, --conf overrides file-based defaults for the same key."
      },
      set: {
        chips: [
          ["startup active = 64", { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }],
          ["SET value = 32", { bg: "rgba(124,77,255,0.10)", fg: PURPLE, border: PURPLE }],
          ["current session = 32", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }]
        ],
        note: "A mutable SQL config can be changed again with SET, but only inside the running session."
      },
      immutable: {
        chips: [
          ["startup spark.master = yarn", { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER }],
          ["SET spark.master = local[*]", { bg: "rgba(239,83,80,0.10)", fg: RED, border: RED }],
          ["effective value stays = yarn", { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR }]
        ],
        note: "Deploy-time settings are chosen before the JVM starts, so they are not reliably changeable with SET later."
      }
    };
    const group = btnGroup(container, [["defaults", "Defaults file"], ["cli", "--conf override"], ["set", "Interactive SET"], ["immutable", "Startup-only key"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      const chipRow = stage.append("div").style("display", "flex").style("gap", "6px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      configs[mode].chips.forEach(([text, opts]) => chip(chipRow, text, opts));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(configs[mode].note);
    }
    render(group.get());
  }

  function renderBeeline(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const urls = {
      basic: {
        url: "jdbc:hive2://thrift-host:10000",
        cards: [["Server", ["thrift-host:10000", "remote endpoint"], PURPLE], ["Database", ["server default"], TEAL], ["Transport", ["binary, no extra params"], GRAY]],
        note: "This is the shortest form: connect to the server and accept its default database."
      },
      db: {
        url: "jdbc:hive2://thrift-host:10000/analytics",
        cards: [["Server", ["thrift-host:10000"], PURPLE], ["Database", ["analytics selected", "right after connect"], HIT_COLOR], ["Transport", ["binary"], GRAY]],
        note: "Adding /analytics selects the working database immediately after the connection opens."
      },
      httpSsl: {
        url: "jdbc:hive2://thrift-host:10000/default;transportMode=http;httpPath=cliservice;ssl=true",
        cards: [["Server", ["thrift-host:10000"], PURPLE], ["Database", ["default"], TEAL], ["Parameters", ["HTTP transport", "SSL enabled"], AMBER]],
        note: "Semicolon parameters extend the same base URL with transport and security settings."
      },
      kerberos: {
        url: "jdbc:hive2://thrift-host:10000/default;principal=hive/thrift-host@REALM.COM",
        cards: [["Server", ["thrift-host:10000"], PURPLE], ["Database", ["default"], TEAL], ["Parameters", ["Kerberos principal"], AMBER]],
        note: "Kerberos authentication is expressed as another JDBC URL parameter rather than a separate protocol."
      }
    };
    const group = btnGroup(container, [["basic", "Server only"], ["db", "Pick database"], ["httpSsl", "HTTP + SSL"], ["kerberos", "Kerberos"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, urls[mode].url, { bg: "#faf8ff", border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      urls[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(urls[mode].note);
    }
    render(group.get());
  }

  function renderScripting(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const modes = {
      file: {
        command: "spark-sql -f ./sql/script.sql",
        cards: [["Input", ["file on disk", "semicolon-delimited"], PURPLE], ["Prompt", ["does not appear"], RED], ["Exit status", ["returned after file completes"], HIT_COLOR]],
        note: "Use -f when SQL already exists as a tracked script artifact."
      },
      stdin: {
        command: "printf '%s\n' "SELECT current_date();" | spark-sql -S",
        cards: [["Input", ["stdin stream", "generated by another tool"], PURPLE], ["Prompt", ["does not appear"], RED], ["Exit status", ["returned when stdin closes"], HIT_COLOR]],
        note: "stdin mode is useful when another command emits SQL dynamically."
      },
      interactive: {
        command: "spark-sql",
        cards: [["Input", ["typed by user", "same parser"], PURPLE], ["Prompt", ["appears and waits"], TEAL], ["Exit status", ["returned only after quit;"], AMBER]],
        note: "Interactive mode keeps the session open so the same temp views and SET values can be reused across statements."
      }
    };
    const group = btnGroup(container, [["file", "-f script"], ["stdin", "stdin pipe"], ["interactive", "interactive shell"]], render);

    function render(mode) {
      stage.selectAll("*").remove();
      box(stage, modes[mode].command, { bg: "#faf8ff", border: PURPLE }).style("margin-bottom", "8px");
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      modes[mode].cards.forEach((card) => stageCard(row, card[0], card[1], card[2]));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(modes[mode].note);
    }
    render(group.get());
  }

  function init() {
    const specs = [
      ["viz-cli-overview", renderOverview],
      ["viz-cli-getting-started", renderGettingStarted],
      ["viz-cli-options", renderOptions],
      ["viz-cli-configuration", renderConfiguration],
      ["viz-cli-beeline", renderBeeline],
      ["viz-cli-scripting", renderScripting],
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
