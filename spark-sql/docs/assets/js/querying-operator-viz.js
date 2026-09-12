/**
 * querying-operator-viz.js
 * D3 v7 interactive visualizations for docs/querying/operator/*.md.
 * Compatible with MkDocs Material instant navigation (document$ observable).
 */
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

  function resultStyle(result) {
    if (result.includes("ERROR")) return { bg: "rgba(239,83,80,0.12)", fg: RED, border: RED };
    if (result === "NULL") return { bg: "rgba(255,167,38,0.18)", fg: AMBER, border: AMBER };
    return { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR };
  }

  function renderOverview(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const specs = {
      arithmetic: {
        color: RED,
        summary: "ANSI mode is on by default in Spark 4.2, so overflow and divide-by-zero raise unless you disable ANSI or use try_* helpers.",
        examples: ["2147483647 + 1 -> ARITHMETIC_OVERFLOW", "1 / 0 -> DIVIDE_BY_ZERO", "1 DIV 2 -> BIGINT"]
      },
      comparison: {
        color: PURPLE,
        summary: "Standard comparisons are three-valued: NULL operands usually produce NULL, while <=> and IS [NOT] DISTINCT FROM stay boolean.",
        examples: ["NULL = NULL -> NULL", "NULL <=> NULL -> TRUE", "'Abc' ILIKE 'a%' -> TRUE"]
      },
      logical: {
        color: TEAL,
        summary: "NOT binds before AND, and AND binds before OR. WHERE still keeps only rows whose final predicate is TRUE.",
        examples: ["TRUE OR FALSE AND FALSE -> TRUE", "NOT TRUE AND FALSE -> FALSE", "FALSE AND NULL -> FALSE"]
      },
      string: {
        color: AMBER,
        summary: "Spark supports ||, but both || and CONCAT() return NULL if any operand is NULL. CONCAT_WS() skips NULLs.",
        examples: ["'a' || NULL -> NULL", "CONCAT('a', NULL) -> NULL", "CONCAT_WS('-', 'a', NULL, 'b') -> a-b"]
      },
      set: {
        color: TEAL,
        summary: "UNION / INTERSECT / EXCEPT deduplicate; the ALL variants preserve duplicate counts and treat matching NULL rows as equal.",
        examples: ["UNION removes duplicates", "INTERSECT ALL keeps multiplicity", "EXCEPT ALL subtracts multiplicity"]
      },
      nullsafe: {
        color: PURPLE,
        summary: "Use IS NULL for null checks, <=> or IS NOT DISTINCT FROM for null-safe equality, and IS DISTINCT FROM for null-safe inequality.",
        examples: ["NULL IS NULL -> TRUE", "NULL <=> NULL -> TRUE", "1 IS DISTINCT FROM NULL -> TRUE"]
      },
      bitwise: {
        color: FG,
        summary: "Bitwise operators are for integral types only; decimal and floating-point operands fail analysis.",
        examples: ["12 & 10 -> 8", "12 ^ 10 -> 6", "5.0 & 1 -> DATATYPE_MISMATCH"]
      }
    };
    const stage = container.append("div");
    const group = btnGroup(container, [
      ["arithmetic", "Arithmetic"],
      ["comparison", "Comparison"],
      ["logical", "Logical"],
      ["string", "String"],
      ["set", "Set"],
      ["nullsafe", "Null-Safe"],
      ["bitwise", "Bitwise"]
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      chip(row, key.toUpperCase(), { bg: spec.color, fg: "#fff", border: spec.color });
      spec.examples.forEach((item) => chip(row, item, { bg: "#f5f5f7", fg: FG, border: "#ddd" }));
      stage.append("div").style("font-size", "0.75rem").style("color", FG).style("line-height", "1.5").text(spec.summary);
    }
    render(group.get());
  }

  function renderArithmeticAnsi(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const cases = {
      overflowInt: {
        expr: "2147483647 + 1",
        outcomes: {
          true: "ERROR: ARITHMETIC_OVERFLOW",
          false: "-2147483648"
        }
      },
      overflowBigint: {
        expr: "CAST(9223372036854775807L AS BIGINT) + 1L",
        outcomes: {
          true: "ERROR: ARITHMETIC_OVERFLOW",
          false: "-9223372036854775808"
        }
      },
      divZero: {
        expr: "1 / 0",
        outcomes: {
          true: "ERROR: DIVIDE_BY_ZERO",
          false: "NULL"
        }
      },
      divInt: {
        expr: "1 DIV 0",
        outcomes: {
          true: "ERROR: DIVIDE_BY_ZERO",
          false: "NULL"
        }
      },
      modZero: {
        expr: "1 % 0",
        outcomes: {
          true: "ERROR: REMAINDER_BY_ZERO",
          false: "NULL"
        }
      }
    };
    let ansi = "true";
    const ansiWrap = container.append("div");
    const exprWrap = container.append("div");
    const stage = container.append("div");
    btnGroup(ansiWrap, [["true", "ANSI true"], ["false", "ANSI false"]], (v) => { ansi = v; render(currentExpr); });
    let currentExpr = "overflowInt";
    btnGroup(exprWrap, [
      ["overflowInt", "INT overflow"],
      ["overflowBigint", "BIGINT overflow"],
      ["divZero", "/ by zero"],
      ["divInt", "DIV by zero"],
      ["modZero", "% by zero"]
    ], (v) => { currentExpr = v; render(v); });

    function render(key) {
      stage.selectAll("*").remove();
      const spec = cases[key];
      box(stage, spec.expr, { bg: "#f5f5f7", border: "#ddd" });
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("align-items", "center").style("margin", "8px 0").style("flex-wrap", "wrap");
      chip(row, `spark.sql.ansi.enabled = ${ansi}`, ansi === "true"
        ? { bg: PURPLE, fg: "#fff", border: PURPLE }
        : { bg: AMBER, fg: "#fff", border: AMBER });
      chip(row, spec.outcomes[ansi], resultStyle(spec.outcomes[ansi]));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
        .text(ansi === "true"
          ? "Verified in PySpark 4.2 default mode: Spark raises instead of wrapping or returning NULL."
          : "Verified in PySpark 4.2 after SET/CONF spark.sql.ansi.enabled = false.");
    }
    render(currentExpr);
  }

  function renderBitwiseMask(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const left = 12;
    const right = 10;
    const specs = {
      and: { expr: "12 & 10", result: 8, bits: "1100 & 1010 = 1000", color: TEAL },
      or: { expr: "12 | 10", result: 14, bits: "1100 | 1010 = 1110", color: PURPLE },
      xor: { expr: "12 ^ 10", result: 6, bits: "1100 ^ 1010 = 0110", color: AMBER },
      not: { expr: "~12", result: -13, bits: "~0000 1100 = ...1111 0011", color: RED }
    };
    const stage = container.append("div");
    const group = btnGroup(container, [["and", "&"], ["or", "|"], ["xor", "^"], ["not", "~"]], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      const head = stage.append("div").style("display", "flex").style("gap", "8px").style("flex-wrap", "wrap").style("margin-bottom", "8px");
      if (key !== "not") {
        chip(head, `left = ${left} (1100)`, { bg: "#f5f5f7", fg: FG, border: "#ddd" });
        chip(head, `right = ${right} (1010)`, { bg: "#f5f5f7", fg: FG, border: "#ddd" });
      } else {
        chip(head, `value = ${left} (1100)`, { bg: "#f5f5f7", fg: FG, border: "#ddd" });
      }
      chip(head, `${spec.expr} -> ${spec.result}`, { bg: spec.color, fg: "#fff", border: spec.color });
      box(stage, spec.bits, { bg: "#f5f5f7", border: "#ddd" });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "8px")
        .text(key === "not"
          ? "Spark 4.2 uses signed two's-complement semantics for bitwise NOT, so ~x equals -(x + 1)."
          : "Verified on integral operands in PySpark 4.2; NULL operands still propagate NULL.");
    }
    render(group.get());
  }

  function renderComparisonNull(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const specs = {
      eq: { expr: "NULL = NULL", result: "NULL" },
      neq: { expr: "NULL <> NULL", result: "NULL" },
      nseq: { expr: "NULL <=> NULL", result: "TRUE" },
      distinct: { expr: "1 IS DISTINCT FROM NULL", result: "TRUE" },
      between: { expr: "7 BETWEEN 10 AND 5", result: "FALSE" },
      ilike: { expr: "'Abc' ILIKE 'a%'", result: "TRUE" }
    };
    const stage = container.append("div");
    const group = btnGroup(container, [
      ["eq", "="],
      ["neq", "<>"],
      ["nseq", "<=>"],
      ["distinct", "IS DISTINCT FROM"],
      ["between", "BETWEEN"],
      ["ilike", "ILIKE"]
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      box(stage, spec.expr, { bg: "#f5f5f7", border: "#ddd" });
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("margin", "8px 0").style("flex-wrap", "wrap");
      chip(row, "PySpark 4.2 result", { bg: PURPLE, fg: "#fff", border: PURPLE });
      chip(row, spec.result, resultStyle(spec.result));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
        .text(key === "nseq"
          ? "Null-safe equality stays boolean, unlike ordinary equality with NULL operands."
          : key === "distinct"
            ? "IS DISTINCT FROM is the readable null-safe inequality form."
            : "Standard comparisons and pattern predicates follow three-valued logic when NULL appears.");
    }
    render(group.get());
  }

  function renderLogicalPrecedence(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const specs = {
      andOr: {
        expr: "TRUE OR FALSE AND FALSE",
        parsed: "TRUE OR (FALSE AND FALSE)",
        result: "TRUE"
      },
      parens: {
        expr: "(TRUE OR FALSE) AND FALSE",
        parsed: "(TRUE OR FALSE) AND FALSE",
        result: "FALSE"
      },
      notAnd: {
        expr: "NOT TRUE AND FALSE",
        parsed: "(NOT TRUE) AND FALSE",
        result: "FALSE"
      },
      shortAnd: {
        expr: "FALSE AND (1 / 0 > 0)",
        parsed: "FALSE AND erroring_expression",
        result: "FALSE"
      },
      shortOr: {
        expr: "TRUE OR (1 / 0 > 0)",
        parsed: "TRUE OR erroring_expression",
        result: "TRUE"
      }
    };
    const stage = container.append("div");
    const group = btnGroup(container, [
      ["andOr", "AND before OR"],
      ["parens", "Parentheses"],
      ["notAnd", "NOT before AND"],
      ["shortAnd", "FALSE AND ..."],
      ["shortOr", "TRUE OR ..."]
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      box(stage, `${spec.expr}\n=> ${spec.parsed}`, { bg: "#f5f5f7", border: "#ddd" });
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("margin", "8px 0").style("flex-wrap", "wrap");
      chip(row, "Verified result", { bg: TEAL, fg: "#fff", border: TEAL });
      chip(row, spec.result, resultStyle(spec.result));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
        .text(key.startsWith("short")
          ? "Spark 4.2 returned the left-determined boolean result in this tested expression, even with ANSI errors enabled."
          : "This confirms Spark SQL precedence rather than relying on visual guesswork.");
    }
    render(group.get());
  }

  function renderNullSafe(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const matrix = {
      eq: {
        label: "=",
        rows: [
          ["5 = 5", "TRUE"],
          ["5 = NULL", "NULL"],
          ["NULL = NULL", "NULL"]
        ]
      },
      nseq: {
        label: "<=>",
        rows: [
          ["5 <=> 5", "TRUE"],
          ["5 <=> NULL", "FALSE"],
          ["NULL <=> NULL", "TRUE"]
        ]
      },
      distinct: {
        label: "IS DISTINCT FROM",
        rows: [
          ["5 IS DISTINCT FROM 5", "FALSE"],
          ["5 IS DISTINCT FROM NULL", "TRUE"],
          ["NULL IS DISTINCT FROM NULL", "FALSE"]
        ]
      },
      notDistinct: {
        label: "IS NOT DISTINCT FROM",
        rows: [
          ["5 IS NOT DISTINCT FROM 5", "TRUE"],
          ["5 IS NOT DISTINCT FROM NULL", "FALSE"],
          ["NULL IS NOT DISTINCT FROM NULL", "TRUE"]
        ]
      }
    };
    const stage = container.append("div");
    const group = btnGroup(container, [
      ["eq", "="],
      ["nseq", "<=>"],
      ["distinct", "IS DISTINCT FROM"],
      ["notDistinct", "IS NOT DISTINCT FROM"]
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = matrix[key];
      spec.rows.forEach(([expr, result]) => {
        const row = stage.append("div").style("display", "flex").style("gap", "8px").style("margin-bottom", "6px").style("flex-wrap", "wrap");
        chip(row, expr, { bg: "#f5f5f7", fg: FG, border: "#ddd" });
        chip(row, result, resultStyle(result));
      });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY)
        .style("margin-top", "6px")
        .text(spec.label === "="
          ? "Ordinary equality becomes unknown as soon as NULL participates."
          : "These null-safe forms stay boolean and are safer for joins and change detection.");
    }
    render(group.get());
  }

  function renderSetMultiset(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const stage = container.append("div");
    const specs = {
      union: { label: "UNION", left: [1, 1], right: [1], result: [1], note: "Deduplicates." },
      unionAll: { label: "UNION ALL", left: [1, 1], right: [1], result: [1, 1, 1], note: "Preserves duplicates." },
      intersect: { label: "INTERSECT", left: [1, 1, 2], right: [1, 1, 3], result: [1], note: "Returns common distinct rows." },
      intersectAll: { label: "INTERSECT ALL", left: [1, 1, 2], right: [1, 1, 1, 3], result: [1, 1], note: "Keeps the minimum duplicate count." },
      except: { label: "EXCEPT", left: [1, 1, 2], right: [1, 3], result: [2], note: "Subtracts distinct matches." },
      exceptAll: { label: "EXCEPT ALL", left: [1, 1, 1, 2], right: [1, 3], result: [1, 1, 2], note: "Subtracts multiplicity, not just membership." }
    };
    const group = btnGroup(container, [
      ["union", "UNION"],
      ["unionAll", "UNION ALL"],
      ["intersect", "INTERSECT"],
      ["intersectAll", "INTERSECT ALL"],
      ["except", "EXCEPT"],
      ["exceptAll", "EXCEPT ALL"]
    ], render);

    function addValueRow(parent, label, values, opts) {
      const row = parent.append("div").style("display", "flex").style("gap", "6px").style("align-items", "center").style("margin-bottom", "6px").style("flex-wrap", "wrap");
      chip(row, label, { bg: "#f5f5f7", fg: FG, border: "#ddd" });
      values.forEach((v) => chip(row, String(v), opts));
      if (!values.length) chip(row, "(empty)", { bg: "#f5f5f7", fg: GRAY, border: "#ddd" });
    }

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      addValueRow(stage, "left", spec.left, { bg: "rgba(124,77,255,0.1)", fg: PURPLE, border: PURPLE });
      addValueRow(stage, "right", spec.right, { bg: "rgba(255,167,38,0.15)", fg: AMBER, border: AMBER });
      addValueRow(stage, spec.label, spec.result, { bg: "rgba(38,166,154,0.12)", fg: HIT_COLOR, border: HIT_COLOR });
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).style("margin-top", "6px").text(spec.note);
    }
    render(group.get());
  }

  function renderStringNull(el) {
    const container = d3.select(el);
    container.selectAll("*").remove();
    const specs = {
      pipe: {
        expr: "'a' || NULL",
        result: "NULL",
        note: "Spark's concatenation operator is null-propagating."
      },
      concat: {
        expr: "CONCAT('a', NULL)",
        result: "NULL",
        note: "CONCAT() matches || here; it does not treat NULL as an empty string."
      },
      concatWs: {
        expr: "CONCAT_WS('-', 'a', NULL, 'b')",
        result: "a-b",
        note: "CONCAT_WS() skips NULL arguments instead of nulling the whole result."
      },
      ilike: {
        expr: "'Abc' ILIKE 'a%'",
        result: "TRUE",
        note: "ILIKE is case-insensitive in Spark 4.2."
      },
      rlike: {
        expr: "'abc' RLIKE '^[a-z]+$'",
        result: "TRUE",
        note: "RLIKE uses Java regular-expression syntax."
      }
    };
    const stage = container.append("div");
    const group = btnGroup(container, [
      ["pipe", "||"],
      ["concat", "CONCAT"],
      ["concatWs", "CONCAT_WS"],
      ["ilike", "ILIKE"],
      ["rlike", "RLIKE"]
    ], render);

    function render(key) {
      stage.selectAll("*").remove();
      const spec = specs[key];
      box(stage, spec.expr, { bg: "#f5f5f7", border: "#ddd" });
      const row = stage.append("div").style("display", "flex").style("gap", "8px").style("margin", "8px 0").style("flex-wrap", "wrap");
      chip(row, "PySpark 4.2 result", { bg: AMBER, fg: "#fff", border: AMBER });
      chip(row, spec.result, resultStyle(spec.result));
      stage.append("div").style("font-size", "0.72rem").style("color", GRAY).text(spec.note);
    }
    render(group.get());
  }

  /* ── Bootstrap ───────────────────────────────────────────────────── */
  function init() {
    const specs = [
      ["viz-operator-overview", renderOverview],
      ["viz-operator-arithmetic-ansi", renderArithmeticAnsi],
      ["viz-operator-bitwise-mask", renderBitwiseMask],
      ["viz-operator-comparison-null", renderComparisonNull],
      ["viz-operator-logical-precedence", renderLogicalPrecedence],
      ["viz-operator-null-safe", renderNullSafe],
      ["viz-operator-set-multiset", renderSetMultiset],
      ["viz-operator-string-null", renderStringNull],
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
