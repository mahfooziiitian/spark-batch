import os
from collections import defaultdict

from sqlalchemy import create_engine, inspect

# ---------- CONFIG ---------- #
token = os.environ.get("M_DBX_TOKEN")
host = os.environ.get("M_DBX_HOST").split("://")[1]
http_path = os.environ.get("M_DBX_HTTP_PATH")
catalog = os.environ.get("M_DBX_CATALOG", "system")
schema = os.environ.get("M_DBX_SCHEMA", "billing")
OUTPUT_FILE = "inferred_erd.gv"
DATABRICKS_URI = f"databricks://token:{token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"
RENDER_ERD = True
CLEANUP = True
# ---------------------------- #


def create_db_engine(uri: str):
    return create_engine(uri)


def get_table_columns(engine):
    inspector = inspect(engine)
    table_columns = defaultdict(list)

    for table_name in inspector.get_table_names():
        for column in inspector.get_columns(table_name):
            table_columns[table_name].append((column["name"], str(column["type"])))
    return table_columns


def infer_composite_relationships(table_columns, max_combination_size=3):
    from itertools import combinations

    composite_rels = []

    for table_a, cols_a in table_columns.items():
        for table_b, cols_b in table_columns.items():
            if table_a == table_b:
                continue

            col_names_a = {name: dtype for name, dtype in cols_a}
            col_names_b = {name: dtype for name, dtype in cols_b}

            for k in range(2, max_combination_size + 1):
                for combo in combinations(col_names_a.keys(), k):
                    if all(
                        col in col_names_b and col_names_a[col] == col_names_b[col]
                        for col in combo
                    ):
                        composite_rels.append((table_a, table_b, list(combo)))
    return composite_rels


def infer_one_way_relationships(table_columns):
    relationships = []

    for src_table, src_cols in table_columns.items():
        for tgt_table, tgt_cols in table_columns.items():
            if src_table == tgt_table:
                continue

            tgt_colnames = {col for col, _ in tgt_cols}
            print(f"tgt_colnames: {tgt_colnames}")

            for col_name, _ in src_cols:
                if (
                    col_name.endswith("_name")
                    or col_name.endswith("_id")
                    or col_name.startswith("parent_")
                    or col_name in ("created_by", "updated_by", "owner")
                ):
                    ref = (
                        col_name.replace("_name", "")
                        .replace("_id", "")
                        .replace("parent_", "")
                    )
                    if ref.endswith("y"):
                        ref = ref[:-1] + "ies"
                    elif not ref.endswith("s"):
                        ref += "s"

                    if ref.lower() in tgt_table.lower():
                        relationships.append((src_table, col_name, tgt_table))
    return relationships


def visualize_erd_compact(table_columns, relationships, output_file, cleanup=True):
    import os

    from graphviz import Digraph

    dot = Digraph("Compact_ERD", format="png")
    dot.attr(
        rankdir="LR",
        fontsize="10",
        fontname="Arial",
        splines="true",
        overlap="false",
        nodesep="0.3",
    )

    # Add only table names (no column details)
    for table in table_columns.keys():
        dot.node(table, shape="box", style="filled", fillcolor="lightgrey")

    # Add compact edges (with or without labels)
    for rel in relationships:
        if len(rel) == 4:
            src, src_col, tgt, tgt_col = rel
            label = f"{src_col}→{tgt_col}"
        elif len(rel) == 3:
            src, tgt, cols = (
                rel if isinstance(rel[2], list) else (rel[0], rel[2], [rel[1]])
            )
            label = ", ".join(cols)
        else:
            continue

        # Optional: skip label to reduce clutter
        dot.edge(src, tgt, label=label, fontsize="8")

    output_path = dot.render(output_file, view=True)

    if cleanup:
        base, _ = os.path.splitext(output_path)
        for ext in [".gv", ".gv.pdf", ".gv.svg"]:
            try:
                os.remove(base + ext)
            except FileNotFoundError:
                pass


def normalize_relation(r):
    return tuple(r[:2]) + (tuple(r[2]) if isinstance(r[2], list) else r[2:])


def main():
    engine = create_db_engine(DATABRICKS_URI)
    table_columns = get_table_columns(engine)

    composite_rels = infer_composite_relationships(table_columns)
    one_way_rels = infer_one_way_relationships(table_columns)

    all_relationships = list(
        {normalize_relation(r) for r in composite_rels + one_way_rels}
    )

    print(f"🔗 Total Relationships Inferred: {len(all_relationships)}")
    for rel in all_relationships:
        if len(rel) == 4:
            print(f"{rel[0]}.{rel[1]} → {rel[2]}.{rel[3]}")
        elif len(rel) == 3:
            cols = rel[2] if isinstance(rel[2], list) else [rel[1]]
            print(
                f"{rel[0]}({', '.join(cols)}) → {rel[1] if isinstance(rel[2], list) else rel[2]}"
            )

    if RENDER_ERD:
        visualize_erd_compact(
            table_columns, all_relationships, OUTPUT_FILE, cleanup=CLEANUP
        )


if __name__ == "__main__":
    main()
