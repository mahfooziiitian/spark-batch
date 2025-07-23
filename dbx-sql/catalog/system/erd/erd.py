import os
from sqlalchemy import create_engine, inspect
from collections import defaultdict
from graphviz import Digraph

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
    """Create SQLAlchemy engine for Databricks."""
    return create_engine(uri)


def get_table_columns(engine):
    """Introspect tables and collect column names and types."""
    inspector = inspect(engine)
    table_columns = defaultdict(list)

    for table_name in inspector.get_table_names():
        for column in inspector.get_columns(table_name):
            table_columns[table_name].append((column['name'], str(column['type'])))
    return table_columns

def infer_composite_relationships(table_columns, max_combination_size=3):
    """Infer multi-column relationships (composite foreign keys)."""
    from itertools import combinations

    composite_rels = []

    for table_a, cols_a in table_columns.items():
        for table_b, cols_b in table_columns.items():
            if table_a == table_b:
                continue

            col_names_a = {name: dtype for name, dtype in cols_a}
            col_names_b = {name: dtype for name, dtype in cols_b}

            # Try all column combinations from size 2 to max
            for k in range(2, max_combination_size + 1):
                for combo in combinations(col_names_a.keys(), k):
                    if all(col in col_names_b and col_names_a[col] == col_names_b[col] for col in combo):
                        composite_rels.append((table_a, table_b, list(combo)))
    return composite_rels


def infer_relationships(table_columns):
    """Infer possible relationships by column name and type matching."""
    relationships = []

    for table_a, cols_a in table_columns.items():
        for table_b, cols_b in table_columns.items():
            if table_a == table_b:
                continue

            for col_a, type_a in cols_a:
                for col_b, type_b in cols_b:
                    if col_a == col_b and type_a == type_b:
                        relationships.append((table_a, col_a, table_b, col_b))
                    elif col_a.endswith("_id") and col_b.endswith("_id") and col_a == col_b and type_a == type_b:
                        relationships.append((table_a, col_a, table_b, col_b))
    return relationships


def visualize_erd(table_columns, relationships, output_file, cleanup=True):
    """Render the inferred ERD using Graphviz and optionally clean temp files."""
    from graphviz import Digraph
    import os

    dot = Digraph("Inferred ERD", format="png")

    # Add tables and columns
    for table, columns in table_columns.items():
        label = f"<<TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0'>"
        label += f"<TR><TD BGCOLOR='lightgray'><B>{table}</B></TD></TR>"
        for col, _ in columns:
            label += f"<TR><TD>{col}</TD></TR>"
        label += "</TABLE>>"
        dot.node(table, label=label, shape="plain")

    # Add relationships (support both simple and composite)
    for rel in relationships:
        if len(rel) == 4:
            src_table, src_col, tgt_table, tgt_col = rel
            label = f"{src_col} → {tgt_col}"
        elif len(rel) == 3:
            src_table, tgt_table, cols = rel
            label = " + ".join(cols)
        else:
            continue  # Skip unknown format

        dot.edge(src_table, tgt_table, label=label)

    output_path = dot.render(output_file, view=True)

    # Cleanup generated .gv, .gv.png, etc.
    if cleanup:
        base, _ = os.path.splitext(output_path)
        for ext in [".gv", ".gv.pdf", ".gv.png", ".gv.svg"]:
            try:
                os.remove(base + ext)
            except FileNotFoundError:
                continue


def main():
    engine = create_db_engine(DATABRICKS_URI)
    table_columns = get_table_columns(engine)
    composite_relationships = infer_composite_relationships(table_columns)

    print(f"\n🔗 Found {len(composite_relationships)} composite relationships:\n")
    for t1, t2, cols in composite_relationships:
        print(f"  {t1}({', '.join(cols)}) → {t2}({', '.join(cols)})")

    if RENDER_ERD:
        visualize_erd(table_columns, composite_relationships, OUTPUT_FILE, cleanup=CLEANUP)


if __name__ == "__main__":
    main()
