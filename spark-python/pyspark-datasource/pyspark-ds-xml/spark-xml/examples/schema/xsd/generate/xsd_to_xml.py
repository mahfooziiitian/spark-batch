import os
import random
import xml.etree.ElementTree as ET
from pathlib import Path

import xmlschema

from spark_xml.util.sample_data import ensure_simple_orders_xsd

data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
xml_file = str(data_home / "file_data" / "xml" / "orders_catalog.xml")
xsd_file = str(ensure_simple_orders_xsd(data_home / "file_data" / "xml" / "orders_catalog.xsd"))

# Load the schema (used to validate the generated data against orders_catalog.xsd)
schema = xmlschema.XMLSchema(xsd_file)


def build_order(order_id: int) -> ET.Element:
    """Build a single ``<Order>`` element matching :data:`orders_catalog.xsd`."""
    order = ET.Element("Order")
    ET.SubElement(order, "OrderID").text = f"ORD-{order_id:04d}"
    ET.SubElement(order, "CustomerName").text = f"Customer_{order_id:04d}"

    items = ET.SubElement(order, "Items")
    total = 0.0
    for _ in range(random.randint(1, 3)):
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(1.0, 100.0), 2)
        item = ET.SubElement(items, "Item")
        ET.SubElement(item, "Quantity").text = str(quantity)
        ET.SubElement(item, "UnitPrice").text = f"{unit_price:.2f}"
        total += quantity * unit_price

    ET.SubElement(order, "TotalAmount").text = f"{total:.2f}"
    return order


def generate_1000_orders():
    root = ET.Element("Orders")
    for i in range(1, 1001):
        root.append(build_order(i))

    tree = ET.ElementTree(root)
    Path(xml_file).parent.mkdir(parents=True, exist_ok=True)
    tree.write(xml_file, encoding="utf-8", xml_declaration=True)
    schema.validate(xml_file)
    print(f"Generated 1000 orders to {xml_file}")


def main():
    generate_1000_orders()


if __name__ == "__main__":
    main()
