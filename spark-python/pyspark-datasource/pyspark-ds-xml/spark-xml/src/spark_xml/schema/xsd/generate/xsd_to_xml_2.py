"""Generate XML data from an XSD schema using xmlschema + lxml.

Creates a sample orders XSD, then uses it to generate 1000 Order
elements with realistic random data and calculated TotalAmount fields.
Validates the output against the XSD.
"""

import os
import random
import sys
import textwrap
from datetime import date, timedelta
from pathlib import Path

from lxml import etree as ET
from xmlschema import XMLSchema

os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME_17", os.environ.get("JAVA_HOME", ""))
os.environ["PYSPARK_PYTHON"] = sys.executable

ORDERS_XSD = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">

      <xs:element name="Orders">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="Order" maxOccurs="unbounded">
              <xs:complexType>
                <xs:sequence>
                  <xs:element name="OrderID" type="xs:string"/>
                  <xs:element name="CustomerName" type="xs:string"/>
                  <xs:element name="OrderDate" type="xs:date"/>
                  <xs:element name="Items">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="Item" minOccurs="1" maxOccurs="5">
                          <xs:complexType>
                            <xs:sequence>
                              <xs:element name="ProductName" type="xs:string"/>
                              <xs:element name="Quantity" type="xs:integer"/>
                              <xs:element name="UnitPrice" type="xs:decimal"/>
                            </xs:sequence>
                          </xs:complexType>
                        </xs:element>
                      </xs:sequence>
                    </xs:complexType>
                  </xs:element>
                  <xs:element name="TotalAmount" type="xs:decimal"/>
                </xs:sequence>
              </xs:complexType>
            </xs:element>
          </xs:sequence>
        </xs:complexType>
      </xs:element>

    </xs:schema>
""")


def generate_xsd_file(xsd_path: Path) -> None:
    """Write the orders XSD to *xsd_path*."""
    xsd_path.parent.mkdir(parents=True, exist_ok=True)
    xsd_path.write_text(ORDERS_XSD, encoding="utf-8")
    print(f"Generated XSD → {xsd_path}")


# --- Data Generation Functions ---
def generate_random_value(data_type_name):
    """Generates a random value based on the XSD data type name."""
    # xmlschema returns fully qualified names like {http://...}string
    local_name = data_type_name.split("}")[-1] if "}" in data_type_name else data_type_name
    # Also handle xs: prefix form
    local_name = local_name.split(":")[-1] if ":" in local_name else local_name

    if local_name in ("string", "normalizedString", "token"):
        return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=10))
    elif local_name in ("integer", "int", "long", "short", "positiveInteger"):
        return str(random.randint(1, 100))
    elif local_name in ("decimal", "float", "double"):
        return f"{random.uniform(1.0, 1000.0):.2f}"
    elif local_name == "date":
        start_date = date(2024, 1, 1)
        end_date = date(2025, 12, 31)
        random_days = random.randint(0, (end_date - start_date).days)
        return str(start_date + timedelta(days=random_days))
    elif local_name == "boolean":
        return random.choice(["true", "false"])
    return "unknown"


def generate_element_data(xsd_element):
    """Recursively generates an lxml.etree.Element based on the XsdElement schema definition."""
    elem = ET.Element(xsd_element.name)

    if xsd_element.type.is_simple():
        elem.text = generate_random_value(xsd_element.type.name)
    elif xsd_element.type.is_complex():
        content = xsd_element.type.content
        if content:
            for child_xsd_element in content:
                min_occurs = child_xsd_element.min_occurs
                max_occurs = (
                    child_xsd_element.max_occurs
                    if child_xsd_element.max_occurs != "unbounded"
                    else 3
                )

                num_occurrences = random.randint(min_occurs, max_occurs)

                for _ in range(num_occurrences):
                    child_elem = generate_element_data(child_xsd_element)
                    elem.append(child_elem)

    return elem


# --- Main Script ---
def main():
    """Parses the XSD, generates XML data, and saves it to a file."""
    data_home = os.environ.get("DATA_HOME", ".")
    xsd_path = Path(data_home) / "file_data" / "xml" / "orders.xsd"
    output_path = Path(data_home) / "file_data" / "xml" / "orders.xml"
    num_orders = 1000

    # Generate the XSD file first
    generate_xsd_file(xsd_path)

    try:
        # 1. Parse the XSD schema
        schema = XMLSchema(xsd_path.as_posix())
        print("Schema loaded successfully.")

        # 2. Get the root element definition via schema.elements
        root_element_schema = schema.elements["Orders"]
        root_element_name = root_element_schema.name
        xml_root = ET.Element(root_element_name)

        # 3. Find the 'Order' element definition
        order_element_schema = next(
            e
            for e in root_element_schema.type.content
            if e.name == "Order"
        )

        # 4. Generate the specified number of 'Order' elements
        print(f"Generating {num_orders} orders...")
        for i in range(num_orders):
            order_elem = generate_element_data(order_element_schema)

            # Set user-friendly OrderID and CustomerName
            order_elem.find("OrderID").text = f"ORD-{i + 1:04d}"
            order_elem.find("CustomerName").text = f"Customer_{i + 1:04d}"

            # Calculate TotalAmount from item values
            total = 0.0
            items_element = order_elem.find("Items")
            if items_element is not None:
                for item in items_element.findall("Item"):
                    quantity = float(item.find("Quantity").text)
                    unit_price = float(item.find("UnitPrice").text)
                    total += quantity * unit_price

            total_amount_elem = order_elem.find("TotalAmount")
            if total_amount_elem is not None:
                total_amount_elem.text = f"{total:.2f}"

            xml_root.append(order_elem)

        # 5. Save with pretty formatting
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tree = ET.ElementTree(xml_root)
        tree.write(
            output_path.as_posix(),
            pretty_print=True,
            xml_declaration=True,
            encoding="utf-8",
        )

        print(
            f"Successfully generated {num_orders} orders → {output_path}"
        )

        # 6. Validate the generated XML against the XSD
        schema.validate(output_path.as_posix())
        print("Validation successful: The generated XML matches the XSD schema.")

    except FileNotFoundError:
        print(f"Error: The file '{xsd_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
