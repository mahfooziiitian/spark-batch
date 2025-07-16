import os
import random
from datetime import date, timedelta

from lxml import etree as ET
from xmlschema import XMLSchema


# --- Data Generation Functions ---
def generate_random_value(data_type_name):
    """
    Generates a random value based on the XSD data type name.
    """
    if data_type_name in ["xs:string", "xs:normalizedString"]:
        return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=10))
    elif data_type_name in ["xs:integer", "xs:int"]:
        return str(random.randint(1, 100))
    elif data_type_name == "xs:decimal":
        return f"{random.uniform(1.0, 1000.0):.2f}"
    elif data_type_name == "xs:date":
        start_date = date(2024, 1, 1)
        end_date = date(2025, 12, 31)
        random_days = random.randint(0, (end_date - start_date).days)
        return str(start_date + timedelta(days=random_days))
    return ""


def generate_element_data(xsd_element):
    """
    Recursively generates an lxml.etree.Element based on the XsdElement schema definition.
    """
    # Create the lxml.etree.Element using the name from the schema element.
    # The 'xsd_element' is an object from the 'xmlschema' library.
    elem = ET.Element(xsd_element.name)

    # Check the type of the element from the schema
    # Use 'xsd_element.type.is_simple()' to check if it's a simple type
    if xsd_element.type.is_simple():
        # Get the simple type name and generate a random value
        elem.text = generate_random_value(xsd_element.type.name)

    # Check if it's a complex type with child elements
    elif xsd_element.type.is_complex():
        # The content_type holds the definition of the children
        content = xsd_element.type.content_type
        if content:
            for child_xsd_element in content.elements:
                # Handle occurrence constraints (minOccurs, maxOccurs)
                min_occurs = child_xsd_element.min_occurs
                # Set a reasonable upper bound for 'unbounded'
                max_occurs = (
                    child_xsd_element.max_occurs
                    if child_xsd_element.max_occurs != "unbounded"
                    else 3
                )

                num_occurrences = random.randint(min_occurs, max_occurs)

                for _ in range(num_occurrences):
                    # Recursively call to generate the child element
                    child_elem = generate_element_data(child_xsd_element)
                    elem.append(child_elem)

    return elem


# --- Main Script ---
def main():
    """
    Parses the XSD, generates XML data, and saves it to a file.
    """
    data_home = os.environ.get("DATA_HOME", ".")
    output_file = os.path.join(data_home, "file_data", "xml", "orders.xml")
    xsd_file = os.path.join(data_home, "file_data", "xml", "orders.xsd")
    num_orders = 1000

    try:
        # 1. Parse the XSD schema using xmlschema.XMLSchema
        schema = XMLSchema(xsd_file)
        print("Schema loaded successfully.")

        # 2. Get the root element definition from the schema
        root_element_schema = schema.root

        # Create the root element for the XML file using the name from the schema
        root_element_name = root_element_schema.name
        xml_root = ET.Element(root_element_name)

        # 3. Find the 'Order' element definition within the root's content type
        order_element_schema = next(
            e
            for e in root_element_schema.type.content_type.elements
            if e.name == "Order"
        )

        # 4. Generate the specified number of 'Order' elements
        print(f"Generating {num_orders} orders...")
        for i in range(num_orders):
            # Use the recursive function to generate a complete <Order> tree
            order_elem = generate_element_data(order_element_schema)

            # --- Domain-specific modifications (not from the schema) ---
            # Set a more user-friendly OrderID and CustomerName
            order_elem.find("OrderID").text = f"ORD-{i + 1:04d}"
            order_elem.find("CustomerName").text = f"Customer_{i + 1:04d}"

            # Dynamically calculate and set TotalAmount based on generated item values
            total = 0.0
            items_element = order_elem.find("Items")
            if items_element:
                for item in items_element.findall("Item"):
                    quantity = float(item.find("Quantity").text)
                    unit_price = float(item.find("UnitPrice").text)
                    total += quantity * unit_price

            # Find the TotalAmount element and set its text
            total_amount_elem = order_elem.find("TotalAmount")
            if total_amount_elem is not None:
                total_amount_elem.text = f"{total:.2f}"

            xml_root.append(order_elem)

        # 5. Create ElementTree and save with pretty formatting
        tree = ET.ElementTree(xml_root)
        tree.write(
            output_file, pretty_print=True, xml_declaration=True, encoding="utf-8"
        )

        print(
            f"Successfully generated {num_orders} orders and saved to '{output_file}'."
        )

        # 6. Validate the generated XML against the XSD
        schema.validate(output_file)
        print("Validation successful: The generated XML matches the XSD schema.")

    except FileNotFoundError:
        print(f"Error: The file '{xsd_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
