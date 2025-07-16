import os
import xmlschema
import xml.etree.ElementTree as ET

data_home = os.environ.get("DATA_HOME", ".")
xml_file = os.path.join(data_home, "file_data", "xml", "orders.xml")
xsd_file = os.path.join(data_home, "file_data", "xml", "orders.xsd")

# Load the schema
schema = xmlschema.XMLSchema(xsd_file)

# Generate one sample order using the schema
sample_order = schema.elements["Orders"].type.content[0].type


# Generate 1000 orders based on the sample structure
def generate_1000_orders():
    root = ET.Element("Orders")
    for i in range(1, 1001):
        # Generate a new example for each order
        order_example = schema.elements["Orders"].type.content[0].type
        order_element = schema.encode(order_example, path="Orders/Order")
        root.append(order_element)

    tree = ET.ElementTree(root)
    tree.write(xml_file, encoding="utf-8", xml_declaration=True)
    print("Generated 1000 orders to orders.xml")


def main():
    generate_1000_orders()


if __name__ == "__main__":
    main()
