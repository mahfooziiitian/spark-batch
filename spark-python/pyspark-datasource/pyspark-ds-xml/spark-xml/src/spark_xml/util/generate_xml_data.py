import csv
import os
import random
import xml.etree.ElementTree as ET
from pathlib import Path

from faker import Faker

fake = Faker()


def remove_empty_elements(element):
    if element is None:
        return
    for child in list(element):
        remove_empty_elements(child)
        if not (child.text or "").strip() and not len(child) and not len(child.attrib):
            element.remove(child)


def load_mapping(file_path):
    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter="|")
        return list(reader)


def generate_value(data_type, allowed_values):
    if allowed_values:
        return random.choice(allowed_values.split(","))
    elif data_type == "int":
        return str(random.randint(18, 99))
    elif data_type == "string":
        return fake.word()
    else:
        return ""


def generate_value(data_type, allowed_values):
    if allowed_values:
        return random.choice(allowed_values.split(","))
    elif data_type == "int":
        return str(random.randint(18, 99))
    elif data_type == "string":
        return fake.word()
    else:
        return ""


def build_single_element(mapping, main_tag):
    """
    Build one element with all attributes and sub-elements based on mapping filtered by tag.
    """
    main_tag_element = ET.Element(main_tag)
    current_element = None
    current_tag = None

    for row in mapping:
        tag = row["tag"]
        attr = row["attribute"]
        attr_type = row["attribute_type"]
        attr_allowed = row.get("attribute_allowed_values", "")
        data_type = row["data_type"]
        allowed = row["allowed_values"]

        # Create new element if tag changes
        if tag != current_tag:
            current_element = ET.SubElement(main_tag_element, tag)
            current_tag = tag

        # Set attribute if present
        if attr:
            val = generate_value(attr_type, attr_allowed)
            current_element.set(attr, val)
        # Otherwise set text if no attribute and data_type exists
        elif data_type:
            val = generate_value(data_type, allowed)
            current_element.text = val

    return main_tag_element


def build_xml(mapping, main_tag, count=1000):
    root = ET.Element("root")

    for _ in range(count):
        element = build_single_element(mapping, main_tag)
        root.append(element)
    remove_empty_elements(root)
    return ET.ElementTree(root)


def main():
    # Example usage
    data_home = os.environ.get("DATA_HOME", ".")
    xml_file = os.path.join(data_home, "file_data", "xml", "person.xml")
    mapping_file = Path(__file__).parents[0] / "mapping.csv"
    mapping = load_mapping(mapping_file)
    xml_tree = build_xml(mapping, main_tag="person", count=1000)
    xml_tree.write(xml_file, encoding="utf-8", xml_declaration=True)
    print(f"✅ XML generated as {xml_file}")


if __name__ == "__main__":
    main()
