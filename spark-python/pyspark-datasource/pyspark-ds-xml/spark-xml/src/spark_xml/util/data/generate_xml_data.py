"""
CSV-Mapping-Based XML Data Generator

Reads a pipe-delimited CSV mapping file that defines the XML structure
(tags, attributes, data types, allowed values) and generates realistic
sample XML data using Faker.

Mapping CSV format (pipe-delimited):
    tag|attribute|attribute_type|attribute_allowed_values|data_type|allowed_values

Supported data types:
    string, int, float, decimal, boolean, date, datetime, name, email,
    phone, address, city, country, zipcode, uuid, sentence, paragraph, url

Usage:
    python generate_xml_data.py                                  # defaults
    python generate_xml_data.py -m mapping.csv -t person -n 50
    python generate_xml_data.py -m mapping.csv -t person -n 100 -o people.xml --root people
    python generate_xml_data.py -m mapping.csv -t person -n 10 --pretty --seed 42
"""

import argparse
import csv
import os
import random
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.dom import minidom

from faker import Faker

fake = Faker()

# Maps data_type strings to Faker-based generator callables
_GENERATORS: dict[str, callable] = {
    "string": lambda: fake.word().capitalize(),
    "int": lambda: str(random.randint(1, 9999)),
    "float": lambda: f"{random.uniform(1.0, 999.99):.2f}",
    "decimal": lambda: f"{random.uniform(1.0, 999.99):.2f}",
    "boolean": lambda: random.choice(["true", "false"]),
    "date": lambda: fake.date_between(start_date="-10y", end_date="today").isoformat(),
    "datetime": lambda: fake.date_time_between(
        start_date="-10y", end_date="now"
    ).strftime("%Y-%m-%dT%H:%M:%S"),
    "name": lambda: fake.name(),
    "first_name": lambda: fake.first_name(),
    "last_name": lambda: fake.last_name(),
    "email": lambda: fake.email(),
    "phone": lambda: fake.phone_number(),
    "address": lambda: fake.street_address(),
    "city": lambda: fake.city(),
    "country": lambda: fake.country(),
    "zipcode": lambda: fake.zipcode(),
    "uuid": lambda: fake.uuid4(),
    "sentence": lambda: fake.sentence(),
    "paragraph": lambda: fake.paragraph(nb_sentences=2),
    "url": lambda: fake.url(),
    "company": lambda: fake.company(),
    "job": lambda: fake.job(),
}


def load_mapping(file_path: str | Path) -> list[dict[str, str]]:
    """Load a pipe-delimited CSV mapping file into a list of row dicts."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")

    with open(path, newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter="|")
        rows = list(reader)

    required_cols = {"tag", "data_type"}
    if rows and not required_cols.issubset(rows[0].keys()):
        missing = required_cols - set(rows[0].keys())
        raise ValueError(f"Mapping CSV missing required columns: {missing}")

    return rows


def generate_value(data_type: str, allowed_values: str = "") -> str:
    """
    Generate a single fake value based on data type and optional allowed values.

    If allowed_values is non-empty (comma-separated), picks one at random.
    Otherwise delegates to the type-specific Faker generator.
    """
    # Strip whitespace from allowed values and filter empties
    if allowed_values:
        choices = [v.strip() for v in allowed_values.split(",") if v.strip()]
        if choices:
            return random.choice(choices)

    generator = _GENERATORS.get(data_type, _GENERATORS["string"])
    return generator()


def build_single_element(mapping: list[dict], main_tag: str) -> ET.Element:
    """
    Build one XML element with sub-elements and attributes from the mapping.

    Each row in the mapping defines either a sub-element text value or an
    attribute on the current sub-element. A new sub-element is created whenever
    the 'tag' column changes.
    """
    main_element = ET.Element(main_tag)
    current_element = None
    current_tag = None

    for row in mapping:
        tag = row.get("tag", "").strip()
        attr = row.get("attribute", "").strip()
        attr_type = row.get("attribute_type", "string").strip()
        attr_allowed = row.get("attribute_allowed_values", "").strip()
        data_type = row.get("data_type", "").strip()
        allowed = row.get("allowed_values", "").strip()

        if not tag:
            continue

        # Create a new sub-element when the tag changes
        if tag != current_tag:
            current_element = ET.SubElement(main_element, tag)
            current_tag = tag

        if attr:
            # Set an attribute on the current element
            current_element.set(attr, generate_value(attr_type, attr_allowed))
        elif data_type:
            # Set the text content
            current_element.text = generate_value(data_type, allowed)

    return main_element


def remove_empty_elements(element: ET.Element) -> None:
    """Recursively remove elements with no text, no children, and no attributes."""
    if element is None:
        return
    for child in list(element):
        remove_empty_elements(child)
        if (
            not (child.text or "").strip()
            and len(child) == 0
            and len(child.attrib) == 0
        ):
            element.remove(child)


def build_xml(
    mapping: list[dict],
    main_tag: str,
    count: int = 1000,
    root_tag: str = "root",
) -> ET.ElementTree:
    """
    Build a complete XML tree with `count` row elements under a root wrapper.

    Args:
        mapping: Parsed CSV mapping rows.
        main_tag: Element tag name for each generated row.
        count: Number of row elements to generate.
        root_tag: Wrapping root element name.

    Returns:
        An ElementTree ready to be written to file.
    """
    root = ET.Element(root_tag)
    for _ in range(count):
        element = build_single_element(mapping, main_tag)
        root.append(element)
    remove_empty_elements(root)
    return ET.ElementTree(root)


def pretty_print_xml(tree: ET.ElementTree) -> str:
    """Return an indented XML string from an ElementTree."""
    rough = ET.tostring(tree.getroot(), encoding="unicode")
    return minidom.parseString(rough).toprettyxml(indent="  ")


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample XML data from a pipe-delimited CSV mapping file."
    )
    parser.add_argument(
        "--mapping",
        "-m",
        type=Path,
        default=Path(__file__).parent / "mapping.csv",
        help="Path to pipe-delimited CSV mapping file (default: mapping.csv in script dir)",
    )
    parser.add_argument(
        "--tag",
        "-t",
        default="person",
        help="Row element tag name (default: person)",
    )
    parser.add_argument(
        "--root",
        "-r",
        default="root",
        help="Root wrapper element tag name (default: root)",
    )
    parser.add_argument(
        "--count",
        "-n",
        type=int,
        default=1000,
        help="Number of row elements to generate (default: 1000)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output XML file path (default: DATA_HOME/file_data/xml/<tag>.xml)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Write pretty-printed (indented) XML output",
    )
    parser.add_argument(
        "--seed",
        "-s",
        type=int,
        help="Random seed for reproducible output",
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)

    mapping = load_mapping(args.mapping)

    xml_tree = build_xml(
        mapping=mapping,
        main_tag=args.tag,
        count=args.count,
        root_tag=args.root,
    )

    if args.output:
        xml_file = args.output
    else:
        data_home = os.environ.get("DATA_HOME", ".")
        xml_file = Path(data_home) / "file_data" / "xml" / f"{args.tag}.xml"

    xml_file = Path(xml_file)
    xml_file.parent.mkdir(parents=True, exist_ok=True)

    if args.pretty:
        xml_file.write_text(pretty_print_xml(xml_tree), encoding="utf-8")
    else:
        xml_tree.write(str(xml_file), encoding="utf-8", xml_declaration=True)

    print(f"✅ Generated {args.count} <{args.tag}> elements → {xml_file}")


if __name__ == "__main__":
    main()
