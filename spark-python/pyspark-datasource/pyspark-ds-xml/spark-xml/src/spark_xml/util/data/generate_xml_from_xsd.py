"""
XSD to XML Data Generator

Reads an XSD schema file, introspects its structure (elements, attributes, types,
constraints), and generates realistic sample XML data using Faker.

Supports:
- Simple types: string, int/integer/long, decimal/float/double, boolean, date, dateTime
- Complex types with nested sequences
- Attributes (required and optional) with type-aware generation
- xs:enumeration restrictions for constrained values
- minOccurs / maxOccurs for repeating elements
- Recursive/nested complex type definitions
- Pretty-printed XML output with xml declaration

Usage:
    python generate_xml_from_xsd.py <xsd_file> [--output <xml_file>] [--count <n>]

Examples:
    python generate_xml_from_xsd.py books.xsd
    python generate_xml_from_xsd.py books.xsd --output books.xml --count 5
"""

import argparse
import random
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.dom import minidom

import xmlschema
from faker import Faker

fake = Faker()

XSD_NS = "http://www.w3.org/2001/XMLSchema"

# XSD type name -> category mapping
_TYPE_MAP = {
    "string": "string",
    "normalizedString": "string",
    "token": "string",
    "int": "int",
    "integer": "int",
    "long": "int",
    "short": "int",
    "positiveInteger": "int",
    "nonNegativeInteger": "int",
    "negativeInteger": "int",
    "nonPositiveInteger": "int",
    "unsignedInt": "int",
    "unsignedLong": "int",
    "unsignedShort": "int",
    "byte": "int",
    "unsignedByte": "int",
    "decimal": "decimal",
    "float": "decimal",
    "double": "decimal",
    "boolean": "boolean",
    "date": "date",
    "dateTime": "dateTime",
    "time": "time",
    "gYear": "year",
    "gYearMonth": "yearMonth",
    "anyURI": "uri",
    "ID": "id",
    "IDREF": "string",
}


def _resolve_type_category(xsd_type_name: str) -> str:
    """Map an XSD type local name to our internal category."""
    if xsd_type_name is None:
        return "string"
    local = xsd_type_name.split("}")[-1] if "}" in xsd_type_name else xsd_type_name
    return _TYPE_MAP.get(local, "string")


def _get_enumerations(xsd_element) -> list[str] | None:
    """Extract xs:enumeration values from an element's type restriction."""
    xsd_type = getattr(xsd_element, "type", None)
    if xsd_type is None:
        return None

    # Check for enumeration facets
    if hasattr(xsd_type, "enumeration"):
        enums = xsd_type.enumeration
        if enums:
            return [str(v) for v in enums]

    # Walk validators/facets for enumeration
    if hasattr(xsd_type, "validators"):
        for validator in xsd_type.validators:
            if hasattr(validator, "enumeration") and validator.enumeration:
                return [str(v) for v in validator.enumeration]

    # Try facets dict
    if hasattr(xsd_type, "facets"):
        enum_facet = xsd_type.facets.get("enumeration")
        if enum_facet is not None and hasattr(enum_facet, "enumeration"):
            return [str(v) for v in enum_facet.enumeration]

    return None


def generate_value(type_category: str, enumerations: list[str] | None = None) -> str:
    """Generate a realistic fake value for the given XSD type category."""
    if enumerations:
        return random.choice(enumerations)

    match type_category:
        case "string":
            return fake.word().capitalize()
        case "int":
            return str(random.randint(1, 9999))
        case "decimal":
            return f"{random.uniform(1.0, 999.99):.2f}"
        case "boolean":
            return random.choice(["true", "false"])
        case "date":
            d = fake.date_between(
                start_date=date(2000, 1, 1), end_date=date(2025, 12, 31)
            )
            return d.isoformat()
        case "dateTime":
            dt = fake.date_time_between(
                start_date=datetime(2000, 1, 1), end_date=datetime(2025, 12, 31)
            )
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        case "time":
            return fake.time()
        case "year":
            return str(random.randint(2000, 2025))
        case "yearMonth":
            return f"{random.randint(2000, 2025)}-{random.randint(1, 12):02d}"
        case "uri":
            return fake.url()
        case "id":
            return f"id_{random.randint(1000, 9999)}"
        case _:
            return fake.word()


def _get_min_max_occurs(xsd_element) -> tuple[int, int]:
    """Get min/max occurrence counts, capped for generation sanity."""
    min_occurs = getattr(xsd_element, "min_occurs", 1) or 1
    max_occurs = getattr(xsd_element, "max_occurs", 1)
    if max_occurs is None or max_occurs == "unbounded" or max_occurs > 5:
        max_occurs = random.randint(min_occurs, max(min_occurs, 3))
    return min_occurs, max_occurs


def build_element(xsd_element, parent_xml: ET.Element) -> None:
    """Recursively build an XML element from its XSD definition."""
    xsd_type = getattr(xsd_element, "type", None)

    # Determine if this is a complex type (has child elements)
    is_complex = False
    child_elements = []
    attributes = []

    if xsd_type is not None:
        if hasattr(xsd_type, "content") and xsd_type.content is not None:
            is_complex = True
        if hasattr(xsd_type, "attributes"):
            attributes = list(xsd_type.attributes.values())
        # Gather child elements from content model
        if is_complex and hasattr(xsd_type, "content"):
            content = xsd_type.content
            if hasattr(content, "__iter__"):
                for child in content:
                    if hasattr(child, "local_name"):
                        child_elements.append(child)
            elif hasattr(content, "iter_elements"):
                child_elements = list(content.iter_elements())

    # Also try iter_elements directly on the type
    if (
        not child_elements
        and xsd_type is not None
        and hasattr(xsd_type, "iter_elements")
    ):
        try:
            child_elements = list(xsd_type.iter_elements())
        except Exception:
            pass

    local_name = getattr(xsd_element, "local_name", None) or xsd_element.name
    xml_el = ET.SubElement(parent_xml, local_name)

    # Set attributes
    for attr in attributes:
        attr_name = getattr(attr, "local_name", None) or attr.name
        attr_type_name = None
        if hasattr(attr, "type") and attr.type is not None:
            attr_type_name = getattr(attr.type, "local_name", None)
        attr_enums = _get_enumerations(attr)
        attr_category = _resolve_type_category(attr_type_name)
        xml_el.set(attr_name, generate_value(attr_category, attr_enums))

    if child_elements:
        # Complex type — recurse into children
        for child_el in child_elements:
            min_occ, max_occ = _get_min_max_occurs(child_el)
            count = random.randint(min_occ, max_occ)
            for _ in range(count):
                build_element(child_el, xml_el)
    else:
        # Simple type — generate text value
        type_name = None
        if xsd_type is not None:
            type_name = getattr(xsd_type, "local_name", None)
        enumerations = _get_enumerations(xsd_element)
        category = _resolve_type_category(type_name)
        xml_el.text = generate_value(category, enumerations)


def generate_xml_from_xsd(
    xsd_path: str,
    count: int = 3,
    root_tag: str | None = None,
) -> ET.ElementTree:
    """
    Parse an XSD file and generate sample XML data.

    Args:
        xsd_path: Path to the XSD schema file.
        count: Number of row elements to generate.
        root_tag: Override for the wrapping root element name.
                  Defaults to 'root' if the XSD has a single top-level element,
                  or '<element_name>s' as a plural wrapper.

    Returns:
        An ElementTree with generated XML data.
    """
    schema = xmlschema.XMLSchema(xsd_path)

    # Get the top-level element(s) defined in the XSD
    top_elements = list(schema.elements.values())
    if not top_elements:
        raise ValueError(f"No top-level elements found in XSD: {xsd_path}")

    # Use the first (or only) top-level element as the row element
    row_element = top_elements[0]
    row_name = getattr(row_element, "local_name", None) or row_element.name

    if root_tag is None:
        root_tag = (
            f"{row_name}s" if not row_name.endswith("s") else f"{row_name}_collection"
        )

    root = ET.Element(root_tag)

    for _ in range(count):
        build_element(row_element, root)

    return ET.ElementTree(root)


def pretty_print_xml(tree: ET.ElementTree) -> str:
    """Return a pretty-printed XML string with proper indentation."""
    rough_string = ET.tostring(tree.getroot(), encoding="unicode")
    parsed = minidom.parseString(rough_string)
    return parsed.toprettyxml(indent="  ", encoding=None)


def main():
    parser = argparse.ArgumentParser(
        description="Generate sample XML data from an XSD schema file."
    )
    parser.add_argument("xsd_file", help="Path to the XSD schema file")
    parser.add_argument(
        "--output",
        "-o",
        help="Output XML file path (default: <xsd_name>_generated.xml in same directory)",
    )
    parser.add_argument(
        "--count",
        "-n",
        type=int,
        default=3,
        help="Number of row elements to generate (default: 3)",
    )
    parser.add_argument(
        "--root-tag",
        "-r",
        help="Custom root element tag name (default: auto-derived from XSD)",
    )
    args = parser.parse_args()

    xsd_path = Path(args.xsd_file)
    if not xsd_path.exists():
        print(f"❌ XSD file not found: {xsd_path}")
        sys.exit(1)

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = xsd_path.parent / f"{xsd_path.stem}_generated.xml"

    print(f"📖 Reading XSD: {xsd_path}")
    tree = generate_xml_from_xsd(
        xsd_path=str(xsd_path),
        count=args.count,
        root_tag=args.root_tag,
    )

    xml_string = pretty_print_xml(tree)
    output_path.write_text(xml_string, encoding="utf-8")
    print(f"✅ Generated {args.count} elements → {output_path}")

    # Preview
    print("\n--- Preview ---")
    lines = xml_string.strip().split("\n")
    preview = "\n".join(lines[:30])
    if len(lines) > 30:
        preview += f"\n... ({len(lines) - 30} more lines)"
    print(preview)


if __name__ == "__main__":
    main()
