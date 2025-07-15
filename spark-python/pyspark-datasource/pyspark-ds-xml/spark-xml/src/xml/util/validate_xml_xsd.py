import os
import xmlschema


def validate_xml(xsd_file, xml_file):
    schema = xmlschema.XMLSchema(xsd_file)
    try:
        schema.validate(xml_file)
        print("✅ XML is valid against the XSD!")
    except xmlschema.XMLSchemaValidationError as e:
        print("❌ XML validation error:")
        print(e)


def main():
    # Example usage
    data_home = os.environ.get("DATA_HOME", ".")
    xsd_file = os.path.join(data_home, "file_data", "xml", "person.xsd")
    xml_file = os.path.join(data_home, "file_data", "xml", "person.xml")
    validate_xml(xsd_file, xml_file)


if __name__ == "__main__":
    main()
