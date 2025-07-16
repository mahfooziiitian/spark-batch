"""
This script generates an XSD schema from a sample XML file.
It uses the xmltoxsd library to create the schema and saves it to a specified location.
"""

import os
from xmltoxsd import XSDGenerator

data_home = os.environ.get("DATA_HOME", ".")
sample_xml_file = os.path.join(data_home, "file_data", "xml", "notes.xml")
generator = XSDGenerator()
xsd_schema = generator.generate_xsd(sample_xml_file)
with open(os.path.join(data_home, "file_data", "xml", "notes.xsd"), "w") as f:
    f.write(xsd_schema)
print("XSD Schema generated successfully:")
