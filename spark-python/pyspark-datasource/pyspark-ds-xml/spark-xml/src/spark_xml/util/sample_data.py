"""Deterministic, self-contained sample-data fixtures for the ``examples/`` scripts.

Every ``ensure_*`` helper writes a small, well-known XML/JSON/XSD fixture to the
requested path only when it does not already exist. This keeps each example
runnable standalone -- with no external data required -- while still honoring
real data an operator may have already placed under ``DATA_HOME``.
"""

from pathlib import Path
from typing import Union

StrPath = Union[str, Path]


def _write_if_missing(path: StrPath, content: str) -> Path:
    resolved = Path(path)
    if not resolved.exists():
        resolved.parent.mkdir(parents=True, exist_ok=True)
        resolved.write_text(content, encoding="utf-8")
    return resolved


PERSON_XML = """<?xml version="1.0" encoding="UTF-8"?>
<root>
  <person age="30" gender="M">
    <name>Alice Johnson</name>
    <city>Seattle</city>
  </person>
  <person age="27" gender="F">
    <name>Priya Nair</name>
    <city>Bengaluru</city>
  </person>
  <person age="45" gender="M">
    <name>Diego Martinez</name>
    <city>Madrid</city>
  </person>
</root>
"""


def ensure_person_xml(path: StrPath) -> Path:
    """Write a small ``person.xml`` fixture (attribute + element mix)."""
    return _write_if_missing(path, PERSON_XML)


BOOK_NAMESPACE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<bk:books xmlns:bk="http://example.com/books">
  <bk:book id="bk101">
    <bk:author>Gambardella, Matthew</bk:author>
    <bk:title>XML Developer's Guide</bk:title>
    <bk:price>44.95</bk:price>
  </bk:book>
  <bk:book id="bk102">
    <bk:author>Ralls, Kim</bk:author>
    <bk:title>Midnight Rain</bk:title>
    <bk:price>5.95</bk:price>
  </bk:book>
</bk:books>
"""


def ensure_book_namespace_xml(path: StrPath) -> Path:
    """Write a namespaced ``book.xml`` fixture (``bk:`` prefixed elements)."""
    return _write_if_missing(path, BOOK_NAMESPACE_XML)


BOOKS_PRICED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<books>
  <book id="bk101">
    <author>Gambardella, Matthew</author>
    <title>XML Developer's Guide</title>
    <price currency="USD">44.95</price>
  </book>
  <book id="bk102">
    <author>Ralls, Kim</author>
    <title>Midnight Rain</title>
    <price currency="USD">5.95</price>
  </book>
</books>
"""


def ensure_books_priced_xml(path: StrPath) -> Path:
    """Write a ``books.xml`` fixture whose ``price`` mixes a value tag and an attribute."""
    return _write_if_missing(path, BOOKS_PRICED_XML)


BOOKS_CATALOG_XML = """<?xml version="1.0" encoding="UTF-8"?>
<root>
  <catalog>
    <dt_creation>2024-01-15</dt_creation>
    <book id="bk101">
      <author>Gambardella, Matthew</author>
      <title>XML Developer's Guide</title>
    </book>
    <book id="bk102">
      <author>Ralls, Kim</author>
      <title>Midnight Rain</title>
    </book>
  </catalog>
</root>
"""


def ensure_books_catalog_xml(path: StrPath) -> Path:
    """Write a ``books.xml`` fixture with a ``catalog`` row containing an array of books."""
    return _write_if_missing(path, BOOKS_CATALOG_XML)


DATA_ARRAY_OF_STRUCTS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<root>
  <arrayOfStructs>
    <struct>
      <field1>value1a</field1>
      <field2>value2a</field2>
    </struct>
    <struct>
      <field1>value1b</field1>
      <field2>value2b</field2>
    </struct>
  </arrayOfStructs>
</root>
"""


def ensure_data_array_of_structs_xml(path: StrPath) -> Path:
    """Write a ``data.xml`` fixture with an array of ``struct`` elements."""
    return _write_if_missing(path, DATA_ARRAY_OF_STRUCTS_XML)


POS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<foo>
  <bar>
    <sum>10</sum>
    <periods>
      <start>2024-01-01</start>
      <end>2024-01-31</end>
    </periods>
  </bar>
  <bar>
    <sum>20</sum>
    <periods>
      <start>2024-02-01</start>
      <end>2024-02-29</end>
    </periods>
  </bar>
</foo>
"""


def ensure_pos_xml(path: StrPath) -> Path:
    """Write a ``pos.xml`` fixture used to demonstrate ``posexplode``."""
    return _write_if_missing(path, POS_XML)


MOVIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<collection>
  <movie>
    <title>Inception</title>
    <year>2010</year>
    <rating>8.8</rating>
  </movie>
  <movie>
    <title>The Matrix</title>
    <year>1999</year>
    <rating>8.7</rating>
  </movie>
</collection>
"""


def ensure_movies_xml(path: StrPath) -> Path:
    """Write a ``movies.xml`` fixture (``collection``/``movie`` rows)."""
    return _write_if_missing(path, MOVIES_XML)


DATE_TIME_SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<root>
  <record>
    <birth_date>15-08-1990</birth_date>
    <created_at>08/15/2024 09:30</created_at>
  </record>
  <record>
    <birth_date>02-11-1985</birth_date>
    <created_at>11/02/2024 14:05</created_at>
  </record>
</root>
"""


def ensure_date_time_sample_xml(path: StrPath) -> Path:
    """Write a fixture with non-ISO ``birth_date``/``created_at`` formats."""
    return _write_if_missing(path, DATE_TIME_SAMPLE_XML)


NON_ISO_TIMESTAMP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<root>
  <record>
    <id>1</id>
    <created_at>2024-08-15T09:30:00</created_at>
  </record>
  <record>
    <id>2</id>
    <created_at>not-a-timestamp</created_at>
  </record>
</root>
"""


def ensure_non_iso_timestamp_xml(path: StrPath) -> Path:
    """Write a fixture with one valid ISO timestamp and one malformed value.

    Used with ``mode=PERMISSIVE`` + ``columnNameOfCorruptRecord`` to demonstrate
    corrupt-record capture.
    """
    return _write_if_missing(path, NON_ISO_TIMESTAMP_XML)


NOTES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<root>
  <note>
    <to>Tove</to>
    <from>Jani</from>
    <heading>Reminder</heading>
    <body>Don't forget me this weekend!</body>
  </note>
  <note>
    <to>Jani</to>
    <from>Tove</from>
    <heading>Re: Reminder</heading>
    <body>I will not forget!</body>
  </note>
</root>
"""


def ensure_notes_xml(path: StrPath) -> Path:
    """Write the classic ``notes.xml`` fixture."""
    return _write_if_missing(path, NOTES_XML)


NOTES_SCHEMA_JSON = """{
  "type": "struct",
  "fields": [
    {"name": "to", "type": "string", "nullable": true, "metadata": {}},
    {"name": "from", "type": "string", "nullable": true, "metadata": {}},
    {"name": "heading", "type": "string", "nullable": true, "metadata": {}},
    {"name": "body", "type": "string", "nullable": true, "metadata": {}}
  ]
}
"""


def ensure_notes_schema_json(path: StrPath) -> Path:
    """Write a ``StructType.fromJson``-compatible schema for :data:`NOTES_XML`."""
    return _write_if_missing(path, NOTES_SCHEMA_JSON)


NESTED_BATCH_XML = """<?xml version="1.0" encoding="UTF-8"?>
<DWHBatch>
  <Header>
    <BatchId>BATCH-0001</BatchId>
    <TotalNoOfRecords>2</TotalNoOfRecords>
  </Header>
  <Records>
    <Issuance>
      <Entry>POLICY-1001</Entry>
    </Issuance>
    <Issuance>
      <Entry>POLICY-1002</Entry>
    </Issuance>
    <PolicyChange>PC-2001</PolicyChange>
    <Cancellation>CX-3001</Cancellation>
    <Submission>SUB-4001</Submission>
    <Reinstatement>RE-5001</Reinstatement>
    <Rewrite>RW-6001</Rewrite>
    <Renewal>REN-7001</Renewal>
    <RenewalSubmission>RENSUB-8001</RenewalSubmission>
  </Records>
</DWHBatch>
"""


def ensure_nested_batch_xml(path: StrPath) -> Path:
    """Write a ``DWHBatch``-shaped fixture used by the nested XML examples."""
    return _write_if_missing(path, NESTED_BATCH_XML)


ORDERS_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Root">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Customers">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="Customer" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="CompanyName" type="xs:string"/>
                    <xs:element name="ContactName" type="xs:string"/>
                    <xs:element name="ContactTitle" type="xs:string"/>
                    <xs:element name="Fax" type="xs:string" minOccurs="0"/>
                    <xs:element name="FullAddress">
                      <xs:complexType>
                        <xs:sequence>
                          <xs:element name="Address" type="xs:string"/>
                          <xs:element name="City" type="xs:string"/>
                          <xs:element name="Country" type="xs:string"/>
                          <xs:element name="PostalCode" type="xs:integer"/>
                          <xs:element name="Region" type="xs:string" minOccurs="0"/>
                        </xs:sequence>
                      </xs:complexType>
                    </xs:element>
                    <xs:element name="Phone" type="xs:string"/>
                  </xs:sequence>
                  <xs:attribute name="CustomerID" type="xs:string" use="required"/>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name="Orders">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="Order" maxOccurs="unbounded">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="CustomerID" type="xs:string"/>
                    <xs:element name="EmployeeID" type="xs:integer"/>
                    <xs:element name="OrderDate" type="xs:date"/>
                    <xs:element name="RequiredDate" type="xs:date"/>
                    <xs:element name="ShipInfo">
                      <xs:complexType>
                        <xs:sequence>
                          <xs:element name="Freight" type="xs:decimal"/>
                          <xs:element name="ShipAddress" type="xs:string"/>
                          <xs:element name="ShipCity" type="xs:string"/>
                          <xs:element name="ShipCountry" type="xs:string"/>
                          <xs:element name="ShipName" type="xs:string"/>
                          <xs:element name="ShipPostalCode" type="xs:integer"/>
                          <xs:element name="ShipRegion" type="xs:string" minOccurs="0"/>
                          <xs:element name="ShipVia" type="xs:integer"/>
                        </xs:sequence>
                        <xs:attribute name="ShippedDate" type="xs:date" use="optional"/>
                      </xs:complexType>
                    </xs:element>
                  </xs:sequence>
                </xs:complexType>
              </xs:element>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>
"""


def ensure_orders_xsd(path: StrPath) -> Path:
    """Write the ``orders.xsd`` schema shared by the XSD validator examples."""
    return _write_if_missing(path, ORDERS_XSD)


ORDERS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Root>
  <Customers>
    <Customer CustomerID="CUST001">
      <CompanyName>Contoso Ltd</CompanyName>
      <ContactName>Maria Anders</ContactName>
      <ContactTitle>Sales Representative</ContactTitle>
      <Fax>030-0076545</Fax>
      <FullAddress>
        <Address>Obere Str. 57</Address>
        <City>Berlin</City>
        <Country>Germany</Country>
        <PostalCode>12209</PostalCode>
        <Region>EU</Region>
      </FullAddress>
      <Phone>030-0074321</Phone>
    </Customer>
    <Customer CustomerID="CUST002">
      <CompanyName>Fabrikam Inc</CompanyName>
      <ContactName>Ana Trujillo</ContactName>
      <ContactTitle>Owner</ContactTitle>
      <FullAddress>
        <Address>Avda. de la Constitucion 2222</Address>
        <City>Mexico D.F.</City>
        <Country>Mexico</Country>
        <PostalCode>5021</PostalCode>
      </FullAddress>
      <Phone>(5) 555-4729</Phone>
    </Customer>
  </Customers>
  <Orders>
    <Order>
      <CustomerID>CUST001</CustomerID>
      <EmployeeID>5</EmployeeID>
      <OrderDate>2024-01-10</OrderDate>
      <RequiredDate>2024-01-20</RequiredDate>
      <ShipInfo ShippedDate="2024-01-12">
        <Freight>32.50</Freight>
        <ShipAddress>Obere Str. 57</ShipAddress>
        <ShipCity>Berlin</ShipCity>
        <ShipCountry>Germany</ShipCountry>
        <ShipName>Contoso Ltd</ShipName>
        <ShipPostalCode>12209</ShipPostalCode>
        <ShipRegion>EU</ShipRegion>
        <ShipVia>2</ShipVia>
      </ShipInfo>
    </Order>
    <Order>
      <CustomerID>CUST002</CustomerID>
      <EmployeeID>3</EmployeeID>
      <OrderDate>2024-02-05</OrderDate>
      <RequiredDate>2024-02-15</RequiredDate>
      <ShipInfo ShippedDate="2024-02-07">
        <Freight>18.75</Freight>
        <ShipAddress>Avda. de la Constitucion 2222</ShipAddress>
        <ShipCity>Mexico D.F.</ShipCity>
        <ShipCountry>Mexico</ShipCountry>
        <ShipName>Fabrikam Inc</ShipName>
        <ShipPostalCode>5021</ShipPostalCode>
        <ShipVia>1</ShipVia>
      </ShipInfo>
    </Order>
  </Orders>
</Root>
"""


def ensure_orders_xml(path: StrPath) -> Path:
    """Write a schema-valid ``orders.xml`` fixture matching :data:`ORDERS_XSD`."""
    return _write_if_missing(path, ORDERS_XML)


ORDERS_CORRUPT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Root>
  <Customers>
    <Customer CustomerID="CUST001">
      <CompanyName>Contoso Ltd</CompanyName>
      <ContactName>Maria Anders</ContactName>
      <FullAddress>
        <Address>Obere Str. 57</Address>
        <City>Berlin</City>
        <Country>Germany</Country>
        <PostalCode>12209</PostalCode>
      </FullAddress>
      <Phone>030-0074321</Phone>
    </Customer>
  </Customers>
  <Orders>
    <Order>
      <CustomerID>CUST001</CustomerID>
      <EmployeeID>5</EmployeeID>
      <OrderDate>2024-01-10</OrderDate>
      <RequiredDate>2024-01-20</RequiredDate>
      <ShipInfo ShippedDate="2024-01-12">
        <Freight>32.50</Freight>
        <ShipAddress>Obere Str. 57</ShipAddress>
        <ShipCity>Berlin</ShipCity>
        <ShipCountry>Germany</ShipCountry>
        <ShipName>Contoso Ltd</ShipName>
        <ShipPostalCode>12209</ShipPostalCode>
        <ShipVia>2</ShipVia>
      </ShipInfo>
    </Order>
  </Orders>
</Root>
"""


def ensure_orders_corrupt_xml(path: StrPath) -> Path:
    """Write an ``orders.xml`` variant that is missing the required ``ContactTitle``.

    Intended to be read with ``mode=FAILFAST`` + ``rowValidationXSDPath`` so the
    XSD validation failure can be demonstrated/caught.
    """
    return _write_if_missing(path, ORDERS_CORRUPT_XML)


ORDERS_INVALID_FORMAT_XML = """<?xml version="1.0" encoding="UTF-8"?>
<Root>
  <Customers>
    <Customer CustomerID="CUST001">
      <CompanyName>Contoso Ltd</CompanyName>
      <ContactName>Maria Anders</ContactName>
      <ContactTitle>Sales Representative</ContactTitle>
      <FullAddress>
        <Address>Obere Str. 57</Address>
        <City>Berlin</City>
        <Country>Germany</Country>
        <PostalCode>12209</PostalCode>
      </FullAddress>
      <Phone>030-0074321</Phone>
    </Customer>
  </Customers>
  <Orders>
    <Order>
      <CustomerID>CUST001</CustomerID>
      <EmployeeID>5</EmployeeID>
      <OrderDate>not-a-valid-date</OrderDate>
      <RequiredDate>2024-01-20</RequiredDate>
      <ShipInfo ShippedDate="2024-01-12">
        <Freight>32.50</Freight>
        <ShipAddress>Obere Str. 57</ShipAddress>
        <ShipCity>Berlin</ShipCity>
        <ShipCountry>Germany</ShipCountry>
        <ShipName>Contoso Ltd</ShipName>
        <ShipPostalCode>12209</ShipPostalCode>
        <ShipVia>2</ShipVia>
      </ShipInfo>
    </Order>
  </Orders>
</Root>
"""


def ensure_orders_invalid_format_xml(path: StrPath) -> Path:
    """Write an ``orders.xml`` variant with a malformed ``OrderDate`` value.

    Intended to be read with ``mode=FAILFAST`` to demonstrate XSD validation
    catching a badly formatted date.
    """
    return _write_if_missing(path, ORDERS_INVALID_FORMAT_XML)


SIMPLE_ORDERS_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="Orders">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="Order" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="OrderID" type="xs:string"/>
              <xs:element name="CustomerName" type="xs:string"/>
              <xs:element name="Items">
                <xs:complexType>
                  <xs:sequence>
                    <xs:element name="Item" maxOccurs="unbounded">
                      <xs:complexType>
                        <xs:sequence>
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
"""


def ensure_simple_orders_xsd(path: StrPath) -> Path:
    """Write a simple ``Orders``/``Order`` XSD used by the XSD-driven XML generators."""
    return _write_if_missing(path, SIMPLE_ORDERS_XSD)
