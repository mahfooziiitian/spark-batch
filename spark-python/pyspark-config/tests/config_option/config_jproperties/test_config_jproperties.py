import os

import pytest

from config_option.config_jproperties.config_jproperties import PropertiesHandler


@pytest.fixture
def file_path():
    print(os.getcwd())
    file_path = '../cfg/config.properties'
    with open(file_path, 'w') as f:
        f.write("key1=value1\nkey2=value2")
    yield file_path
    if os.path.exists(file_path):
        os.remove(file_path)


def test_read_properties(file_path):
    handler = PropertiesHandler(file_path)
    properties = handler.read_properties()
    assert (properties['key1'].data == 'value1')
    assert (properties['key2'].data == 'value2')


def test_write_properties(file_path):
    handler = PropertiesHandler(file_path)
    properties = handler.read_properties()
    properties['key3'] = 'value3'

    handler.write_properties(properties)

    # Read properties again and check if the new key-value pair is added
    updated_properties = handler.read_properties()
    assert (updated_properties['key1'].data == 'value1')
    assert (updated_properties['key2'].data == 'value2')
    assert (updated_properties['key3'].data == 'value3')
