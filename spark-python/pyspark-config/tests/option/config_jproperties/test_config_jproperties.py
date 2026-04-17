import pytest

from cfg.option.config_jproperties.config_jproperties import PropertiesHandler


@pytest.fixture
def file_path(tmp_path):
    path = str(tmp_path / "config.properties")
    with open(path, 'w') as f:
        f.write("key1=value1\nkey2=value2")
    return path


def test_read_properties(file_path):
    handler = PropertiesHandler(file_path)
    properties = handler.read_properties()
    assert properties['key1'].data == 'value1'
    assert properties['key2'].data == 'value2'


def test_write_properties(file_path):
    handler = PropertiesHandler(file_path)
    properties = handler.read_properties()
    properties['key3'] = 'value3'

    handler.write_properties(properties)

    updated_properties = handler.read_properties()
    assert updated_properties['key1'].data == 'value1'
    assert updated_properties['key2'].data == 'value2'
    assert updated_properties['key3'].data == 'value3'


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
