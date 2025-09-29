from mapping.rest_response_to_table_mapping import get_nested_value


def test_get_nested_value_simple_path():
    data = {"a": 1, "b": 2}
    assert get_nested_value(data, "a") == 1
    assert get_nested_value(data, "b") == 2


def test_get_nested_value_nested_path():
    data = {"a": {"b": {"c": 42}}}
    assert get_nested_value(data, "a.b.c") == 42


def test_get_nested_value_missing_key_returns_default():
    data = {"a": {"b": 1}}
    assert get_nested_value(data, "a.c", default="not found") == "not found"
    assert get_nested_value(data, "x.y.z", default=None) is None


def test_get_nested_value_non_dict_in_path():
    data = {"a": {"b": 5}}
    # "a.b" is 5 (not a dict), so "a.b.c" should return default
    assert get_nested_value(data, "a.b.c", default="fail") == "fail"


def test_get_nested_value_none_in_path():
    data = {"a": None}
    assert get_nested_value(data, "a.b", default="missing") == "missing"


def test_get_nested_value_empty_path():
    data = {"a": 1}
    assert get_nested_value(data, "", default="") == ""


def test_get_nested_value_default_is_used_when_path_not_found():
    data = {"x": {"y": 2}}
    assert get_nested_value(data, "x.z", default=123) == 123


def test_get_nested_value_path_to_none_value():
    data = {"a": {"b": None}}
    assert get_nested_value(data, "a.b", default="default") == "default"
