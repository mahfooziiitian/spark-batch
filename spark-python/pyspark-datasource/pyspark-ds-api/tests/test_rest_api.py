from rest_api import read_key_value


def test_read_key_value_1():
    json_dict = {
        "data": [],
        "meta": {"page": 2, "page_size": 10, "total": 100},
        "links": {
            "self": "/items?page=2",
            "next": "/items?page=3",
            "prev": "/items?page=1",
            "first": "/items?page=1",
            "last": "/items?page=10",
        },
    }

    key = "meta.page"
    expected_value = 2
    actual_value = read_key_value(json_dict, key)
    assert (
        actual_value == expected_value
    ), f"Expected {expected_value}, but got {actual_value}"


def test_read_key_value_2():
    json_dict = {
        "data": [],
        "meta": {"page": 2, "page_size": 10, "total": 100},
        "links": {
            "self": "/items?page=2",
            "next": "/items?page=3",
            "prev": "/items?page=1",
            "first": "/items?page=1",
            "last": "/items?page=10",
        },
    }

    key = "links.next"
    expected_value = "/items?page=3"
    actual_value = read_key_value(json_dict, key)
    assert (
        actual_value == expected_value
    ), f"Expected {expected_value}, but got {actual_value}"
