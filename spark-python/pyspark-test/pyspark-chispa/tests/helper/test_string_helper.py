import pytest

from data_frame.helper.string_helper import dots_to_underscores, snake_case, truncate


class TestDotsToUnderscores:
    """Tests for the dots_to_underscores helper."""

    def test_single_dot(self):
        assert dots_to_underscores("a.b") == "a_b"

    def test_multiple_dots(self):
        assert dots_to_underscores("a.b.c.d") == "a_b_c_d"

    def test_no_dots(self):
        assert dots_to_underscores("no_dots_here") == "no_dots_here"

    def test_empty_string(self):
        assert dots_to_underscores("") == ""


class TestSnakeCase:
    """Tests for the snake_case helper."""

    def test_spaces(self):
        assert snake_case("Hello World") == "hello_world"

    def test_hyphens(self):
        assert snake_case("my-column-name") == "my_column_name"

    def test_dots(self):
        assert snake_case("my.column.name") == "my_column_name"

    def test_mixed_separators(self):
        assert snake_case("My Column.Name-here") == "my_column_name_here"

    def test_empty_string(self):
        assert snake_case("") == ""


class TestTruncate:
    """Tests for the truncate helper."""

    def test_within_limit(self):
        assert truncate("hello", 10) == "hello"

    def test_at_limit(self):
        assert truncate("hello", 5) == "hello"

    def test_exceeds_limit(self):
        assert truncate("hello world", 8) == "hello..."

    def test_custom_suffix(self):
        assert truncate("hello world", 8, suffix="~") == "hello w~"

    def test_invalid_max_length_raises(self):
        with pytest.raises(ValueError, match="max_length"):
            truncate("hello", 2, suffix="...")

    def test_empty_string(self):
        assert truncate("", 5) == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
