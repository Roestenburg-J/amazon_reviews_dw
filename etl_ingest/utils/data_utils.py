import re
import ast


def convert_value(value):
    """Safely convert string representations of Python structures."""
    try:
        return ast.literal_eval(value)  # Converts dicts, lists, tuples safely
    except (ValueError, SyntaxError):
        return value  # Return as-is if conversion fails


def sanitize_string(value):
    """Function that replaces problimatic characters in strings."""
    replacements = {
        "=\ ": "__EQ_BACKSLASH__",
        "\\": "__BACKSLASH__",
        "'": "__SINGLE_QUOTE__",
        '"': "__DOUBLE_QUOTE__",
        "\n": "__NEWLINE__",
        "\r": "__CARRIAGE_RETURN__",
        "\t": "__TAB__",
    }

    # Replace values
    for char, replacement in replacements.items():
        value = value.replace(char, replacement)

    # Remove spaces that could be interpreted as tabs
    value = re.sub(r"\s{2,}", " ", value)

    return value


def restore_string(sanitized_value):

    replacements = {
        "__EQ_BACKSLASH__": "=\ ",
        "__BACKSLASH__": "\\",
        "__SINGLE_QUOTE__": "'",
        "__DOUBLE_QUOTE__": '"',
        "__NEWLINE__": "\n",
        "__CARRIAGE_RETURN__": "\r",
    }

    for replacement, char in replacements.items():
        sanitized_value = sanitized_value.replace(replacement, char)

    return sanitized_value
