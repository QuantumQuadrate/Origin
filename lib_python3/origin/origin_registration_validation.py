"""
Function for making sure the stream registration conforms to the standard
"""

from origin import data_types
import string
from typing import Dict, List, Tuple


def simple_string(string_input: str) -> int:
    """
    Check for punctuation in the string, underscore is ok
    Args:
        string_input: string to be checked
    Returns:
        1 if string contains invalid characters
        0 if string is ok for use
    """
    invalid_chars = set(string.punctuation.replace("_", ""))
    if any(char in invalid_chars for char in string_input):
        return 1
    return 0


def registration_validation(
        stream_name: str,
        template: Dict[str: str],
        key_order: List[str] = None
) -> Tuple[bool, str]:
    """
    Making sure the registration conforms to the standard
    Returns True if the registration is valid

    Checks made:
        stream_name is valid (doesn't have punctuation except for '_')
        the data types outlined in template are recognized by origin
        field names outlined in template are valid (as in stream_name)
        all the field names template match the field names in key_order
    Args:
        stream_name : name of the stream to be registered
        template : dict of fields to be registered in the stream, and the data type of each field
        key_order : order in which fields will be registered. If none order is arbitrary

    Returns:
        valid, error_message:
            valid : true if the arguments are valid
            error_message : an error message related to the failure mode, '' if stream is valid
    """
    fields = template.keys()
    for field, data_type in template.items():
        try:
            data_types[data_type]
        except KeyError:
            msg = f"type '{data_type}' not recognized"
            return False, msg
        if simple_string(field) != 0:
            msg = f"Invalid field name: '{field}'"
            return False, msg
    if simple_string(stream_name) != 0:
        msg = f"Invalid stream name: '{stream_name}'"
        return False, msg

    if key_order is not None:
        if len(key_order) != len(fields):
            msg = "Fields and key_order do not have the same length."
            return False, msg
        for field in template:
            if field not in key_order:
                msg = f"Field `{field}` in registration is not present in key_order"
                return False, msg.format(field)

    return True, ''
