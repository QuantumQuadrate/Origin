"""
Function for making sure the stream registration conforms to the standard
"""

from origin import data_types
import string


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

# should check that the stream name and field names do not have punctuation (except for '_')
# should check that the data_types are recognized
# check that the key_order has the same length as the number of fields
def registration_validation(stream, template, key_order):
    '''making sure the regsitration conforms to the standard
    Returns True if the registration is valid
    '''
    fields = template.keys()
    for f in fields:
        try:
            data_types[template[f]]
        except KeyError:
            msg = "type {} not recognized".format(template[f])
            return (False, msg)
        if simple_string(f) != 0:
            msg = "Invalid field name: {}".format(f)
            return (False, msg)
    if simple_string(stream) != 0:
        msg = "Invalid stream name: {}".format(stream)
        return (False, msg)

    if key_order is not None:
        if len(key_order) != len(fields):
            msg = "Fields and key_order do not have the same length."
            return (False, msg)
        for key in template:
            if not key in key_order:
                msg = "Field `{}` in registration is not present in key_order"
                return (False, msg.format(key))

    return (True, '')
