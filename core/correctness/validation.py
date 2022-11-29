
def check_input(variable, expected_type, or_none=False):
    """
    Checks if a given variable is of the expected type. Raises TypeError or
    ValueError as appropriate if any issues are encountered.

    :param variable: (any) variable to check type of

    :param expected_type: (type) expected type of the provided variable

    :param or_none: (optional) boolean of if the variable can be unset.
    Default value is False.

    :return: No return.
    """

    if not or_none:
        if not isinstance(variable, expected_type):
            raise TypeError(
                'Expected type was %s, got %s'
                % (expected_type, type(variable))
            )
    else:
        if not isinstance(variable, expected_type) \
                and not isinstance(variable, type(None)):
            raise TypeError(
                'Expected type was %s or None, got %s'
                % (expected_type, type(variable))
            )

def valid_string(variable, valid_chars):
    """
    Checks that all characters in a given string are present in a provided
    list of characters. Will raise an ValueError if unexpected character is
    encountered.

    :param variable: (str) variable to check.

    :param valid_chars: (str) collection of valid characters.

    :return: No return.
    """
    check_input(variable, str)
    check_input(valid_chars, str)

    for char in variable:
        if char not in valid_chars:
            raise ValueError(
                "Invalid character '%s'. Only valid characters are: "
                "%s" % (char, valid_chars)
            )
