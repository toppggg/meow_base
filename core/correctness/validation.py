
from abc import ABCMeta
from typing import Any, _SpecialForm

def check_input(variable:Any, expected_type:type, alt_types:list[type]=[], 
        or_none:bool=False)->None:
    """
    Checks if a given variable is of the expected type. Raises TypeError or
    ValueError as appropriate if any issues are encountered.

    :param variable: (any) variable to check type of

    :param expected_type: (type) expected type of the provided variable

    :param alt_types: (optional)(list) additional types that are also 
    acceptable

    :param or_none: (optional) boolean of if the variable can be unset.
    Default value is False.

    :return: No return.
    """

    type_list = [expected_type]
    type_list = type_list + alt_types

    if not or_none:
        if expected_type != Any \
                and type(variable) not in type_list:
            raise TypeError(
                'Expected type was %s, got %s'
                % (expected_type, type(variable))
            )
    else:
        if expected_type != Any \
                and not type(variable) not in type_list \
                and not isinstance(variable, type(None)):
            raise TypeError(
                'Expected type was %s or None, got %s'
                % (expected_type, type(variable))
            )

def valid_string(variable:str, valid_chars:str, min_length:int=1)->None:
    """
    Checks that all characters in a given string are present in a provided
    list of characters. Will raise an ValueError if unexpected character is
    encountered.

    :param variable: (str) variable to check.

    :param valid_chars: (str) collection of valid characters.

    :param min_length: (int) minimum length of variable.

    :return: No return.
    """
    check_input(variable, str)
    check_input(valid_chars, str)

    if len(variable) < min_length:
        raise ValueError (
            f"String '{variable}' is too short. Minimum length is {min_length}"
        )

    for char in variable:
        if char not in valid_chars:
            raise ValueError(
                "Invalid character '%s'. Only valid characters are: "
                "%s" % (char, valid_chars)
            )

def valid_dict(variable:dict[Any, Any], key_type:type, value_type:type, 
        required_keys:list[Any]=[], optional_keys:list[Any]=[], 
        strict:bool=True, min_length:int=1)->None:
    check_input(variable, dict)
    check_input(key_type, type, alt_types=[_SpecialForm, ABCMeta])
    check_input(value_type, type, alt_types=[_SpecialForm, ABCMeta])
    check_input(required_keys, list)
    check_input(optional_keys, list)
    check_input(strict, bool)

    if len(variable) < min_length:
        raise ValueError(f"Dictionary '{variable}' is below minimum length of "
            f"{min_length}")

    for k, v in variable.items():
        if key_type != Any and not isinstance(k, key_type):
            raise TypeError(f"Key {k} had unexpected type '{type(k)}' "
                f"rather than expected '{key_type}' in dict '{variable}'")
        if value_type != Any and not isinstance(v, value_type):
            raise TypeError(f"Value {v} had unexpected type '{type(v)}' "
                f"rather than expected '{value_type}' in dict '{variable}'")

    for rk in required_keys:
        if rk not in variable.keys():
            raise KeyError(f"Missing required key '{rk}' from dict "
                f"'{variable}'")
    
    if strict:
        for k in variable.keys():
            if k not in required_keys and k not in optional_keys:
                raise ValueError(f"Unexpected key '{k}' should not be present "
                    f"in dict '{variable}'")

def valid_list(variable:list[Any], entry_type:type,
        alt_types:list[type]=[], min_length:int=1)->None:
    check_input(variable, list)
    if len(variable) < min_length:
        raise ValueError(f"List '{variable}' is too short. Should be at least "
            f"of length {min_length}")
    for entry in variable:
        check_input(entry, entry_type, alt_types=alt_types)
