from typing import Any, Dict

import json


def city_and_resident_codes(line: str) -> Dict[str, Any]:
    """
    Extract the region of birth and region of residence from the SAS file
    :param line: A line in the file
    :return: The parsed line.
    """
    stack, results = [], {'code': None, 'region': None, 'valid': False}
    i = 0
    while i < len(line):
        if line[i].isdigit():  # i94code
            while line[i].isdigit():
                stack.append(line[i])
                i += 1
            results['code'] = (int(''.join(stack)))
            stack = []
        elif line[i] == "'":  # beginning of value
            i += 1  # Set i to the beginning of the string value, not the quote marks
            while i < len(line):
                stack.append(line[i])
                i += 1
            parsed_line = ''.join(stack)
            stack = []
            if 'INVALID' in parsed_line:
                idx = parsed_line.index(":") + 2  # Skip the space as well
                # country, is_valid_entry = parsed_line[idx:], False
                # results['region'] = parsed_line[idx:]
                parsed_line = parsed_line[idx:]
            if 'No Country Code' not in parsed_line:
                parsed_line = ' '.join([word.lower().capitalize() for word in parsed_line.split(' ')])
                if parsed_line[-1] == "'":
                    parsed_line = parsed_line[0:len(parsed_line) - 1]
                print(parsed_line)
                results['region'] = parsed_line
                results['valid'] = True
        else:
            i += 1
    return results


def port_code(line: str) -> Dict[str, str]:
    """
    Extract all of the port codes that correspond with ports of entry.
    :param line: A line in the SAS file
    :return: The parsed line.
    """
    stack, results = [], []
    for char in line:
        if char == "'" and not stack:
            stack.append(char)
        elif char == "'" and stack:
            while stack[-1] == " ":
                stack.pop()
            results.append(''.join(stack[1:]))
            stack = []
        elif char == " " and stack:
            stack.append(char)
        elif char != " " and char != "\t" and char != "=":
            stack.append(char)
    value = results.pop()
    if "No PORT Code" in value:
        results.extend([None, None])
    else:
        value = value.split(',')
        if len(value) == 2:
            municipality, region = value
        elif len(value) == 1:
            municipality, region = value[0], None
        else:
            municipality, region = value[0], value[-1]
        if region:
            region = region if region[0] != ' ' else region[1:]
            if len(region) == 2:  # US State
                region = region.upper()
            else:
                region = ' '.join([word.lower().capitalize() for word in region.split(' ')])
        municipality = ' '.join([word.lower().capitalize() for word in municipality.split(' ')])
        results.extend([municipality, region])
    return {'code': results[0], 'municipality': results[1], 'region': results[2]}


def main() -> None:
    """
    Parse the file 'I94_SAS_Labels_Descriptions.SAS' and place desired fields into separate JSON files.

    This file should only be run in the same directory as i94_SAS_Labels_Description.SAS
    :return: Nothing
    """
    json_dump = {
        'i94port': [],
        'i94cit_and_i94res': [],
        'i94visa': [{'code': 1, 'type': 'Business'}, {'code': 2, 'type': 'Pleasure'}, {'code': 3, 'type': 'Student'}],
        'i94mode': {'1': 'Air', '2': 'Sea', '3': 'Land', '9': 'Not reported'}
    }
    with open('I94_SAS_Labels_Descriptions.SAS') as f:
        for i, line in enumerate(f):
            line = line.rstrip()
            if 9 <= i <= 297:  # subtract 1 from official line number
                json_dump['i94cit_and_i94res'].append((city_and_resident_codes(line)))
            elif 302 <= i <= 961:
                json_dump['i94port'].append(port_code(line))
    for key in json_dump:
        if key != 'i94visa':
            continue
        file = key + ".json"
        with open(file, 'w') as f:
            json.dump(json_dump[key], f, indent=4)
    with open('I94_SAS_Labels_Description.json', 'w') as f:
        json.dump(json_dump, f, indent=4)


if __name__ == '__main__':
    exit()
