# -*- coding: utf-8 -*-


from collections import defaultdict
from io import BytesIO, StringIO
import csv
import xlsxwriter


DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_DATE_FORMAT = "%Y-%m-%d"


def export_excel(enclasped: (list, tuple), file_obj=None):
    """Export dictionary data into bytes-io file

    :param enclasped: list of dictionary
        that include headers, data, and sheet name
    :type enclasped: list, tuple
    :return: byte-io file
    :rtype: BytesIO
    """
    if not file_obj:
        file_obj = BytesIO()
    book = xlsxwriter.Workbook(file_obj, {'strings_to_numbers': False})
    for row_data in enclasped:
        sheet = book.add_worksheet()
        for row, item in enumerate(row_data):
            sheet.write_row(row, 0, item)
    book.close()
    file_obj.seek(0)
    return file_obj


def export_csv(row_data, file_obj=None):
    """Export data to csv bytes-io file

    :param data: [description]
    :type data: [type]
    """
    if not file_obj:
        file_obj = StringIO()
    csv_writer = csv.writer(file_obj)
    csv_writer.writerows(row_data)
    file_obj.seek(0)
    return file_obj


def dict2lines(records: (list, tuple), upper_key=None):
    """ Convert record list to defaultdict list, which line shows
    """
    ret = []
    for item in records:
        data = defaultdict(str)
        other_lines = []
        for key, value in item.items():
            if upper_key:
                key = '__'.join((upper_key, key))
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    new_sub_key = '__'.join((key, sub_key))
                    data[new_sub_key] = sub_value
            elif isinstance(value, (list, tuple)):
                details = dict2lines(value, key)
                if details:
                    line_one = details.pop(0)
                    data.update(line_one)
                    other_lines.extend(details)
            else:
                data[key] = value
        if data:
            ret.append(data)
        if other_lines:
            ret.extend(other_lines)
    return ret


def lines2export(lines):
    """ Convert dict-lines to list-lines
    >>> d = [{
    ...    'a': '*',
    ...     'b': {
    ...         'b1': '-',
    ...         'b2': '-'
    ...     },
    ...     'c': [{
    ...         'c1': '#',
    ...         'c2': '#'
    ...     }, {
    ...         'c1': '+',
    ...         'c2': '+'
    ...     }]
    ... }, {
    ...     'a': '**',
    ...     'b': {
    ...         'b1': '--',
    ...         'b2': '--',
    ...     },
    ...     'c': None
    ... }]
    >>> lines = dict2lines(d)
    >>> lines2export(lines)
    [
        ['a', 'b__b1', 'b__b2', 'c', 'c__c1', 'c__c2'],
        ['*', '-', '-', '', '#', '#'],
        ['', '', '', '', '+', '+'],
        ['**', '--', '--', None, '', '']
    ]
    """
    headers = sorted(set(
        k
        for line in lines
        for k in line.keys()
    ))
    if 'id' in headers:
        id_index = headers.index('id')
        headers.pop(id_index)
        headers.insert(0, 'id')
    ret = [headers]
    for line in lines:
        ret.append(
            [line[head] for head in headers]
        )
    return ret


def export2file(data, fmt='csv'):
    """

    :param data: [description]
    :type data: [type]
    :param fmt: [description], defaults to 'csv'
    :type fmt: str, optional
    """
    if 'csv' == fmt:
        return export_csv(data)
    if 'excel' == fmt:
        return export_excel((data, ))
    raise ValueError("Invalid format")


def archive(files, format='zip'):
    """ 压缩 """
    pass


if __name__ == "__main__":
    import doctest
    doctest.testmod()
