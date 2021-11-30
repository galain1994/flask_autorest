# -*- coding: utf-8 -*-
"""
FieldFilter 字段过滤器
Usage:
    - IncludeFilter
    - ExcludeFilter

* IncludeFilter: 从字典中获取所需要的字段

* ExcludeFilter: 从字典中剔除不需要的字段
  - 获取关系: 在字段后增加'.'
    例子: 'children.' 获取关系children 且全局过滤is_deleted字段
    ExcludeFilter(['children.', ], global_fields=['is_deleted'])
  - 剔除子项所有数据: 与普通字段相同 'children'
"""

from itertools import chain
from collections import defaultdict


__all__ = ['BaseFilter', 'ExcludeFilter', 'IncludeFilter']


class BaseFilter(object):

    _type = None

    def __init__(self, filter_list=None, name=None, global_fields=None):
        self.name = name
        # 过滤的字段
        self.fields = []
        # 下层过滤器
        self.filters = defaultdict(list)
        # 全局字段
        self.global_fields = global_fields or []
        # 层级关系的字段
        self.relations = defaultdict(list)
        if not filter_list:
            filter_list = []
        self._init_filter(filter_list)

    def _init_filter(self, _fields):
        for _field in _fields:
            if '.' in _field:
                relation, sub_field = _field.split('.', 1)
                self.relations[relation].append(sub_field)
            elif _field:
                self.fields.append(_field)
        for _relation, sub_fields in self.relations.items():
            _filter = type(self)(sub_fields, _relation, self.global_fields)
            self.filters[_relation].append(_filter)

    def __repr__(self):
        return f"Filter: {self.name} <{id(self)}>"

    __str__ = __repr__

    def _filter_dict(self, d):
        raise NotImplementedError()

    def filter(self, data):
        if not data:
            return data
        return self._filter_dict(data)


class ExcludeFilter(BaseFilter):

    _type = 'exclude'

    def recursive_filter(self, item):
        """ _type == 'exclude'时,递归删除全局字段 """
        assert isinstance(item, dict)

        pop_keys = []
        for k, v in item.items():
            if isinstance(v, (list, tuple)):
                for nested in v:
                    self.recursive_filter(nested)
            elif isinstance(v, dict):
                self.recursive_filter(v)
            if k in self.global_fields:
                pop_keys.append(k)
        for pop_key in pop_keys:
            item.pop(pop_key)

    def _filter_dict(self, d):
        """从字典中移除部分字段"""
        is_dict = False
        if isinstance(d, dict):
            d = (d, )
            is_dict = True
        for item in d:
            for _field in chain(self.fields, self.global_fields):
                # 移除指定的字段
                item.pop(_field, None)
            for relation, _filters in self.filters.items():
                if relation in item:
                    for _filter in _filters:
                        _filter.filter(item[relation])
            self.recursive_filter(item)
        return d[0] if is_dict else d


class IncludeFilter(BaseFilter):

    _type = 'include'

    def _filter_dict(self, d):
        """ 从字典中挑出部分字段 """
        is_dict = False
        ret = []
        extended_fields = list(chain(self.fields,
                                     self.global_fields,
                                     self.relations.keys()))
        if isinstance(d, dict):
            d = (d, )
            is_dict = True
        for item in d:
            data = {}
            for _field in extended_fields:
                if _field in item:
                    data[_field] = item[_field]
            for relation, sub_filters in self.filters.items():
                if relation in data:
                    for sub_filter in sub_filters:
                        data[relation] = sub_filter.filter(data[relation])
            ret.append(data)
        return ret[0] if is_dict else ret
