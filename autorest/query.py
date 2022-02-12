# -*- coding: utf-8 -*-
"""

Purpose:
- Equality search: key = value 等值搜索
- In search: key = value1 & key = value2 存在性搜索
- Negation search: key != value 否定性搜索
- Like search: key like value
- Or search: key1 = value1 or key2 = value2
- And search: key > value1 and key < value2

* 分页
* 排序
* 限制数量

"""

import json
import base64
from sqlalchemy import and_, or_, desc, asc
from sqlalchemy.sql.elements import ClauseElement
from flask import request
from .operators import parse_filter


FIXED_ARGS = ('format', 'q', 'order', 'limit', 'offset', 'pageNo', 'pageSize', 'fields')


def create_filter(model, filter_dict):
    if 'or' in filter_dict:
        _or_clause = []
        subfilters = filter_dict.get('or') or []
        for d in subfilters:
            _filter = create_filter(model, d)
            if isinstance(_filter, ClauseElement):
                _or_clause.append(_filter)
        return or_(*_or_clause)
    if 'and' in filter_dict:
        _and_clause = []
        subfilters = filter_dict.get('and') or []
        for d in subfilters:
            _filter = create_filter(model, d)
            if isinstance(_filter, ClauseElement):
                _and_clause.append(_filter)
        return and_(*_and_clause)
    filedname = filter_dict.get('name')
    operator = filter_dict.get('op')
    argument = filter_dict.get('val')
    if not filedname:
        return None
    return parse_filter(model, filedname, operator, argument)


def parse_domain(domains):
    """转化domain"""
    try:
        domains = json.loads(domains)
    except json.JSONDecodeError:
        return None
    if domains:
        return [
            {
                "name": _domain[0],
                "op": _domain[1],
                "val": _domain[2],
            } for _domain in domains
        ]


def parse_old_school():
    conditions = []
    for k, v in request.args.items():
        if k in FIXED_ARGS or v == '':
            continue
        if 'domains' == k:
            domains = parse_domain(v)
            if domains:
                conditions.extend(domains)
        else:
            conditions.append({
                "name": k,
                "op": "eq",
                "val": v
            })
    if conditions:
        return json.dumps({"and": conditions})
    return "{}"


def search(session, model, q=None, format='json'):
    """ Search model by query dictionary
    :param session: database connection session
    :type session: `session` is sqlalchemy session
    :param model: class of sqlalchemy model
    :type model: class
    :param search_params: search dictionary
    :type search_params: dict
    :return: `Query` object
    """
    if not q or "{}" == q:
        q = parse_old_school()
    if 'base64' == format:
        q = base64.b64decode(q)
    q_json = json.loads(q)
    query = parse_query(session, model, q_json)
    return query


def parse_query(session, model, q):
    """
    filtering
    :param q: list of query dictionary
    :type q: list
    """
    query = session.query(model)
    if q and isinstance(q, dict):
        filters = [create_filter(model, q)]
    else:
        filters = [create_filter(model, filt) for filt in q if filt]
    query = query.filter(*filters)
    return query


def get_objects(model, query, order=None, limit=50, offset=0):
    """ 根据搜索条件获取对象
    :param order: format: field1.asc,field2,field3.desc
    :type order: str
    :param limit: page size
    :type limit: int
    :param offset: offset of the first record
    :type offset: int
    Building the query proceeds in this order:
        1. ordering
        2. limiting
        3. offsetting
    """
    # 顺序
    if not order:
        order = ''
    criterions = [criterion.split('.') for criterion in str(order).split(',') if criterion]
    for fieldname_direction in criterions:
        if 1 == len(fieldname_direction):
            fieldname = fieldname_direction[0]
            direction = 'asc'
        else:
            fieldname, direction = fieldname_direction
        field = getattr(model, fieldname, None)
        if not field:
            continue
        if 'desc' == direction:
            query = query.order_by(desc(field))
        else:
            query = query.order_by(asc(field))
    # limit
    if limit:
        query = query.limit(limit)
    # offset
    if offset:
        query = query.offset(offset)
    return query.all()
