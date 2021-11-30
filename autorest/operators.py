# -*- coding: utf-8 -*-

__all__ = ['OPERATORS', 'parse_filter']


def is_null(field, value):
    return field == None


def is_not_null(field, value):
    return field != None


def eq(field, value):
    return field == value


def ne(field, value):
    return field != value


def gt(field, value):
    return field > value


def ge(field, value):
    return field >= value


def lt(field, value):
    return field < value


def le(field, value):
    return field <= value


def ilike(field, value):
    return field.ilike("%" + value + "%")


def like(field, value):
    return field.contains(value)


def notlike(field, value):
    return field.notlike("%s" + value + "%s")


def notilike(field, value):
    return field.notilike("%s" + value + "%s")


def in_(field, value):
    return field.in_(value)


def not_in(field, value):
    return ~field.in_(value)


def any_has(relation, argument):
    sub_fieldname = argument['name']
    operator = argument['op']
    sub_argument = argument['val']
    sub_model = relation.property.mapper.class_
    if relation.property.uselist:
        func = getattr(relation, 'any')
    else:
        func = getattr(relation, 'has')
    return func(parse_filter(sub_model, sub_fieldname, operator, sub_argument))


OPERATORS = {
    'is_null': is_null,
    'is_not_null': is_not_null,
    'eq': eq,
    'ne': ne,
    'gt': gt,
    'ge': ge,
    'lt': lt,
    'le': le,
    'ilike': ilike,
    'like': like,
    'notlike': notlike,
    'notilike': notilike,
    'in': in_,
    'not_in': not_in,
    'has': any_has,
    'any': any_has,
}


def parse_filter(model, fieldname, operator, argument):
    opfunc = OPERATORS[operator]
    field = getattr(model, fieldname)
    return opfunc(field, argument)
