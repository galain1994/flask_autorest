# -*- coding: utf-8 -*-
"""
Serialize & Deserialize
"""

from functools import partial
from collections import defaultdict
from sqlalchemy.schema import Table
from sqlalchemy.inspection import inspect as sqla_inspect
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.attributes import InstrumentedAttribute
from .errors import ValidationError, DeserializeError
from .utils import try_custom_delete

#: Types which should be considered columns of a model when iterating over all
#: attributes of a model class.
COLUMN_TYPES = (InstrumentedAttribute, hybrid_property)


def default_converter(x):
    return x


def serializer(filters=None, type_converters=None, column_converters=None):
    return partial(serialize, filters=filters,
                   type_converters=type_converters,
                   column_converters=column_converters)


def deserializer(filters=None, validators=None, column_converters=None):
    return partial(deserialize, filters=filters,
                   validators=validators,
                   column_converters=column_converters)


def serialize(instance, filters=None,
              type_converters=None,
              column_converters=None):
    """ 序列化: object -> dict
    [ATTENSION] 先根据类型转化, 再转化具体字段
    :param instance: 查询出来的对象
    :param filters: Filter对象的列表
    :param type_converters: convert function map
           根据类型转化函数映射,使用type进行类型判断 [active for children as well]
           e.g {datetime: convert_dt }
    :param column_converters: convert function map with specific column
           特定表的特定字段的函数映射,指定对应的表和对应的字段 [active for children as well]
           e.g {'<table_a>.<column_a>': function_a}
    :return dict
    """
    if not instance:
        return {}
    if not filters:
        filters = []
    default_type_converters = defaultdict(lambda: default_converter)
    if type_converters:
        default_type_converters.update(type_converters)
    if not column_converters:
        column_converters = {}

    data = {}
    custom_attrs = sqla_inspect(instance.__class__).all_orm_descriptors
    for attr_key, attr_val in custom_attrs.items():
        if getattr(attr_val, 'property', None) and \
                'relationship' == getattr(attr_val.property, 'strategy_wildcard_key', None):
            continue
        if attr_val._is_internal_proxy:
            continue
        data[attr_key] = getattr(instance, attr_key)
    for col_name, column in instance.__table__.columns.items():
        value = getattr(instance, col_name, None)
        # 根据特定字段进行转化的key
        column_convert_key = '{table}.{column}'.format(
            table=instance.__tablename__,
            column=col_name
        )
        # 根据特定类型进行转换value
        data[col_name] = default_type_converters[type(value)](value)
        if column_convert_key in column_converters:
            data[col_name] = column_converters[column_convert_key](
                data[col_name])
    for _filter in filters:
        # 过滤自身的字段
        data = _filter.filter(data)
        # 过滤关联模型的字段
        for relation, sub_filters in _filter.filters.items():
            relate_instances = getattr(instance, relation, None)
            if not relate_instances:
                continue
            if isinstance(relate_instances, (tuple, list)):
                # many2many, one2many
                data[relation] = [
                    serialize(rel_instance, sub_filters,
                              type_converters=type_converters,
                              column_converters=column_converters)
                    for rel_instance in relate_instances
                ]
            else:
                # many2one
                data[relation] = serialize(relate_instances, sub_filters,
                                           type_converters=type_converters,
                                           column_converters=column_converters)
    return data


def deserialize(data, model, session, instance_id=None, primary_key='id',
                filters=None, validators=None, column_converters=None):
    """ 反序列化: dict -> object(尚未在数据库创建)

    :param data: input dict
    :param model: sqlalchemy model
    :param validators: map of functions: to validate input value
    :param column_converters: map of functions: convert input value
        e.g { <table_name>.<column_name>: convert_function }
    :return instance
    [reference restless]
    """
    if not column_converters:
        column_converters = {}

    if not validators:
        validators = []

    if not filters:
        filters = []

    for _filter in filters:
        data = _filter.filter(data)

    # 检测该模型是否有对应的字段
    inspected_model = sqla_inspect(model)
    descriptors = inspected_model.all_orm_descriptors._data
    for key, value in data.items():
        if (key in descriptors and not getattr(descriptors[key], 'fset', None)) and \
                not hasattr(model, key):
            raise ValidationError(f"{model} does not have field {key}")

    for validator in validators:
        validator(data)

    instance_data = {}
    cols = get_columns(model)
    relations = get_relations(model)
    data_keys = data.keys()

    instance_columns = set(cols.keys()).intersection(data_keys).difference(relations.keys())
    for col_name in instance_columns:
        # 根据converters转化value
        convert_key = f"{model.__tablename__}.{col_name}"
        if convert_key in column_converters:
            instance_data[col_name] = column_converters[convert_key](data[col_name])
        else:
            instance_data[col_name] = data[col_name]

    # 删除主键值
    instance_id = instance_id or instance_data.pop(primary_key, None)
    if instance_id:
        instance = session.query(model) \
            .filter(getattr(model, primary_key) == instance_id) \
            .first()
        if not instance:
            raise DeserializeError(f"Cannot find record with {primary_key} equals to {instance_id}")
        set_attributes(instance, **instance_data)
    else:
        try:
            instance = model(**instance_data)
        except Exception as e:
            raise DeserializeError(e)

    # relations
    # 传入的关系值
    exist_relation_keys = set(relations.keys()).intersection(data_keys)
    for col in exist_relation_keys:
        submodel = relations[col]
        subdata = data[col]
        attr = inspected_model.attrs[col]
        old_data = getattr(instance, col, None)
        if isinstance(old_data, list):
            # 更新的数据
            new_attribute = set()
            # many2many, one2many
            old_set = set(old_data)
            for sub_item_data in subdata:
                sub_inst = get_or_create_instance(
                    session, submodel, sub_item_data, column_converters)
                new_attribute.add(sub_inst)
            new_attribute = list(new_attribute)
            # 将要删除的对象
            to_remove = [old_obj for old_obj in old_set if old_obj not in new_attribute]
            if not isinstance(attr.secondary, Table):
                # one2many 需要删除对象
                for _obj in to_remove:
                    try_custom_delete(_obj, session)
        else:
            new_attribute = get_or_create_instance(
                session, submodel, subdata, column_converters)
        setattr(instance, col, new_attribute)
    return instance


def get_or_create_instance(session, model, data, column_converters=None):
    """创建或者获取对象 reference restless"""
    if not isinstance(data, dict):
        return data
    if not column_converters:
        # 默认返回原值
        column_converters = {}
    # 关联模型: recursive:
    relations = get_relations(model)
    for rel in set(relations.keys()).intersection(data.keys()):
        # related model
        sub_model = relations[rel]
        if isinstance(data[rel], dict):
            # many2one
            data[rel] = get_or_create_instance(
                session, sub_model, data[rel], column_converters)
        else:
            # many2many, one2many
            data[rel] = [
                get_or_create_instance(session, sub_model, sub_data, column_converters)
                for sub_data in data[rel]
            ]

    for k, v in data.items():
        # 转化值
        convert_key = f'{model.__tablename__}.{k}'
        if convert_key in column_converters:
            data[k] = column_converters[convert_key](data[k])

    # 根据定义的主键获取主键列表
    primary_keys = get_primary_key(model)
    # 所有主键都在data中
    if all(k in data for k in primary_keys):
        # 主键值 用于搜索已存在的数据
        pk_values = {}
        for k in primary_keys:
            pk_values[k] = data[k]

        instance = session.query(model).filter_by(**pk_values).first()
        if instance:
            # existed, changed by given data
            set_attributes(instance, **data)
            return instance
    return model(**data)


def get_columns(model):
    """Returns a dictionary-like object containing all the columns of the
    specified `model` class.

    This includes `hybrid attributes`_.

    .. _hybrid attributes: http://docs.sqlalchemy.org/en/latest/orm/extensions/hybrid.html

    """
    columns = {}
    for superclass in model.__mro__:
        for name, column in superclass.__dict__.items():
            if isinstance(column, COLUMN_TYPES):
                columns[name] = column
    return columns


def get_relations(model):
    """
    Get all relations of a model 修改restless的返回, 改成字典
    :return: {relation_name: Model}
    """
    relations = {}
    for rel_name, relationships in sqla_inspect(model).relationships.items():
        relations[rel_name] = relationships.mapper.class_
    return relations


def get_primary_key(model):
    """Returns all the primary keys for a model.
    [copy from restless: primary_key_names]
    """
    return [column.name
            for column in sqla_inspect(model).columns
            if column.primary_key]


def set_attributes(instance, **kwargs):
    """设置字段值 reference restless"""
    for key, value in kwargs.items():
        if hasattr(instance, key):
            setattr(instance, key, value)
