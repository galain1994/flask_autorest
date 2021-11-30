# -*- coding: utf-8 -*-


class AutoBaseException(Exception):

    pass


class ResourceNotFoundException(AutoBaseException):
    """ 资源未找到 """
    code = 101
    pass


class BlueprintExists(AutoBaseException):
    """ 蓝图已存在 """
    code = 102
    pass


class ValidationError(AutoBaseException):
    """ 参数校验错误 """
    code = 103
    pass


class DeserializeError(AutoBaseException):
    """ 反序列化错误 """
    code = 104
    pass


class RelationshipError(AutoBaseException):
    """ 关系未找到 """
    code = 105
    pass


class RelateObjectError(AutoBaseException):
    """ 关联对象不能获取 """
    code = 106
    pass
