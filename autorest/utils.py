# -*- coding: utf-8 -*-

import json
from datetime import datetime
from functools import wraps
from werkzeug.exceptions import BadRequest
from flask import request, current_app
from flask.json import jsonify
from .errors import AutoBaseException
from decimal import Decimal


OBJECT_CUSTOM_DELETE = 'delete'
DEFAULT_ERROR_CODE_MESSAGE = {
    101: {
        "msg": "Resource not found",
        "msg_cn": "资源未找到",
    },
    102: {
        "msg": "BluePrint already existed",
        "msg_cn": "蓝图已存在",
    },
    103: {
        "msg": "Data validation error",
        "msg_cn": "数据校验错误",
    },
    104: {
        "msg": "Deserialize error",
        "msg_cn": "反序列化错误"
    },
    105: {
        "msg": "Relationship not found",
        "msg_cn": "关系未找到"
    },
    106: {
        "msg": "Relate object error",
        "msg_cn": "关联对象未找到"
    }
}


def unpack(value):
    """Return a three tuple of data, code, and headers"""
    if not isinstance(value, tuple):
        return value, 200, {}

    try:
        data, code, headers = value
        return data, code, headers
    except ValueError:
        pass

    try:
        data, code = value
        return data, code, {}
    except ValueError:
        pass

    return value, 200, {}


def json_response(view):
    """ 作为最里层的装饰器.
    将相应的view执行的结果转换为application/json返回
    """
    @wraps(view)
    def decorate(*args, **kwargs):
        ret = view(*args, **kwargs)
        return jsonify(ret)
    return decorate


def _is_msie8or9():
    """ 判断客户端是否是IE8/IE9 从restless框架复制过来
    Returns ``True`` if and only if the user agent of the client making the
    request indicates that it is Microsoft Internet Explorer 8 or 9.

    .. note::

       We have no way of knowing if the user agent is lying, so we just make
       our best guess based on the information provided.

    """
    # request.user_agent.version comes as a string, so we have to parse it
    def version(ua):
        return tuple(int(d) for d in ua.version.split('.'))

    return request.user_agent is not None \
        and request.user_agent.version is not None \
        and request.user_agent.browser == 'msie' \
        and (8, 0) <= version(request.user_agent) < (10, 0)


def try_load_json_data(func):
    @wraps(func)
    def decorate(*args, **kwargs):
        content_type = request.headers.get('Content-Type', None)
        content_is_json = content_type.startswith('application/json')
        is_msie = _is_msie8or9()
        if not is_msie and not content_is_json:
            msg = 'Request must have "Content-Type: application/json" header'
            return dict(msg=msg, msg_cn="请求头需包含: \"Content-Type: application/json\""), 415

        try:
            if is_msie:
                data = json.loads(request.get_data()) or {}
            else:
                data = request.get_json() or {}
        except (BadRequest, TypeError, ValueError, OverflowError) as exception:
            current_app.logger.exception(str(exception))
            return dict(msg='Unable to decode data', msg_cn="json decode错误"), 400
        kwargs['json_data'] = data
        return func(*args, **kwargs)
    return decorate


def try_custom_delete(instance, session):
    """ 先尝试软删除 """
    custom_delete_method = getattr(instance, OBJECT_CUSTOM_DELETE)
    if custom_delete_method:
        custom_delete_method(instance)
    else:
        session.delete(instance)
    return True


def default_rest_decorator(func):
    @wraps(func)
    def decorate(*args, **kwargs):
        return {
            'data': func(*args, **kwargs),
            'code': 200,
            'success': True,
        }
    return decorate


def default_method_decorator(func):
    @wraps(func)
    def decorate(*args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except AutoBaseException as e:
            code = getattr(e, 'code')
            result = DEFAULT_ERROR_CODE_MESSAGE.get(code) or {}
            if result:
                result['code'] = code
            result['data'] = None
            return result
        return ret
    return decorate


def format_time(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S"):
    return dt.strftime(fmt)


default_type_converters = {
    datetime: format_time,
    Decimal: float
}
