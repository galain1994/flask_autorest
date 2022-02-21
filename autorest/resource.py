# -*- coding: utf-8 -*-
"""
资源: Model的MethodView定义
"""


import logging
import traceback
from functools import partial
from collections import OrderedDict, defaultdict
from flask import request, Response
from flask.views import MethodView, MethodViewType
from werkzeug.wrappers import Response as ResponseBase
from .query import search, get_objects
from .errors import ResourceNotFoundException, RelationshipError, RelateObjectError, DeserializeError
from .serializer import get_relations, get_primary_key
from .export import dict2lines, lines2export, export2file
from .import_data import guess_file_format, load_upload_file, import2lines, \
    lines2root, deserialize_root
from .fieldfilter import ExcludeFilter, IncludeFilter
from .utils import unpack, try_load_json_data, json_response, try_custom_delete
try:
    from collections.abc import Mapping
except ImportError:
    from collections import Mapping


logger = logging.getLogger(__name__)
DEFAULT_METHOD = frozenset(('GET', ))


def Resource(name, representations=None, rest_decorators=None, method_decorators=None):
    """ 创建自定义的资源类
    用法: e.g

    def get_decorator(func):
        @wraps(func)
        def decorate(*args, **kwargs):
            ret = func(*args, **kwargs)
            return {'result': ret, 'code': 200}
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
            except Exception as e:
                return {
                    'code': 500,
                    'data': None,
                    'msg': str(e)
                }
            return ret
        return decorate

    CustomResource = Resource('CustomResource', rest_decorators={
        'get': get_decorator,
    }, method_decorators=(default_method_decorator, ))
    其中, rest_decorator是在相应的函数加上的装饰器,
    method_decorator

    :param name: class name
    :param representations: 展示函数, 针对不同的媒体类型的不同解析方法
    :param rest_decorators: 不同函数不同的装饰器 e.g {'get': get_decorator}
    :param method_decorators: 全局方法装饰器，如错误解析, 或格式化返回数据
    :return ModelResource: class
    """
    kw = {
        'representations': representations,
        'rest_decorators': rest_decorators,
        'method_decorators': method_decorators
    }
    return ResourceMeta(name, (ModelResource, ), kw)


class ResourceMeta(MethodViewType):
    """
    用于基础资源类定义的元类, 用法可参照Resource.__doc__
    """

    def __new__(cls, name, bases, kwargs):
        method_decorators = kwargs.pop('method_decorators', tuple())
        new_cls = super(ResourceMeta, cls).__new__(cls, name, bases, kwargs)
        if 'rest_decorators' in kwargs:
            # method: decorator Mapper, 一般用于自定义格式输出
            for method, decorator in kwargs['rest_decorators'].items():
                # 针对特定的方法增加装饰器
                func = getattr(new_cls, method, None)
                if func:
                    setattr(new_cls, method, decorator(func))
        if method_decorators and hasattr(new_cls, 'method_decorators'):
            new_cls.method_decorators = method_decorators + new_cls.method_decorators
        return new_cls


class ModelResource(MethodView):

    representations = None
    method_decorators = (json_response, )

    def __init__(self, model, session_fac,
                 serializer, deserializer,
                 allow_methods=None, primary_key='id', name=None,
                 *args, **kwargs):
        """
        Create Resource from model, also create blueprints and views.
        Create URLs with RESTFul-style.
        Convert model object to dictionary data,
        or convert dictionary data to records in database.

        :param model: model of sqlalchemy
        :param session: database session
        :param serializer: serializer
        :param deserializer: deserializer
        :param allow_methods: allowed http method, to create views
        :param primary_key: specify which key to deserialize
        :type primary_key: str, [optional] split by comma if multi
        :param name: resource name
        """
        super(ModelResource, self).__init__(*args, **kwargs)
        self.model = model
        if not name:
            name = model.__tablename__
        self.name = name
        self.session_fac = session_fac
        self.serializer = serializer
        self.deserializer = partial(deserializer, model=self.model)
        if allow_methods:
            self.allow_methods = frozenset((m.upper() for m in allow_methods))
        else:
            self.allow_methods = DEFAULT_METHOD
        self.primary_key = primary_key
        self.relationships = get_relations(self.model)
        self.relate_serializer = defaultdict(lambda: serializer)
        if kwargs.get('relate_serializer'):
            self.relate_serializer.update(kwargs['relate_serializer'])

    def dispatch_request(self, *args, **kwargs):
        """ Copy from flask """
        # noinspection PyUnresolvedReferences
        meth = getattr(self, request.method.lower(), None)
        if meth is None and request.method == 'HEAD':
            meth = getattr(self, 'get', None)
        assert meth is not None, 'Unimplemented method %r' % request.method
        session = self.session_fac()
        kwargs.update(session=session)

        if isinstance(self.method_decorators, Mapping):
            decorators = self.method_decorators.get(request.method.lower(), [])
        else:
            decorators = self.method_decorators

        for decorator in decorators:
            meth = decorator(meth)

        try:
            resp = meth(*args, **kwargs)
        except Exception as e:
            session.rollback()
            raise e

        if isinstance(resp, ResponseBase):  # There may be a better way to test
            return resp

        representations = self.representations or OrderedDict()

        # noinspection PyUnresolvedReferences
        mediatype = request.accept_mimetypes.best_match(representations, default=None)
        if mediatype in representations:
            data, code, headers = unpack(resp)
            resp = representations[mediatype](data, code, headers)
            resp.headers['Content-Type'] = mediatype
            return resp

        return resp

    def list(self, model, q, order, limit, offset, format, session):
        """ List Records """
        # sqlalchemy query object
        query = search(session, model, q, format)
        total = query.count()
        instances = get_objects(model, query, order, limit, offset)
        if offset + limit < total:
            has_next = True
        else:
            has_next = False
        return total, has_next, instances

    def get_inst_by_primary_key(self, value, model=None, **kwargs):
        """ 根据主键获取对象
        model == None 时, 根据当前resource定义的主键获取数据
        model指定时, 根据模型定义的主键获取数据
        value为多主键时, 才有,分割不同key
        >>> value = "order_id=6542341,shop_id=3212"
        >>> kvs = value.split(',')
        >>> pk_values = dict([kv.split('=') for kv in kvs])
        {
            "order_id": '6542341',
            "shop_id": '3212',
        }
        """
        session = kwargs['session']
        # 主键的keys
        if not model:
            model = self.model
            pks = self.primary_key.split(',')
        else:
            pks = get_primary_key(model)
        # 主键key: 主键value
        if '=' in value:
            kvs = value.split(',')
            pk_values = dict([kv.split('=') for kv in kvs])
        else:
            pk_values = dict.fromkeys(pks, value)
        return session.query(model).filter(*[
            getattr(model, pk) == pk_values.get(pk) for pk in pks
        ]).first()

    def get(self, instid=None, relationname=None, relateid=None, **kwargs):
        """ Get Record 
        instid == None: 返回全部
        instid != None: 
            relationname == None: 返回当前对象的数据
            relationname != None:
                relateid == None: 返回当前对象的当前relation所有对象的数据
                relateid != None: 当指定relation_object与当前对象有关联才返回
        """
        session = kwargs['session']
        if not instid:
            # 返回全部对象的数据
            format = request.args.get('format') or 'json'
            q = request.args.get('q') or "{}"
            order = request.args.get('order')
            limit = int(request.args.get('limit') or request.args.get('pageSize') or 50)
            offset = int(request.args.get('offset') or 0)
            if not offset:
                page = int(request.args.get('pageNo') or 1)
                if page < 1:
                    page = 1
                offset = (page-1) * limit
            total, has_next, instances = self.list(self.model, q, order, limit, offset, format, session)
            return {'total': total, 'has_next': has_next, 'limit': limit, 'offset': offset,
                    'result': [self.serializer(inst) for inst in instances]}
        rec = self.get_inst_by_primary_key(instid, session=session)
        if not rec:
            # 未找到对象
            raise ResourceNotFoundException()
        if not relationname:
            return self.serializer(rec)
        if relationname not in self.relationships:
            raise RelationshipError()
        relate_objects = getattr(rec, relationname, None)
        if not relate_objects:
            return [], 404
        # 当前对象关联的对象的id列表
        relate_serializer = self.relate_serializer[relationname]
        allow_relate_ids = [obj.id for obj in relate_objects]
        if not relateid:
            # 全部相关联的对象的数据
            return [
                relate_serializer(relate_object) for relate_object in relate_objects]
        relate_model = self.relationships[relationname]
        relate_record = self.get_inst_by_primary_key(relateid, relate_model, session=session)
        if relate_record.id not in allow_relate_ids:
            # 无法获取没有关联的其他对象
            raise RelateObjectError()
        return relate_serializer(relate_record)

    @try_load_json_data
    def put(self, instid=None, relationname=None, relateid=None,
            *args, **kwargs):
        """ Modify Records """
        session = kwargs['session']
        data = kwargs.get('json_data') or {}
        instance = self.deserializer(data, instance_id=instid, primary_key=self.primary_key)
        if not instance.id:
            session.rollback()
            raise ResourceNotFoundException()
        session.add(instance)
        session.commit()
        return {"instance_id": instance.id}

    def patch(self, instid=None, relationname=None, relateid=None,
              *args, **kwargs):
        return self.put(instid, relationname, relateid, *args, **kwargs)

    @try_load_json_data
    def post(self, *args, **kwargs):
        """ Create Records """
        session = kwargs['session']
        data = kwargs.get('json_data') or {}
        instance = self.deserializer(data)
        session.add(instance)
        session.commit()
        return {'instance_id': instance.id}

    def delete(self, instid=None, relationname=None, relateid=None, **kwargs):
        """ Delete Records """
        if not instid:
            raise ResourceNotFoundException()
        session = kwargs['session']
        rec = self.get_inst_by_primary_key(instid, session=session)
        if not rec:
            raise ResourceNotFoundException()
        rec_id = rec.id
        if not relationname:
            try_custom_delete(rec, session)
            session.commit()
            return {"instance_id": rec_id}
        if relationname not in self.relationships:
            raise RelationshipError()
        relate_objects = getattr(rec, relationname, None)
        if not (relate_objects and relateid):
            raise ResourceNotFoundException()
        allow_relate_ids = [obj.id for obj in relate_objects]
        relate_model = self.relationships[relationname]
        relate_record = self.get_inst_by_primary_key(relateid, relate_model, session=session)
        if relate_record.id not in allow_relate_ids:
            # 无法获取没有关联的其他对象
            raise RelateObjectError()
        delete_id = relate_record.id
        try_custom_delete(relate_record, session)
        session.commit()
        return {"instance_id": delete_id}

    def export(self, *args, **kwargs):
        """Add export method 增加导出功能
        """
        def generate_file(f_obj, chunk_size=1024):
            while True:
                # 每次读取一部分数据进内存
                data = f_obj.read(chunk_size)
                if not data:
                    break
                yield data
            f_obj.close()

        session = kwargs['session']
        fmt = request.args.get('format', 'csv')
        fields = request.args.get('fields', '')
        ids = request.args.get('ids', '')
        if not ids:
            return ResponseBase("提供导出数据的ids".encode('utf-8'), 400)
        if 'csv' == fmt:
            mimetype = 'text/csv'
            suffix = 'csv'
        elif 'excel' == fmt:
            mimetype = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            suffix = 'xlsx'
        else:
            return ResponseBase("格式只允许是excel/csv".encode('utf-8'), 400)
        ids = ids.split(',')
        if fields:
            fields = fields.split(',')
            field_filter = IncludeFilter(fields)
        else:
            field_filter = ExcludeFilter(['meta'])
        records = [self.get_inst_by_primary_key(f'id={_id}', session=session) for _id in ids]
        if not records:
            return ResponseBase("未找到相应数据".encode('utf-8'), 404)
        json_data = [
            field_filter.filter(
                self.serializer(record)
            ) for record in records if record
        ]
        lines = dict2lines(json_data)
        export_data = lines2export(lines)
        f_obj = export2file(export_data, fmt)
        return Response(
            generate_file(f_obj),
            status=200, mimetype=mimetype,
            headers={
                'Content-Disposition':
                f'attachment;filename={self.name}.{suffix}'
            }
        )

    def import_(self, *args, **kwargs):
        """导入excel/csv的视图函数"""
        session = kwargs['session']
        fmt = request.args.get('format')
        file = request.files.get('file')
        if not fmt:
            fmt = guess_file_format(file)
        try:
            upload_data = load_upload_file(file, fmt=fmt)
        except ValueError as e:
            logger.error(traceback.format_exc())
            return Response(f"读取上传文件失败:{e}", 400)
        finally:
            file.close()
        lines = import2lines(upload_data)
        root = lines2root(lines)
        try:
            records_data = deserialize_root(root, self.model)
        except Exception as e:
            logger.error(traceback.format_exc())
            return Response(f"转化行数据失败了: {e}")
        errors = []
        instances = []
        for join_lines, record_line in records_data.items():
            try:
                instance = self.deserializer(record_line, primary_key=self.primary_key, session=session)
            except Exception as e:
                logger.error(traceback.format_exc())
                errors.append(f"数据序列化失败: 行:{join_lines}, {record_line}")
            else:
                instances.append(instance)
        if errors:
            return Response("失败的数据有: \n {}".format('\n'.join(errors)), 400)
        session.add_all(instances)
        session.commit()
        return Response(u"成功", 200)

    def query(self):
        """list方法的代替方法, instid=None"""
        return self.dispatch_request(instid=None, relationname=None, relateid=None)
