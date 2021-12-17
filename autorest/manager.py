# -*- coding: utf-8 -*-

from flask import Blueprint
from .resource import ModelResource
from .errors import BlueprintExists


NO_INSTANCE_METHODS = frozenset(('GET', 'POST'))
INSTANCE_METHODS = frozenset(('GET', 'PATCH', 'PUT', 'DELETE'))


class SingletonMeta(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class APIManager(metaclass=SingletonMeta):
    """ APIManger for blueprints and views

    :param app: flask application, defaults to None
    :type app: class:`flask.Flask`
    :param resources: list for ModelResource, defaults to None
    :type resources: list, optional
    :param url_prefix: url prefix for all apis, defaults to '/api'
    :type url_prefix: str, optional
    :param kwargs: key-word arguments to init app
    :type kwargs: dict-lick object
    """
    _resources = {}

    def __init__(self, app=None, resources=None, url_prefix='/api', **kwargs):
        self.app = app
        self.url_prefix = url_prefix
        self.blueprints = []
        if resources:
            for resource in resources:
                self.add_resource(resource)
        if self.app:
            self.init_app(self.app, **kwargs)

    def init_app(self, app, **kwargs):
        """ Initial application for APIManager
        Raise class: `errors.BlueprintExists` if blueprint is already exists

        :param app: flask application
        :type app: class: `flask.Flask`
        :raises BlueprintExists: Blueprint's name is existed
        """
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['autorest'] = self
        for bp in self.blueprints:
            if bp.name in app.blueprints:
                raise BlueprintExists(f"{bp.name} already exists")
            app.register_blueprint(bp)

    def add_resource(self, resource: ModelResource, **kwargs):
        """ 增加资源 """
        bp = self.create_api_blueprint(resource, **kwargs)
        self.blueprints.append(bp)
        self._resources[resource.name] = resource
        return bp

    @staticmethod
    def generate_bp_name(all_bps, name, suffix='api'):
        """ 生成蓝图名字: 参照 restless生成蓝图函数: _next_blueprint_name """
        def find_max(bp_names):
            m = 0
            for i in bp_names:
                n = int(i.partition(name)[-1])
                if m < n:
                    m = n
            return m

        existing = [bp for bp in all_bps if bp.startswith(name)]
        if not existing:
            next_number = 0
        else:
            next_number = find_max(existing)
        return f"{name}{suffix}{next_number}"

    def create_api_blueprint(self, resource: ModelResource, url_prefix='/api',
                             collection_name=None,
                             add_relation_route=False,
                             allow_export=True,
                             allow_import=True):
        """ create blueprint from resource """
        collection_name = collection_name or resource.name
        blueprints = [] if not self.app else self.app.blueprints
        bp_name = self.generate_bp_name(blueprints, collection_name)
        bp = Blueprint(bp_name, __name__, url_prefix=url_prefix)
        self._add_api_route(bp, resource, collection_name,
                            add_relation_route=add_relation_route)
        if allow_export and hasattr(resource, 'export'):
            self._add_export_endpoint(
                bp, collection_name, resource.export, resource.allow_methods)
        if allow_import and hasattr(resource, 'import_'):
            self._add_import_endpoint(
                bp, collection_name, resource.import_, resource.allow_methods)
        return bp

    def _add_api_route(self, bp: Blueprint, resource: ModelResource, collection_name,
                       add_relation_route):
        """ 增加具体URL路由 """
        resource_view = resource.dispatch_request
        allow_methods = resource.allow_methods
        self._add_no_instance_endpoint(bp, collection_name,
                                       view=resource_view, methods=allow_methods)
        self._add_instance_endpoint(bp, collection_name,
                                    view=resource_view, methods=allow_methods)
        if add_relation_route:
            self._add_relation_endpoint(bp, collection_name,
                                        view=resource_view, methods=allow_methods)

    @staticmethod
    def _add_export_endpoint(bp, collection_name, view, methods):
        """ 增加导出路由
        /[collection_name]/export
        """
        methods = methods & {'GET'}
        bp.add_url_rule(
            '/{}/export'.format(collection_name),
            methods=methods, view_func=view
        )

    @staticmethod
    def _add_import_endpoint(bp, collection_name, view, methods):
        methods = methods & {'POST'}
        bp.add_url_rule(
            '/{}/import'.format(collection_name),
            methods=methods, view_func=view
        )

    @staticmethod
    def _add_no_instance_endpoint(bp, collection_name, view, methods):
        """ 增加非具体id的路由
        /[collection_name]
        /[collection_name]/
        """
        methods = methods & NO_INSTANCE_METHODS
        bp.add_url_rule('/{}'.format(collection_name), methods=methods, view_func=view)
        bp.add_url_rule('/{}/'.format(collection_name), methods=methods, view_func=view)

    @staticmethod
    def _add_instance_endpoint(bp, collection_name, view, methods):
        """ 增加具体对象的路由
        /[collection_name]/<instid>
        """
        methods = methods & INSTANCE_METHODS
        bp.add_url_rule('/{}/<instid>'.format(collection_name),
                        methods=methods, view_func=view)

    @staticmethod
    def _add_relation_endpoint(bp, collection_name, view, methods):
        """ 增加关联关系的路由, 允许的方法与当前模型相同
        /[collection_name]/<instid>/<relationname>
        /[collection_name]/<instid>/<relationname>/
        /[collection_name]/<instid>/<relationname>/<relateid>
        """
        no_inst_methods = methods & NO_INSTANCE_METHODS
        bp.add_url_rule('/{}/<instid>/<relationname>'.format(collection_name),
                        methods=no_inst_methods, view_func=view)
        inst_methods = methods & INSTANCE_METHODS
        bp.add_url_rule('/{}/<instid>/<relationname>/<relateid>'.format(collection_name),
                        methods=inst_methods, view_func=view)
