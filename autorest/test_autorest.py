# -*- coding: utf-8 -*-

import logging
from datetime import datetime
from uuid import uuid4
import pytest
import flask
import sqlalchemy as sqla
from sqlalchemy import Table
from sqlalchemy.orm import relationship, scoped_session, sessionmaker
from sqlalchemy.ext import declarative
from .fieldfilter import ExcludeFilter, IncludeFilter
from .manager import APIManager
from .resource import Resource
from .. import sqlalchemy as extend_sqla
from base.sqlalchemy import ExtendedColumn, VirtualForeignKey


logger = logging.getLogger(__name__)

Base = declarative.declarative_base(cls=extend_sqla.Base,
                                    metadata=extend_sqla.BaseMetaData())


def format_time(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S"):
    return dt.strftime(fmt)


parent_sub_table = Table('parent_sub_table', Base.metadata,
    sqla.Column('parent_id', sqla.ForeignKey('parent.id'), primary_key=True),
    sqla.Column('sub_id', sqla.ForeignKey('sub.id'), primary_key=True)
)


class ParentModel(Base):
    __tablename__ = 'parent'

    id = sqla.Column(sqla.Integer, comment="ID", primary_key=True)
    name = sqla.Column(sqla.String(64), comment="Name", unique=True)
    children = relationship("ChildModel", back_populates='parent')
    new_children = relationship("ChildModel", back_populates='new_parent')
    subs = relationship("SubModel", secondary=parent_sub_table, back_populates='parents')


class ChildModel(Base):
    __tablename__ = 'child'

    id = sqla.Column(sqla.Integer, comment="ID", primary_key=True)
    name = sqla.Column(sqla.String(64), comment="Name")
    parent_id = sqla.Column(sqla.Integer, sqla.ForeignKey('parent.id'))
    parent = relationship("ParentModel", back_populates='children')
    new_parent_id = ExtendedColumn(VirtualForeignKey("ParentModel.id", ondelete="CASCADE"))
    new_parent = relationship("ParentModel", primaryjoin='ChildModel.new_parent_id == ParentModel.id',
                              foreign_keys=[new_parent_id])


class SubModel(Base):
    __tablename__ = 'sub'

    id = sqla.Column(sqla.Integer, comment="ID", primary_key=True)
    name = sqla.Column(sqla.String(64), comment="Name")
    parents = relationship("ParentModel", secondary=parent_sub_table, back_populates='subs')


@pytest.fixture
def client():
    from product.cli import app

    with app.test_client() as client:
        yield client


class TestMultiModel():

    @classmethod
    def setup_class(cls):
        """ setup any state specific to the execution of the given class (which
        usually contains tests).
        """
        mysql_conn_uri = "mysql+pymysql://root:mysql@localhost:3306/test"
        engine = sqla.create_engine(mysql_conn_uri)
        Base.metadata.create_all(engine)
        Session = scoped_session(sessionmaker(bind=engine))
        print("create session")
        cls.session = Session()
        cls.manager = APIManager()
        cls.app = flask.Flask(__name__)
        cls.create_parent_resource()

    @classmethod
    def create_parent_resource(cls):
        """创建Parent的view"""
        from functools import wraps
        from .serializer import serializer, deserializer

        def test_decorator(func):
            @wraps(func)
            def decorate(*args, **kwargs):
                ret = func(*args, **kwargs)
                return {'result': ret, 'code': 200}
            return decorate

        def test_method_decorator(func):
            @wraps(func)
            def decorate(*args, **kwargs):
                # response object
                response = func(*args, **kwargs)
                response['id'] = uuid4().hex.lower()
                return response
            return decorate

        rest_decorators = {
            'get': test_decorator,
        }
        method_decorators = (test_method_decorator, )

        CustomResource = Resource(
            'CustomResource',
            rest_decorators=rest_decorators,
            method_decorators=method_decorators
        )

        filters = [
            ExcludeFilter(
                ['children.'],
                global_fields=['is_deleted', 'create_time', 'update_time']
            )
        ]
        type_converters = {
            datetime: format_time,
        }

        column_converters = {
            'child.parent_id': int,
        }

        parent_serializer = serializer(filters=filters, type_converters=type_converters)
        parent_deserializer = deserializer(filters=filters,
                                           column_converters=column_converters)

        parent_resource = CustomResource(ParentModel, session=cls.session, name='parent',
                                         allow_methods=('GET', 'POST', 'PUT', 'DELETE'),
                                         serializer=parent_serializer, primary_key='id',
                                         deserializer=parent_deserializer)
        cls.manager.add_resource(parent_resource, url_prefix='/api')
        cls.manager.init_app(cls.app)

    @classmethod
    def teardown_class(cls):
        """ teardown any state that was previously setup with a call to
        setup_class.
        """
        print("close session")
        cls.session.rollback()
        cls.session.close()

    """单例模型"""
    def test_singleton_manager(self):
        """测试api_manager的单例模型"""
        manager1 = APIManager()
        manager2 = APIManager()
        assert id(manager1) == id(manager2)

    """测试蓝图/视图"""
    def test_restapi(self):
        """测试创建资源"""
        assert 'parentapi0' in self.app.blueprints
        assert '/api/parent' in [rule.rule for rule in self.app.url_map.iter_rules()]
        assert '/api/parent/' in [rule.rule for rule in self.app.url_map.iter_rules()]

    def test_create_custom_resource(self):
        with self.app.test_client() as client:
            response = client.get('/api/parent/1')


        ret_json = response.json
        print(ret_json)
        # method_decorator 增加的id
        assert 'id' in ret_json

        # rest_decorator 增加的外层包裹
        assert 'result' in ret_json and isinstance(ret_json['result'], dict)
        assert 200 == ret_json['code']

    """测试filter"""
    def test_create_filter(self):
        """测试创建filter"""
        def print_children(filter_instance):
            nonlocal layer
            layer += 1
            for relation, _filters in filter_instance.filters.items():
                for _filter in _filters:
                    print_children(_filter)

        _filter_list = ['id', 'create_time',
                        'lines.id', 'lines.create_time',
                        'lines.create_uid.create_time', 'lines.create_uid.id',
                        'lines.create_uid.partner_id.id', 'lines.create_uid.partner_id.create_time']
        _filter = ExcludeFilter(_filter_list, "TestFilter")
        layer = 0
        print_children(_filter)
        assert layer == 4

    def test_exclude_dict(self):
        _filter_list = ['lines.tags',
                        'brand',
                        'tags',
                        ]
        _filter = ExcludeFilter(_filter_list, "TestFilter", global_fields=['id', 'create_time'])
        data = [{
            "id": 123,
            "name": "test_name",
            "create_time": "2020-09-01 00:00:00",
            "update_time": "2020-09-02 00:00:00",
            "lines": [{
                "id": 1233,
                "name": "test_line",
                "tags": [{'id': 123}]
            },{
                "id": 223,
                "create_uid": {
                    "id": 1231,
                    "name": "test_username"
                }
            },{
                "id": 233132,
                "create_uid": {
                    "id": 12321,
                    "create_time": "2020-09-01 00:00:00",
                    "partner_id": {
                        "id": 321431,
                        "name": "test_partner_name",
                        "create_time": "2020-09-01 00:00:00"
                    }
                }
            }],
            "brand": {
                "id": 123,
                "name": "test_brand",
            },
            "tags": [{
                "id": 1,
                "name": "test_tag1",
            },{
                "id": 2,
                "name": "test_tag2"
            }]
        }]
        data.append(data[0])

        filtered_data = _filter.filter(data)
        expect_data = [{
            "name": "test_name",
            "update_time": "2020-09-02 00:00:00",
            "lines": [{
                "name": "test_line"
            },{
                "create_uid": {
                    "name": "test_username"
                }
            },{
                "create_uid": {
                    "partner_id": {
                        "name": "test_partner_name",
                    }
                }
            }]
        }]
        expect_data.append(expect_data[0])
        assert filtered_data == expect_data

    def test_include_dict(self):
        """测试include的过滤"""
        _filter_list = ['name', 'lines.create_uid',  ]
        include_filter = IncludeFilter(_filter_list, "IncludeFilter", global_fields=['id', ])
        data = [{
            "id": 123,
            "name": "test_name",
            "create_time": "2020-09-01 00:00:00",
            "update_time": "2020-09-02 00:00:00",
            "lines": [{
                "id": 1233,
                "name": "test_line",
                "tags": [{'id': 123}]
            },{
                "id": 223,
                "create_uid": {
                    "id": 1231,
                    "name": "test_username"
                }
            },{
                "id": 233132,
                "create_uid": {
                    "id": 12321,
                    "create_time": "2020-09-01 00:00:00",
                    "partner_id": {
                        "id": 321431,
                        "name": "test_partner_name",
                        "create_time": "2020-09-01 00:00:00"
                    }
                }
            }],
            "brand": {
                "id": 123,
                "name": "test_brand",
            },
            "tags": [{
                "id": 1,
                "name": "test_tag1",
            },{
                "id": 2,
                "name": "test_tag2"
            }]
        }]
        filtered_data = include_filter.filter(data)
        expected = [{
            "id": 123,
            "name": "test_name",
            "lines": [{
                "id": 1233,
            }, {
                "id": 223,
                "create_uid": {
                    "id": 1231,
                    "name": "test_username"
                }
            }, {
                "id": 233132,
                "create_uid": {
                    "id": 12321,
                    "create_time": "2020-09-01 00:00:00",
                    "partner_id": {
                        "id": 321431,
                        "name": "test_partner_name",
                        "create_time": "2020-09-01 00:00:00"
                    }
                }
            }],
        }]
        print(filtered_data)
        # assert 0
        assert filtered_data == expected

    """测试序列化/反序列化"""
    def test_serialize(self):
        """测试序列化"""
        from .serializer import serialize
        import json
        child1 = ChildModel(**{
            'name': 'child1',
        })
        child2 = ChildModel(**{
            'name': 'child2'
        })
        parent = ParentModel(**{
            'name': 'parent',
        })
        parent.children.append(child1)
        parent.children.append(child2)
        filters = [
            ExcludeFilter(['children.'],
                       global_fields=['id', 'is_deleted', 'status', 'create_time', 'update_time'])
        ]
        data = serialize(parent, filters)
        expected = {
            'name': 'parent',
            'children': [{
                'name': 'child1',
                'parent_id': None,
                'new_parent_id': None,
            }, {
                'name': 'child2',
                'parent_id': None,
                'new_parent_id': None,
            }]
        }
        print(json.dumps(data))
        assert data == expected

    def test_deserialize(self):
        """测试反序列化"""
        from .serializer import deserialize

        def validate_update_time(data):
            for child in data['children']:
                if child.get('update_time') and \
                        not isinstance(child['update_time'], str):
                    raise ValueError()

        parent_name = 'parent5'
        data = {
            'name': parent_name,
            'children': [{
                'name': 'child3',
            }, {
                'id': 2,
                'name': 'child2',
                'is_deleted': 1,
                'update_time': "2020-09-28 00:00:00"
            }]
        }
        filters = [
            ExcludeFilter(global_fields=['is_deleted', ])
        ]

        validators = [validate_update_time, ]

        column_converters = {
            'child.name': lambda x: x.join(('xx', 'xx'))
        }

        parent = deserialize(data, ParentModel, self.session,
                             filters=filters, validators=validators,
                             column_converters=column_converters)
        print(f"Parent: {parent.name}, parent.id: {parent.id}")
        child_ids = []
        for child in parent.children:
            print(f"Child: {child.id}, "
                  f"parent_id: {child.parent_id}, "
                  f"name: {child.name}, is_deleted: {child.is_deleted}")
            child_ids.append(child.id)
        self.session.rollback()
        # assert 0
        assert parent.name == parent_name
        assert 2 in child_ids

    def test_serial_converters(self):
        """测试序列化的转化"""
        from .serializer import serialize

        type_converters = {
            datetime: format_time,
        }

        column_converters = {
            'child.name': lambda x: x.join(('xx', 'xx'))
        }

        filters = [
            ExcludeFilter(['children.', ], global_fields=['is_deleted'])
        ]
        parent = self.session.query(ParentModel)\
            .filter(ParentModel.id == 4).first()

        child = self.session.query(ChildModel)\
            .filter(ChildModel.id == 2).first()

        child.parent_id = 4

        ret = serialize(parent, filters, type_converters, column_converters)
        child_ids = [child['id'] for child in ret['children']]
        assert 2 in child_ids

        parent_create_time = ret['create_time']
        children_create_time_list = [
            child['create_time'] for child in ret['children']
        ]

        for t in (parent_create_time, *children_create_time_list):
            assert datetime.strptime(t, "%Y-%m-%d %H:%M:%S")

        for child in ret['children']:
            print(child)
            assert child['name'].startswith('xx') \
                and child['name'].endswith('xx')

    """测试具体接口"""
    def _test_get_by_id(self):
        """根据id获取单个数据"""
        self.test_create_custom_resource()

    def test_list(self):
        """测试获取列表"""
        with self.app.test_client() as client:
            ret = client.get('/api/parent/')
            ret_json = ret.json

        assert 200 == ret_json['code']
        assert isinstance(ret_json['result'], list) and len(ret_json['result']) > 0

    def test_post(self):
        """测试创建数据"""

        parent = self.session.query(ParentModel).execution_options(**{
            'with_deleted': True
        }).order_by(ParentModel.id.desc()).first()
        name, sequence = parent.name[:-1], parent.name[-1]
        try:
            sequence = int(sequence)
        except Exception:
            sequence = 0

        parent_name = f'{name}{sequence + 1}'
        data = {
            'name': parent_name,
            'children': [{
                'create_time': '12345',
                'name': 'child3',
            }, {
                'name': 'child2',
                'is_deleted': 1,
            }]
        }

        with self.app.test_client() as client:
            response = client.post('/api/parent', json=data)
            ret = response.json

        assert response.status_code == 200
        assert isinstance(ret['instance_id'], int)

    def test_put(self):
        first_rec = self.session.query(ParentModel).first()
        new_parent_name = "parent_test_change_name1"
        data = {
            'name': new_parent_name,
            'children': [{
                'id': 26,
                'create_time': '12345',
                'name': 'child3',
            }, {
                'id': 25,
                'name': 'child2',
                'is_deleted': 1,
            }]
        }

        with self.app.test_client() as client:
            response = client.put(f'/api/parent/{first_rec.id}', json=data)
            ret = response.json
            print(ret)

        assert response.status_code == 200

    def test_delete(self):
        last_rec = self.session.query(ParentModel)\
            .order_by(ParentModel.id.desc()).first()

        with self.app.test_client() as client:
            response = client.delete(f'/api/parent/{last_rec.id}')
            ret = response.json
            print(ret)
        assert isinstance(ret['instance_id'], int)

    def test_m2m(self):
        """ 测试m2m """
        from .serializer import deserialize

        parent_name = "test_m2m"

        parent_obj = self.session.query(ParentModel).filter(
            ParentModel.name == parent_name
        ).first()

        subs = [{
            'name': 'test_m2m_sub1',
        }, {
            'name': 'test_m2m_sub2'
        }]

        if parent_obj:
            children = [{
                'name': child.name,
                'id': child.id
            } for child in parent_obj.children]
            difference = 2 - len(parent_obj.subs)
            if difference < 0:
                parent_obj.subs = parent_obj.subs[:2]
            else:
                for i in range(difference):
                    sub = SubModel(**subs[i % 2])
                    parent_obj.subs.append(sub)
        else:
            children = [{
                'name': 'test_m2m_child1',
            }, {
                'name': 'test_m2m_child2'
            }]

            parent_obj = ParentModel(**{
                'name': parent_name,
                'children': children,
                'subs': subs
            })
        self.session.add(parent_obj)
        self.session.commit()

        # 先确定m2m的
        assert len(parent_obj.subs) == 2

        children = [{'id': child.id} for child in parent_obj.children]

        # 删除关系
        new_sub = parent_obj.subs[0]

        data = {
            'children': children,
            'subs': [{'id': new_sub.id}]
        }
        parent = deserialize(data, ParentModel, self.session, instance_id=parent_obj.id)
        self.session.add(parent)
        self.session.commit()

        assert len(parent.subs) == 1

    def test_o2m(self):
        """ 测试删除o2m的关系 """
        from .serializer import deserialize

        parent_name = "test_o2m"
        parent_obj = self.session.query(ParentModel).filter(
            ParentModel.name == parent_name
        ).first()

        if not parent_obj:
            parent_obj = deserialize({
                'name': parent_name,
                'new_children': [{
                    'name': 'test_o2m_child1',
                }, {
                    'name': 'test_o2m_child2'
                }]
            }, ParentModel, self.session)
            self.session.add(parent_obj)
            self.session.commit()

        if not parent_obj.children:
            parent_obj.new_children = [ChildModel(**{
                'name': 'test_o2m_child1',
            }), ChildModel(**{
                'name': 'test_o2m_child2'
            })]
            self.session.add(parent_obj)
            self.session.commit()

        new_children = parent_obj.new_children
        keep_id = new_children[0].id
        ids = [child.id for child in new_children]

        data = {
            'new_children': [{
                'name': 'new_test_o2m_child',
            }, {
                'id': keep_id,
            }]
        }
        parent_obj = deserialize(data, ParentModel, self.session, instance_id=parent_obj.id)
        self.session.add(parent_obj)
        self.session.commit()

        new_ids = [child.id for child in parent_obj.new_children]

        assert keep_id in new_ids
        assert ids != new_ids
