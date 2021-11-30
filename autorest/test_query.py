# -*- coding: utf-8 -*-


import base64
import json
import sqlalchemy as sqla
from sqlalchemy.orm import scoped_session, sessionmaker
from .query import search
from .test_autorest import ParentModel


class TestQuery(object):

    parent_sql = """
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-22 11:04:54', '2020-09-25 11:17:13', 1, 1, 'parent_test_change_name1');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-22 11:06:20', '2020-09-22 11:06:20', 0, 3, 'parent3');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-22 11:07:32', '2020-09-22 11:07:32', 1, 4, 'parent4');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-23 10:21:59', '2020-09-23 10:21:59', 0, 13, 'parent5');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-24 17:10:44', '2020-09-24 17:10:44', 0, 19, 'parent_post');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-24 17:12:24', '2020-09-24 17:12:24', 0, 21, 'parent_post1');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-24 17:12:55', '2020-09-24 17:12:55', 1, 22, NULL);
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (1, '2020-09-24 17:26:18', '2020-09-25 11:00:49', 1, 24, 'parent_post4');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (1, '2020-09-25 11:13:03', '2020-09-25 11:13:03', 1, 25, 'parent_post5');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (1, '2020-09-25 11:17:13', '2020-09-25 11:17:13', 1, 27, 'parent_post6');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (1, '2020-09-25 11:45:33', '2020-09-25 11:45:33', 1, 28, 'parent_post7');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-25 11:46:34', '2020-09-25 11:46:34', 1, 29, 'parent_post8');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (1, '2020-09-25 12:11:29', '2020-09-25 12:11:29', 1, 30, 'parent_post9');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (1, '2020-09-25 12:18:27', '2020-09-25 12:18:27', 1, 31, 'parent_post10');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-25 15:03:01', '2020-09-25 15:03:01', 1, 32, 'parent_post11');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-27 12:02:10', '2020-09-27 12:02:10', 1, 33, 'parent_pOst12');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-27 12:02:19', '2020-09-27 12:02:19', 1, 34, 'parent_poST13');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-28 11:25:08', '2020-09-28 11:25:08', 1, 35, 'test_m2m');
INSERT INTO `parent`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`) VALUES (0, '2020-09-28 12:24:44', '2020-09-28 12:24:44', 1, 38, 'test_o2m');
"""

    child_sql = """
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-22 11:04:54', '2020-09-22 11:04:54', 1, 1, 'child3', 1, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-22 11:04:54', '2020-09-25 11:13:03', 1, 2, 'child2', 4, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-22 11:06:20', '2020-09-22 11:06:20', 1, 3, 'child3', 3, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-22 11:06:20', '2020-09-22 11:06:20', 1, 4, 'child2', 3, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-22 11:07:32', '2020-09-22 11:07:32', 1, 5, 'child3', 4, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-23 10:21:59', '2020-09-23 10:21:59', 1, 14, 'child3', 13, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:10:44', '2020-09-24 17:10:44', 1, 15, 'child3', 19, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:10:44', '2020-09-24 17:10:44', 1, 16, 'child2', 19, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:12:24', '2020-09-24 17:12:24', 1, 17, 'child3', 21, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:12:24', '2020-09-24 17:12:24', 1, 18, 'child2', 21, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:12:55', '2020-09-24 17:12:55', 1, 19, 'child3', 22, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:12:55', '2020-09-24 17:12:55', 1, 20, 'child2', 22, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:26:18', '2020-09-24 17:26:18', 1, 21, 'child3', 24, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 17:26:18', '2020-09-24 17:26:18', 1, 22, 'child2', 24, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 18:03:11', '2020-09-24 18:03:11', 1, 23, 'child3', 24, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 18:03:11', '2020-09-24 18:03:11', 1, 24, 'child2', 24, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 18:05:09', '2020-09-25 11:17:13', 1, 25, 'child2', 1, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-24 18:05:09', '2020-09-25 11:17:13', 1, 26, 'child3', 1, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:13:03', '2020-09-25 11:13:03', 1, 27, 'child3', 25, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:13:03', '2020-09-25 11:13:03', 1, 28, 'child2', 25, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:17:13', '2020-09-25 11:17:13', 1, 29, 'child3', 27, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:17:13', '2020-09-25 11:17:13', 1, 30, 'child2', 27, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:45:33', '2020-09-25 11:45:33', 1, 31, 'child3', 28, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:45:33', '2020-09-25 11:45:33', 1, 32, 'child2', 28, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:46:34', '2020-09-25 11:46:34', 1, 33, 'child3', 29, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 11:46:34', '2020-09-25 11:46:34', 1, 34, 'child2', 29, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 12:11:29', '2020-09-25 12:11:29', 1, 35, 'child3', 30, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 12:11:29', '2020-09-25 12:11:29', 1, 36, 'child2', 30, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 12:18:27', '2020-09-25 12:18:27', 1, 37, 'child3', 31, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 12:18:27', '2020-09-25 12:18:27', 1, 38, 'child2', 31, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 15:03:01', '2020-09-25 15:03:01', 1, 39, 'child3', 32, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-25 15:03:01', '2020-09-25 15:03:01', 1, 40, 'child2', 32, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-27 12:02:10', '2020-09-27 12:02:10', 1, 41, 'child3', 33, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-27 12:02:10', '2020-09-27 12:02:10', 1, 42, 'child2', 33, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-27 12:02:19', '2020-09-27 12:02:19', 1, 43, 'child3', 34, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-27 12:02:19', '2020-09-27 12:02:19', 1, 44, 'child2', 34, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-28 11:25:08', '2020-09-28 11:27:33', 1, 45, 'test_child3', 35, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-28 11:25:08', '2020-09-28 11:27:33', 1, 46, 'test_child2', 35, NULL);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-28 12:24:44', '2020-09-28 12:24:44', 1, 47, 'test_o2m_child1', 38, 38);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (1, '2020-09-28 12:24:44', '2020-09-28 14:54:17', 1, 48, 'test_o2m_child2', 38, 38);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (1, '2020-09-28 14:54:17', '2020-09-28 14:58:32', 1, 49, 'new_test_o2m_child', 38, 38);
INSERT INTO `child`(`is_deleted`, `create_time`, `update_time`, `status`, `id`, `name`, `parent_id`, `new_parent_id`) VALUES (0, '2020-09-28 14:59:38', '2020-09-28 14:59:38', 1, 52, 'new_test_o2m_child', 38, 38);
"""

    @classmethod
    def setup_class(cls):
        mysql_conn_uri = "mysql+pymysql://root:mysql@localhost:3306/test"
        engine = sqla.create_engine(mysql_conn_uri)
        cls.engine = engine
        Session = scoped_session(sessionmaker(bind=engine))
        print("create session")
        cls.session = Session()
        cls.setup_data()

    @classmethod
    def setup_data(cls):
        from itertools import chain
        with cls.engine.begin() as connection:
            connection.execute("delete from child")
            connection.execute("delete from parent_sub_table")
            connection.execute("delete from parent")
            for line in chain(cls.parent_sql.splitlines(), cls.child_sql.splitlines()):
                if line:
                    connection.execute(line)

    @classmethod
    def teardown_class(cls):
        """ teardown any state that was previously setup with a call to
        setup_class.
        """
        print("close session")
        cls.session.rollback()
        cls.session.close()

    def test_base64(self):
        """测试query json base64之后的搜索"""
        q = base64.b64encode("{}".encode('utf-8'))
        query = search(self.session, ParentModel, q, format='base64')
        assert query.count() >= 0

    def test_null(self):
        q ='[{"name": "name", "op": "is_null"}]'
        query = search(self.session, ParentModel, q)
        assert query.count()
        for obj in query:
            assert None == obj.name

    def test_compare(self):
        filter1 = base64.b64encode('{"name": "id", "op": "gt", "val": 20}'.encode('utf-8'))
        query1 = search(self.session, ParentModel, filter1, format='base64')
        assert query1.count()
        for obj in query1:
            assert obj.id > 20
        filter2 = '{"name": "id", "op": "le", "val": 19}'
        query2 = search(self.session, ParentModel, filter2)
        assert query2.count()
        for obj in query2:
            assert obj.id <= 19
        filter3 = '{"name": "id", "op": "eq", "val": 35}'
        query3 = search(self.session, ParentModel, filter3)
        obj = query3.one()
        assert obj.id == 35

    def test_like(self):
        filter1 = '{"name": "name", "op": "like", "val": "post"}'
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        for obj in query1:
            print(obj.name)
            assert 'post' in obj.name.lower()

        filter2 = '{"name": "name", "op": "ilike", "val": "post"}'
        query2 = search(self.session, ParentModel, filter2)
        assert query2.count()
        for obj in query2:
            assert 'post' in obj.name.lower()

    def test_in(self):
        l = [19, 20, 21, 29, 30, 32]
        filter1 = '{"name": "id", "op": "in", "val": %s}' % l
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        for obj in query1:
            print(obj.id)
            assert obj.id in l

    def test_has(self):
        filter1 = '{"name": "children", "op": "has", "val": {"name": "id", "op": "eq", "val": 52}}'
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        for obj in query1:
            assert 52 in [child.id for child in obj.children]

    def test_any(self):
        filter1 = '{"name": "children", "op": "any", "val": {"name": "id", "op": "eq", "val": 52}}'
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        for obj in query1:
            assert 52 in [child.id for child in obj.children]

    def test_and(self):
        """"""
        l = [2, 3, 12, 13, 14]
        filter1 = '{"and": [{"name": "status", "op": "eq", "val": 0}, {"name": "id", "op": "in", "val": %s}]}' % l
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        for obj in query1:
            assert obj.id in l and 0 == obj.status

    def test_or(self):
        l = [1, 2, 3, 4]
        filter1 = '{"or": [{"name": "status", "op": "eq", "val": 0}, {"name": "id", "op": "in", "val": %s}]}' % l
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        for obj in query1:
            assert obj.id in l or 0 == obj.status

    def test_and_or(self):
        """测试and和or组合"""
        l = [2, 3, 12, 13, 14]
        filt_dict = {
            'and': [{
                'name': 'status',
                'op': 'eq',
                'val': 0
            }, {'or': [{
                'name': 'id',
                'op': 'eq',
                'val': 21
            }, {
                'name': 'id',
                'op': 'in',
                'val': l
            }]}]
        }
        filter1 = json.dumps(filt_dict)
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        ids = []
        status = []
        for obj in query1:
            ids.append(obj.id)
            status.append(obj.status)
        assert 1 not in status
        assert [3, 13, 21] == ids

    def test_or_and(self):
        l = [1, 2, 3, 4, 5]
        filt_dict = {
            'or': [{
                'and': [{
                    'name': 'id',
                    'op': 'in',
                    'val': l
                }, {
                    'name': 'status',
                    'op': 'eq',
                    'val': '0',
                }]
            }, {
                'name': 'id',
                'op': 'eq',
                'val': 38
            }]
        }
        filter1 = json.dumps(filt_dict)
        query1 = search(self.session, ParentModel, filter1)
        assert query1.count()
        ids = []
        for obj in query1:
            ids.append(obj.id)
        assert [3, 38] == ids
