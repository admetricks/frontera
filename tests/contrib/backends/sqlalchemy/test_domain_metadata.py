import pytest
from frontera.contrib.backends.sqlalchemy.components import DomainMetadata, DomainMetadataKV, States
from frontera.contrib.backends.sqlalchemy.models import StateModel
from frontera.contrib.backends.sqlalchemy import Distributed
from frontera.core.models import Request
from tests.mocks.frontier_manager import FakeFrontierManager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest import TestCase
import random
import string

r1 = Request(
    'https://www.example.com',
    meta={
        b'fingerprint': b'10',
        b'domain': {b'name': b'www.example.com', b'fingerprint': b'81'},
        b'state': 1
    }
)

def random_string(N):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))


class TestSqlAlchemyDomainMetadata(TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.session_cls = sessionmaker()
        self.session_cls.configure(bind=self.engine)
        DomainMetadataKV.__table__.create(bind=self.engine)

    def test_basic(self):
        dm = DomainMetadata(self.session_cls)
        value = {"someint": 1, "somefloat": 1, "someblob": b"bytes"}
        dm["test"] = value
        assert "test" in dm
        assert dm["test"] == value
        del dm["test"]
        assert "test" not in dm

        dm["test"] = 111
        assert "test" in dm
        assert dm["test"] == 111

    def test_many_items(self):
        dm = DomainMetadata(self.session_cls)
        for i in range(200):
            dm["key%d" % i] = random_string(10)

        for i in range(200):
            assert "key%d" % i in dm
            assert len(dm["key%d" % i]) == 10
            del dm["key%d" % i]

@pytest.fixture(scope="module")
def sql_backend():
    manager = FakeFrontierManager.from_settings()
    backend = Distributed(manager)
    backend._init_strategy_worker(manager)
    yield backend
    backend.frontier_stop()
    backend.engine.dispose()

def test_check_and_create_tables_is_clear(sql_backend):
    session_cls = sessionmaker()
    session_cls.configure(bind=sql_backend.engine)
    another_session = session_cls()

    sql_backend.states.update_cache([r1])
    sql_backend.states.flush()
    model_states = sql_backend.models['StateModel']
    model_dm = sql_backend.models['DomainMetadataModel']
    assert another_session.query(model_states).count() == 1

    sql_backend.check_and_create_tables(False, True, (model_states, model_dm))
    assert another_session.query(model_states).count() == 0

