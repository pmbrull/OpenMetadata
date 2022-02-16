#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
Test Metrics behavior
"""
from unittest import TestCase

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base

from metadata.orm_profiler.engines import create_and_bind_session
from metadata.orm_profiler.metrics.registry import StaticMetrics, ComposedMetrics
from metadata.orm_profiler.profiles.core import SingleProfiler

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(256))
    fullname = Column(String(256))
    nickname = Column(String(256))
    age = Column(Integer)


class MetricsTest(TestCase):
    """
    Run checks on different metrics
    """
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    session = create_and_bind_session(engine)

    @classmethod
    def setUpClass(cls) -> None:
        """
        Prepare Ingredients
        """
        User.__table__.create(bind=cls.engine)

        data = [
            User(name="John", fullname="John Doe", nickname="johnny b goode", age=30),
            User(name="Jane", fullname="Jone Doe", nickname=None, age=31),
        ]
        cls.session.add_all(data)
        cls.session.commit()

    def test_min(self):
        """
        Check the Min metric
        """
        min_age = StaticMetrics.MIN(col=User.age)
        min_profiler = SingleProfiler(self.session, min_age)
        res = min_profiler.execute()

        # Note how we can get the result value by passing the metrics name
        assert res.get(StaticMetrics.MIN.name) == 30

    def test_std(self):
        """
        Check STD metric
        """
        std_age = StaticMetrics.STDDEV(col=User.age)
        std_profiler = SingleProfiler(self.session, std_age)
        res = std_profiler.execute()
        # SQLITE STD custom implementation returns the squared STD.
        # Only useful for testing purposes
        assert res.get(StaticMetrics.STDDEV.name) == 0.25

    def test_null_count(self):
        """
        Check null count
        """
        null_count = StaticMetrics.NULL_COUNT(col=User.nickname)
        nc_profiler = SingleProfiler(self.session, null_count)
        res = nc_profiler.execute()

        assert res.get(StaticMetrics.NULL_COUNT.name) == 1

    def test_null_ratio(self):
        count = StaticMetrics.COUNT(col=User.nickname)
        null_count = StaticMetrics.NULL_COUNT(col=User.nickname)

        # Build the ratio based on the other two metrics
        null_ratio = ComposedMetrics.NULL_RATIO(col=User.nickname)

        composed_profiler = SingleProfiler(self.session, count, null_count, null_ratio)
        res = composed_profiler.execute()
        assert res.get(ComposedMetrics.NULL_RATIO.name) == 0.5
