# coding: utf-8
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property, Comparator
from sqlalchemy import Column, Unicode, Integer, func


Model = declarative_base()


class RedisComparator(Comparator):
    def __init__(self, *args, **kwargs):
        self.key_function = kwargs.pop('key_function')
        super(RedisComparator, self).__init__(*args, **kwargs)

    @property
    def convert_func(self):
        return getattr(func, self.key_function)

    def __eq__(self, other):
        return self.__clause_element__() == self.convert_func(other)


class RedisModel(Model):
    """
    Just wrapper around redis.
    More about foreign data wrappers:
        http://wiki.postgresql.org/wiki/Foreign_data_wrappers

    About setup redis_fwd extenstion:
        https://github.com/pg-redis-fdw/redis_fdw/

    After all we can take values from redis and use standart
    postgresql aggregation functions!

    >>> NOTE <<<
    Its may be not so fast (because use redis.keys and not support db index
    on column) but its fun :) and get realtime data from redis.

    Sublcass must setup `__key_function__` - name of user defention function on
    sql side, using for transform `id` to `redis_key`.

    Example of function for get likies for abstract object:

    CREATE OR REPLACE FUNCTION convert_redis_ids(s integer)
      RETURNS text AS
    $BODY$
      BEGIN
        RETURN 'likes:' || s;
      END
    $BODY$
      LANGUAGE plpgsql VOLATILE
      COST 100;
    ALTER FUNCTION convert_redis_ids(integer)
      OWNER TO postgres;


    Example sqlalchemy query for getting likes for Place model:

    from sqlalchemy.sql.expression import func
    from sqlalchemy.orm import eagerload, aliased
    from sqlalchemy import Integer, cast


    class Likes(RedisModel):
        __key_function__ = 'convert_redis_ids'


    l = aliased(Likes)
    query = session.query(Place, l.value)\
                   .outerjoin(l, l.id==Place.id)\
                   .query.order_by(cast(l.value, Integer).desc().nullslast())

    """
    __tablename__ = 'redis_db0'
    key = Column(Unicode, primary_key=True)
    value = Column(Integer)

    def __init__(self, key, value):
        self.key = key
        self.value = value

    @hybrid_property
    def id(self):
        return int(self.key.split(':')[-1])

    @id.comparator
    def id(cls):
        try:
            return RedisComparator(cls.key, key_function=cls.__key_function__)
        except AttributeError:
            msg = 'Set postgresql function for convert redis keys!'
            raise AttributeError(msg)
