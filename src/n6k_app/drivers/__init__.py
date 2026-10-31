"""Importing this package (any ``n6k_app.drivers.*`` import does) registers every
built-in driver; the lookups live in ``n6k_app.drivers.base``, not re-exported here.
"""

from n6k_app.drivers.base import register_driver
from n6k_app.drivers.mysql import MysqlDriver
from n6k_app.drivers.postgres import PostgresDriver

register_driver(PostgresDriver())
register_driver(MysqlDriver())
