import sys
from warnings import filterwarnings

import MySQLdb

from graphite.load import AbstractOutputFormat
from graphite.extract.base import NODE_TYPE_USER


# TODO: create a meta schema object that can be shared by all DB outputs
TABLES = []
TABLES.append(('user',
	"CREATE TABLE IF NOT EXISTS `user` ("
	"  `user_id` BIGINT UNSIGNED NOT NULL,"
	"  `name` varchar(128),"
	"  `username` varchar(128),"
	"  `first_name` varchar(128),"
	"  `last_name` varchar(128),"
	"  `profile_image` varchar(512),"
	"  `ts` TIMESTAMP,"
	"  PRIMARY KEY (`user_id`)"
	")")
)
TABLES.append(('friend',
	"CREATE TABLE IF NOT EXISTS `friend` ("
	"  `user_id` BIGINT UNSIGNED NOT NULL,"
	"  `friend_id` BIGINT UNSIGNED NOT NULL,"
	"  UNIQUE (`user_id`, `friend_id`)"
	")")
)

TABLES.append(('object',
	"CREATE TABLE IF NOT EXISTS `object` ("
	"  `id` BIGINT UNSIGNED NOT NULL,"
	"  `friend_id` BIGINT UNSIGNED NOT NULL,"
	"  `url` varchar(512),"
	"  `image` varchar(512),"
	"  `title` varchar(512),"
	"  `ts` TIMESTAMP,"
	"  PRIMARY KEY (`id`)"
	")")
)

TABLES.append(('action',
	"CREATE TABLE IF NOT EXISTS `action` ("
	"  `id` BIGINT UNSIGNED NOT NULL,"
	"  `user_id` BIGINT UNSIGNED NOT NULL,"
	"  `object_id` BIGINT UNSIGNED NOT NULL,"
	"  `created` TIMESTAMP,"
	"  `deleted` TIMESTAMP,"
	"  `action` varchar(32),"
	"  PRIMARY KEY (`id`)"
	")")
)


filterwarnings("ignore", category=MySQLdb.Warning)


class MySQLOutput(AbstractOutputFormat):
	def __init__(self, host=None, port=None, db=None, user=None, password=None):
		# The MySQLdb.connect() function acts weird if we send it None kwargs
		self.conn_kwargs = dict(host=host, port=port, db=db, user=user, passwd=password)
		for name, value in self.conn_kwargs.items():
			if value is None:
				del self.conn_kwargs[name]
		self.conn_kwargs.setdefault("charset", "utf8")
		self.conn = MySQLdb.connect(**self.conn_kwargs)
		self.create_tables()
		self.conn.close()
		self.user_inserts = []
		self.friend_inserts = []

	def start(self, node_type):
		self.conn = MySQLdb.connect(**self.conn_kwargs)
		self.conn.autocommit(False)
		self.cursor = self.conn.cursor()
		self.user_inserts = []
		self.friend_inserts = []

	def handle(self, node_type, id, node):
		if node_type is NODE_TYPE_USER:
			self.user_insert(id, node)
			for friend in node.get("friends", []):
				self.friend_edge_insert(id, friend)

	def user_insert(self, id, node):
		self.user_inserts.append((id, node.get("name", ""), node.get("username", ""), node.get("first_name", ""), node.get("last_name", "")))

	def friend_edge_insert(self, id, friend):
		self.friend_inserts.append((id, friend))

	def commit(self):
		# MySQLdb runs *much* faster if we use executemany() to bulk insert.
		self.cursor.execute("BEGIN")
		if self.user_inserts:
			self.cursor.executemany("""
				REPLACE INTO user(user_id, name, username, first_name, last_name)
				VALUES (%s, %s, %s, %s, %s)
				""", self.user_inserts)
		if self.friend_inserts:
			self.cursor.executemany("INSERT IGNORE INTO friend VALUES (%s, %s)", self.friend_inserts)
		self.cursor.execute("COMMIT")
		
	def complete(self):
		self.conn.close()

	def create_tables(self):
		cursor = self.conn.cursor()
		for name, ddl in TABLES:
			try:
				print >> sys.stderr, "Creating table {}: ".format(name)
				cursor.execute(ddl)
			except Exception as err:
				print >> sys.stderr, err
			else:
				print >> sys.stderr, "OK"
		cursor.close()

