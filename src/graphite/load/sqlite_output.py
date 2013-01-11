from graphite.load import AbstractOutputFormat
from graphite.extract.base import NODE_TYPE_USER
import sqlite3
import sys

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
	"  PRIMARY KEY (`user_id`) ON CONFLICT REPLACE"
	")")
)
TABLES.append(('friend',
	"CREATE TABLE IF NOT EXISTS `friend` ("
	"  `user_id` BIGINT UNSIGNED NOT NULL,"
	"  `friend_id` BIGINT UNSIGNED NOT NULL,"
	"  CONSTRAINT relation_edge UNIQUE (`user_id`, `friend_id`) ON CONFLICT IGNORE"
	")")
)

TABLES.append(('object',
	"CREATE TABLE IF NOT EXISTS `friend` ("
	"  `id` BIGINT UNSIGNED NOT NULL,"
	"  `friend_id` BIGINT UNSIGNED NOT NULL,"
	"  `url` varchar(512),"
	"  `image` varchar(512),"
	"  `title` varchar(512),"
	"  `description` varchar(512),"
	"  `price` varchar(512),"
	"  `ts` TIMESTAMP,"
	"  PRIMARY KEY (`id`) ON CONFLICT REPLACE"
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
	"  PRIMARY KEY (`id`) ON CONFLICT REPLACE"
	")")
)


class SqliteOutput(AbstractOutputFormat):
	filename = None
	tables = None

	def __init__(self, filename=None, **kwds):
		self.filename = filename
		if self.filename is None:
			self.filename = "igapi-example.db"
		self.conn = sqlite3.connect(self.filename)
		self.create_tables()
		self.conn.close()

	def start(self, node_type):
		self.conn = sqlite3.connect(self.filename)
		self.cursor = self.conn.cursor()

	def handle(self, node_type, id, node):
		if node_type is NODE_TYPE_USER:
			self.user_insert(id, node)
			for friend in node.get("friends", []):
				self.friend_edge_insert(id, friend)
			self.conn.commit()

	def user_insert(self, id, node):
		self.cursor.execute("INSERT INTO user VALUES (?, ?, ?, ?, ?)", (id, node.get("name", ""), node.get("username", ""), node.get("first_name", ""), node.get("last_name", "")))

	def friend_edge_insert(self, id, friend):
		self.cursor.execute("INSERT INTO friend VALUES (?, ?)", [id, friend])

	def commit(self):
		self.conn.commit()

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

