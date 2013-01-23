import sys
from datetime import datetime
from warnings import filterwarnings

import MySQLdb

from graphite.load import AbstractOutputFormat
from graphite.extract.base import NODE_TYPE_USER, NODE_TYPE_OBJECT, NODE_TYPE_ACTION, NODE_TYPE_USER_BOARD
from graphite import NODE_TYPE_FOLLOW


# TODO: create a meta schema object that can be shared by all outputs
TABLES = []
TABLES.append(('user',
	"CREATE TABLE IF NOT EXISTS `user` ("
	"  `user_id` CHAR(24) NOT NULL,"
	"  `facebook_id` BIGINT UNSIGNED NULL,"
	"  `name` varchar(128),"
	"  `username` varchar(128),"
	"  `first_name` varchar(128),"
	"  `last_name` varchar(128),"
	"  `profile_image` varchar(512),"
	"  `hometown` varchar(128),"
	"  `location` varchar(128),"
	"  `email` varchar(128),"
	"  `gender` varchar(10),"
	"  `birthday` date,"
	"  `is_user` bit,"
	"  `ts` TIMESTAMP,"
	"  PRIMARY KEY (`user_id`)"
	")")
)
TABLES.append(('friend',
	"CREATE TABLE IF NOT EXISTS `friend` ("
	"  `user_id` CHAR(24) NOT NULL,"
	"  `friend_id` BIGINT UNSIGNED NOT NULL,"
	"  UNIQUE (`user_id`, `friend_id`)"
	")")
)

TABLES.append(('object',
	"CREATE TABLE IF NOT EXISTS `object` ("
	"  `id` CHAR(24) NOT NULL,"
	"  `url` varchar(512),"
	"  `image` varchar(512),"
	"  `title` varchar(512),"
	"  `ts` TIMESTAMP,"
	"  PRIMARY KEY (`id`)"
	")")
)

TABLES.append(('action',
	"CREATE TABLE IF NOT EXISTS `action` ("
	"  `id` CHAR(24) NOT NULL,"
	"  `user_id` CHAR(24) NOT NULL,"
	"  `object_id` CHAR(24) NOT NULL,"
	"  `created` TIMESTAMP,"
	"  `deleted` TIMESTAMP,"
	"  `action` varchar(32),"
	"  PRIMARY KEY (`id`)"
	")")
)

TABLES.append(('user_board',
	"CREATE TABLE IF NOT EXISTS `user_board` ("
	"  `id` CHAR(24) NOT NULL,"
	"  `name` varchar(128),"
	"  `user_id` CHAR(24) NOT NULL,"
	"  `created` TIMESTAMP NOT NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  PRIMARY KEY (`id`)"
	")")
)
TABLES.append(('user_board_object',
	"CREATE TABLE IF NOT EXISTS `user_board_object` ("
	"  `board_id` CHAR(24) NOT NULL,"
	"  `object_id` CHAR(24) NOT NULL,"
	"  UNIQUE (`board_id`, `object_id`)"
	")")
)

TABLES.append(('follow',
	"CREATE TABLE IF NOT EXISTS `follow` ("
	"  `user_id` CHAR(24) NOT NULL,"
	"  `follower_id` CHAR(24) NOT NULL,"
	"  `created` TIMESTAMP NOT NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  UNIQUE (`user_id`, `follower_id`)"
	")")
)

TABLES.append(('user_board_action',
	"CREATE TABLE IF NOT EXISTS `user_board_action` ("
	"  `board_id` CHAR(24) NOT NULL,"
	"  `user_id` CHAR(24) NOT NULL,"
	"  `object_id` CHAR(24) NULL,"
	"  `action` varchar(32) NOT NULL,"
	"  `created` TIMESTAMP NOT NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  UNIQUE (`board_id`, `user_id`, `object_id`, `action`)"
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
		self.conn = self.new_conn()
		self.create_tables()
		self.conn.close()
		self.reset()
	
	def new_conn(self):
		return MySQLdb.connect(**self.conn_kwargs)
	
	def reset(self):
		self.user_inserts = []
		self.friend_inserts = []
		self.object_inserts = []
		self.user_board_inserts = []
		self.user_board_object_inserts = []
		self.follow_inserts = []
		self.user_board_action_inserts = []

	def start(self, node_type):
		self.conn = self.new_conn()
		self.conn.autocommit(False)
		self.cursor = self.conn.cursor()
		self.reset()

	def handle(self, node_type, id, node):
		if node_type is NODE_TYPE_USER:
			self.user_insert(id, node)
			for friend in node.get("friends", []):
				# Where are these values coming from?
				if isinstance(friend, dict):
					friend = friend["id"]
				self.friend_edge_insert(id, friend)
		elif node_type is NODE_TYPE_OBJECT:
			self.object_insert(id, node)
		elif node_type is NODE_TYPE_ACTION and "board_id" in node:
			self.user_board_action_insert(id, node)
		elif node_type is NODE_TYPE_USER_BOARD:
			self.user_board_insert(id, node)
			for object_id in node.get("object_ids", []):
				self.user_board_object_insert(id, object_id)
		elif node_type is NODE_TYPE_FOLLOW:
			self.follow_insert(id, node)

	def user_insert(self, id, node):
		fbid = node.get("fbid")
		profile_image = "http://graph.facebook.com/{}/picture".format(fbid) if fbid is not None else None
		birthday = node.get("birthday")
		birthday = datetime.strptime(birthday, "%m/%d/%Y") if birthday else None
		self.user_inserts.append((id, fbid, node.get("name"), node.get("username"), node.get("first_name"), node.get("last_name"), profile_image, node.get("hometown"), node.get("location.name"), node.get("email"), node.get("gender"), birthday, node.get("is_user")))

	def friend_edge_insert(self, id, friend):
		self.friend_inserts.append((id, friend))

	def object_insert(self, id, node):
		self.object_inserts.append((id, node.get("url", ""), node.get("image", ""), node.get("title", ""), node.get("updated", "")))

	def user_board_insert(self, id, node):
		self.user_board_inserts.append((id, node.get("name", ""), node.get("user_id", ""), node.get("created"), node.get("deleted")))

	def user_board_object_insert(self, id, object_id):
		self.user_board_object_inserts.append((id, object_id))

	def user_board_action_insert(self, id, node):
		self.user_board_action_inserts.append((node["board_id"], node.get("uid", ""), node.get("oid"), node.get("action", ""), node.get("created"), node.get("deleted")))

	def follow_insert(self, id, node):
		self.follow_inserts.append((id, node.get("follower_id", ""), node.get("created"), node.get("deleted")))
		
	def commit(self):
		# MySQLdb runs *much* faster if we use executemany() to bulk insert.
		self.cursor.execute("BEGIN")
		if self.user_inserts:
			self.cursor.executemany("""
				REPLACE INTO user(user_id, facebook_id, name, username, first_name, last_name, profile_image, hometown, location, email, gender, birthday, is_user)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
				""", self.user_inserts)
		if self.friend_inserts:
			self.cursor.executemany("INSERT IGNORE INTO friend VALUES (%s, %s)", self.friend_inserts)
		if self.object_inserts:
			self.cursor.executemany("""
				REPLACE INTO object(id, url, image, title, ts)
				VALUES (%s, %s, %s, %s, %s)
				""", self.object_inserts)
		if self.user_board_inserts:
			self.cursor.executemany("""
				REPLACE INTO user_board(id, name, user_id, created, deleted)
				VALUES (%s, %s, %s, %s, %s)
				""", self.user_board_inserts)
		if self.user_board_object_inserts:
			self.cursor.executemany("INSERT IGNORE INTO user_board_object VALUES (%s, %s)", self.user_board_object_inserts)
		if self.user_board_action_inserts:
			self.cursor.executemany("""
				REPLACE INTO user_board_action(board_id, user_id, object_id, action, created, deleted)
				VALUES (%s, %s, %s, %s, %s, %s)
				""", self.user_board_action_inserts)
		if self.follow_inserts:
			self.cursor.executemany("""
				REPLACE INTO follow(user_id, follower_id, created, deleted)
				VALUES (%s, %s, %s, %s)
				""", self.follow_inserts)
		self.cursor.execute("COMMIT")
		self.reset()
		
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

