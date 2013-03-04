import sys
from datetime import datetime
from warnings import filterwarnings

import MySQLdb

from graphite.load import AbstractOutputFormat
from graphite import NODE_TYPE_USER, NODE_TYPE_OBJECT, NODE_TYPE_ACTION, NODE_TYPE_USER_BOARD, NODE_TYPE_BRAND_BOARD
from graphite import NODE_TYPE_FOLLOW, NODE_TYPE_LIKE


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
	"  PRIMARY KEY (`user_id`),"
	"  INDEX (`facebook_id`)"
	")")
)
TABLES.append(('friend',
	"CREATE TABLE IF NOT EXISTS `friend` ("
	"  `user_id` CHAR(24) NOT NULL,"
	"  `friend_id` BIGINT UNSIGNED NOT NULL,"
	"  `friend_user_id` CHAR(24) NULL,"
	"  UNIQUE (`user_id`, `friend_id`),"
	"  INDEX (`friend_id`, `friend_user_id`),"
	"  INDEX (`friend_user_id`)"
	")")
)

TABLES.append(('object',
	"CREATE TABLE IF NOT EXISTS `object` ("
	"  `id` CHAR(24) NOT NULL,"
	"  `url` varchar(512),"
	"  `image` varchar(512),"
	"  `title` varchar(512),"
	"  `description` varchar(512),"
	"  `price` varchar(20),"
	"  `ts` TIMESTAMP,"
	"  PRIMARY KEY (`id`)"
	")")
)

TABLES.append(('object_tag',
	"CREATE TABLE IF NOT EXISTS `object_tag` ("
	"  `object_id` CHAR(24) NOT NULL,"
	"  `tag` varchar(512) NOT NULL,"
	"  `is_user_tag` BIT NOT NULL,"
	"  UNIQUE (`object_id`, `tag`)"
	")")
)

TABLES.append(('action',
	"CREATE TABLE IF NOT EXISTS `action` ("
	"  `user_id` CHAR(24) NOT NULL,"
	"  `object_id` CHAR(24) NOT NULL,"
	"  `action` varchar(32) NOT NULL,"
	"  `created` TIMESTAMP NOT NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  UNIQUE (`user_id`, `object_id`, `action`)"
	")")
)

TABLES.append(('board',
	"CREATE TABLE IF NOT EXISTS `board` ("
	"  `id` CHAR(24) NOT NULL,"
	"  `is_brand_board` BIT NOT NULL,"
	"  `name` VARCHAR(128) NOT NULL,"
	"  `user_id` CHAR(24) NULL,"
	"  `created` TIMESTAMP NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  PRIMARY KEY (`id`)"
	")")
)
TABLES.append(('board_object',
	"CREATE TABLE IF NOT EXISTS `board_object` ("
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

TABLES.append(('board_action',
	"CREATE TABLE IF NOT EXISTS `board_action` ("
	"  `board_id` CHAR(24) NOT NULL,"
	"  `user_id` CHAR(24) NOT NULL,"
	"  `object_id` CHAR(24) NULL,"
	"  `action` varchar(32) NOT NULL,"
	"  `created` TIMESTAMP NOT NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  UNIQUE (`board_id`, `user_id`, `object_id`, `action`)"
	")")
)

TABLES.append(('like',
	"CREATE TABLE IF NOT EXISTS `like` ("
	"  `facebook_id` BIGINT UNSIGNED NOT NULL,"
	"  `user_id` CHAR(24),"
	"  `like_id` BIGINT UNSIGNED NOT NULL,"
	"  `category` varchar(32) NOT NULL,"
	"  `name` varchar(512) NULL,"
	"  `created` TIMESTAMP NULL,"
	"  UNIQUE (`facebook_id`, `like_id`),"
	"  INDEX (`user_id`, `facebook_id`)"
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
		self.friend_updates = []
		self.object_inserts = []
		self.object_tag_inserts = []
		self.action_inserts = []
		self.board_inserts = []
		self.board_object_inserts = []
		self.follow_inserts = []
		self.board_action_inserts = []
		self.like_inserts = []

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
			for tag in node.get("tags", []):
				self.object_tag_insert(id, tag)
		elif node_type is NODE_TYPE_ACTION:
			if "board_id" in node:
				self.board_action_insert(id, node)
			else:
				self.action_insert(id, node)
		elif node_type in [NODE_TYPE_USER_BOARD, NODE_TYPE_BRAND_BOARD]:
			self.board_insert(id, node,  node_type is NODE_TYPE_BRAND_BOARD)
			for object_id in node.get("object_ids", []):
				self.board_object_insert(id, object_id)
		elif node_type is NODE_TYPE_FOLLOW:
			self.follow_insert(id, node)
		elif node_type is NODE_TYPE_LIKE:
			# "likes" will exist in node, but may have a value of None
			likes = node["likes"]
			if likes:
				for like in likes:
					self.like_insert(id, like)

	def user_insert(self, id, node):
		fbid = node.get("fbid")
		profile_image = "http://graph.facebook.com/{}/picture".format(fbid) if fbid is not None else None
		birthday = node.get("birthday")
		try:
			birthday = datetime.strptime(birthday, "%m/%d/%Y") if birthday else None
		except ValueError:
			birthday = None
		self.user_inserts.append((id, fbid, node.get("name"), node.get("username"), node.get("first_name"), node.get("last_name"), profile_image, node.get("hometown"), node.get("location.name"), node.get("email"), node.get("gender"), birthday, node.get("is_user")))
		self.friend_updates.append((id, fbid))

	def friend_edge_insert(self, id, friend):
		self.friend_inserts.append((id, friend))

	def object_insert(self, id, node):
		self.object_inserts.append((id, node.get("url", ""), node.get("image", ""), node.get("title", ""), node.get("description"), node.get("price"), node.get("updated", "")))

	def object_tag_insert(self, id, node):
		self.object_tag_inserts.append((id, node["en"], node["ns"] == "user"))

	def action_insert(self, id, node):
		self.action_inserts.append((node["uid"], node["oid"], node["action"], node["created"], node.get("deleted")))

	def board_insert(self, id, node, is_brand_board):
		self.board_inserts.append((id, is_brand_board, node["name"], node.get("user_id"), node.get("created"), node.get("deleted")))

	def board_object_insert(self, id, object_id):
		self.board_object_inserts.append((id, object_id))

	def board_action_insert(self, id, node):
		self.board_action_inserts.append((node["board_id"], node["uid"], node.get("oid"), node["action"], node["created"], node.get("deleted")))

	def follow_insert(self, id, node):
		self.follow_inserts.append((id, node.get("follower_id", ""), node.get("created"), node.get("deleted")))
		
	def like_insert(self, id, node):
		self.like_inserts.append((id, node["id"], node["category"], node.get("name"), node.get("created_time")))
		
	def commit(self):
		# MySQLdb runs *much* faster if we use executemany() to bulk insert.
		self.cursor.execute("BEGIN")
		if self.user_inserts:
			self.cursor.executemany("""
				REPLACE INTO user(user_id, facebook_id, name, username, first_name, last_name, profile_image, hometown, location, email, gender, birthday, is_user)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
				""", self.user_inserts)
		if self.friend_inserts:
			self.cursor.executemany("""
				INSERT IGNORE INTO friend(user_id, friend_id)
				VALUES (%s, %s)
				""", self.friend_inserts)
		if self.friend_updates:
			# Incrementally update friend_user_id values as much as we can while
			# loading, so that the update in self.complete() isn't so drastic.
			self.cursor.executemany("""
				UPDATE friend
				SET friend_user_id = %s
				WHERE friend_id = %s
					AND friend_user_id IS NULL
				""", self.friend_updates)
		if self.object_inserts:
			self.cursor.executemany("""
				REPLACE INTO object(id, url, image, title, description, price, ts)
				VALUES (%s, %s, %s, %s, %s, %s, %s)
				""", self.object_inserts)
		if self.object_tag_inserts:
			self.cursor.executemany("""
				REPLACE INTO object_tag(object_id, tag, is_user_tag)
				VALUES (%s, %s, %s)
				""", self.object_tag_inserts)
		if self.action_inserts:
			self.cursor.executemany("""
				REPLACE INTO action(user_id, object_id, action, created, deleted)
				VALUES (%s, %s, %s, %s, %s)
				""", self.action_inserts)
		if self.board_inserts:
			self.cursor.executemany("""
				REPLACE INTO board(id, is_brand_board, name, user_id, created, deleted)
				VALUES (%s, %s, %s, %s, %s, %s)
				""", self.board_inserts)
		if self.board_object_inserts:
			self.cursor.executemany("INSERT IGNORE INTO board_object VALUES (%s, %s)", self.board_object_inserts)
		if self.board_action_inserts:
			self.cursor.executemany("""
				REPLACE INTO board_action(board_id, user_id, object_id, action, created, deleted)
				VALUES (%s, %s, %s, %s, %s, %s)
				""", self.board_action_inserts)
		if self.follow_inserts:
			self.cursor.executemany("""
				REPLACE INTO follow(user_id, follower_id, created, deleted)
				VALUES (%s, %s, %s, %s)
				""", self.follow_inserts)
		if self.like_inserts:
			self.cursor.executemany("""
				REPLACE INTO `like`(facebook_id, like_id, category, name, created)
				VALUES (%s, %s, %s, %s, %s)
				""", self.like_inserts)
		self.cursor.execute("COMMIT")
		self.reset()
		
	def complete(self):
		self.cursor.execute("""
			UPDATE friend f, user u 
			SET f.friend_user_id = u.user_id
			WHERE f.friend_id = u.facebook_id
				AND f.friend_user_id IS NULL
			""")
		self.cursor.execute("""
			UPDATE `like` l, user u 
			SET l.user_id = u.user_id
			WHERE l.facebook_id = u.facebook_id
				AND l.user_id IS NULL
			""")
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

