import sys
from datetime import datetime
from warnings import filterwarnings

import MySQLdb

from graphite.load import AbstractOutputFormat
from graphite import (NODE_TYPE_USER, NODE_TYPE_FRIEND, NODE_TYPE_OBJECT,
	NODE_TYPE_ACTION, NODE_TYPE_SALE, NODE_TYPE_USER_BOARD, NODE_TYPE_BRAND_BOARD,
	NODE_TYPE_FOLLOW, NODE_TYPE_USER_LIKE, NODE_TYPE_LIKE)


# TODO: create a meta schema object that can be shared by all outputs
TABLES = []
TABLES.append(('profile',
	"CREATE TABLE IF NOT EXISTS `profile` ("
	"  `facebook_id` BIGINT UNSIGNED NOT NULL,"
	"  `is_user` bit,"
	"  `user_id` CHAR(24) NULL,"
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
	"  `ts` TIMESTAMP,"
	"  PRIMARY KEY (`facebook_id`),"
	"  INDEX (`user_id`)"
	")")
)
TABLES.append(('friend',
	"CREATE TABLE IF NOT EXISTS `friend` ("
	"  `facebook_id` BIGINT UNSIGNED NOT NULL,"
	"  `friend_id` BIGINT UNSIGNED NOT NULL,"
	"  PRIMARY KEY (`facebook_id`, `friend_id`)"
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
	"  `created` TIMESTAMP,"
	"  PRIMARY KEY (`id`)"
	")")
)

TABLES.append(('object_tag',
	"CREATE TABLE IF NOT EXISTS `object_tag` ("
	"  `object_id` CHAR(24) NOT NULL,"
	"  `tag` varchar(512) NOT NULL,"
	"  `is_user_tag` BIT NOT NULL,"
	"  PRIMARY KEY (`object_id`, `tag`)"
	")")
)

TABLES.append(('action',
	"CREATE TABLE IF NOT EXISTS `action` ("
	"  `user_id` CHAR(24) NOT NULL,"
	"  `object_id` CHAR(24) NOT NULL,"
	"  `action` varchar(32) NOT NULL,"
	"  `created` TIMESTAMP NOT NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  PRIMARY KEY (`user_id`, `object_id`, `action`)"
	")")
)

TABLES.append(('sale',
	"CREATE TABLE IF NOT EXISTS `sale` ("
	"  `sale_id` CHAR(24) NOT NULL,"
	"  `user_id` CHAR(24) NOT NULL,"
	"  `order_number` VARCHAR(32),"
	"  `total` decimal(9,2) NOT NULL,"
	"  `ts` TIMESTAMP NOT NULL,"
	"  PRIMARY KEY (`id`)"
	")")
)

TABLES.append(('sale_object',
	"CREATE TABLE IF NOT EXISTS `sale_object` ("
	"  `sale_id` CHAR(24) NOT NULL,"
	"  `object_id` CHAR(24) NOT NULL,"
	"  `price` decimal(9,2) NOT NULL,"
	"  `quantity` INT UNSIGNED NOT NULL,"
	"  PRIMARY KEY (`sale_id`, `object_id`)"
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
	"  PRIMARY KEY (`board_id`, `object_id`)"
	")")
)

TABLES.append(('follow',
	"CREATE TABLE IF NOT EXISTS `follow` ("
	"  `user_id` CHAR(24) NOT NULL,"
	"  `follower_id` CHAR(24) NOT NULL,"
	"  `created` TIMESTAMP NOT NULL,"
	"  `deleted` TIMESTAMP NULL,"
	"  PRIMARY KEY (`user_id`, `follower_id`)"
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
	"  PRIMARY KEY (`board_id`, `user_id`, `object_id`, `action`)"
	")")
)

TABLES.append(('user_like',
	"CREATE TABLE IF NOT EXISTS `user_like` ("
	"  `facebook_id` BIGINT UNSIGNED NOT NULL,"
	"  `like_id` BIGINT UNSIGNED NOT NULL,"
	"  `category` varchar(32) NOT NULL,"
	"  `name` varchar(512) NULL,"
	"  `created` TIMESTAMP NULL,"
	"  PRIMARY KEY (`facebook_id`, `like_id`)"
	")")
)

TABLES.append(('like',
	"CREATE TABLE IF NOT EXISTS `like` ("
	"  `facebook_id` BIGINT UNSIGNED NOT NULL,"
	"  `like_id` BIGINT UNSIGNED NOT NULL,"
	"  `category` varchar(32) NOT NULL,"
	"  `name` varchar(512) NULL,"
	"  `created` TIMESTAMP NULL,"
	"  PRIMARY KEY (`facebook_id`, `like_id`)"
	")")
)


filterwarnings("ignore", category=MySQLdb.Warning)


def price(text):
	if text is None:
		return None
	price = text[1:] if text.startswith("$") else text
	if not price:
		return "0.00"
	return price

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
		self.user_profile_inserts = []
		self.friend_profile_inserts = []
		self.friend_inserts = []
		self.object_inserts = []
		self.object_tag_inserts = []
		self.action_inserts = []
		self.sale_inserts = []
		self.sale_object_inserts = []
		self.board_inserts = []
		self.board_object_inserts = []
		self.follow_inserts = []
		self.board_action_inserts = []
		self.user_like_inserts = []
		self.like_inserts = []

	def start(self, node_type):
		# TODO: is there a way to not update indexes, w/o dropping them completely?
		self.conn = self.new_conn()
		self.conn.autocommit(False)
		self.cursor = self.conn.cursor()
		self.reset()

	def handle(self, node_type, id, node):
		if node_type is NODE_TYPE_USER:
			self.user_profile_insert(id, node)
			friends = node.get("friends")
			if friends:
				facebook_id = node["fbid"]
				for friend_id in friends:
					# Where are these values coming from?
#					if isinstance(friend, dict):
#						friend = friend["id"]
					self.friend_edge_insert(facebook_id, friend_id)
		elif node_type is NODE_TYPE_FRIEND:
			self.friend_profile_insert(id, node)
		elif node_type is NODE_TYPE_OBJECT:
			self.object_insert(id, node)
			for tag in node.get("tags", []):
				self.object_tag_insert(id, tag)
		elif node_type is NODE_TYPE_ACTION:
			if "board_id" in node:
				self.board_action_insert(id, node)
			else:
				self.action_insert(id, node)
		elif node_type is NODE_TYPE_SALE:
			self.sale_insert(id, node)
			for object in node["products"]:
				self.sale_object_insert(id, object)
		elif node_type in [NODE_TYPE_USER_BOARD, NODE_TYPE_BRAND_BOARD]:
			self.board_insert(id, node,  node_type is NODE_TYPE_BRAND_BOARD)
			for object_id in node.get("object_ids", []):
				self.board_object_insert(id, object_id)
		elif node_type is NODE_TYPE_FOLLOW:
			self.follow_insert(id, node)
		elif node_type is NODE_TYPE_USER_LIKE:
			# "likes" will exist in node, but may have a value of None
			likes = node["likes"]
			if likes:
				for like in likes:
					self.user_like_insert(id, like)
		elif node_type is NODE_TYPE_LIKE:
			# "likes" will exist in node, but may have a value of None
			likes = node["likes"]
			if likes:
				for like in likes:
					self.like_insert(id, like)

	def user_profile_insert(self, id, node):
		self.user_profile_inserts.append(self.profile_row(id, node, True))

	def friend_profile_insert(self, id, node):
		self.friend_profile_inserts.append(self.profile_row(None, node, False))

	def profile_row(self, id, node, is_user):
		fbid = node["fbid"]
		assert fbid, node
		profile_image = "http://graph.facebook.com/{}/picture".format(fbid)
		birthday = node.get("birthday")
		# The birthday string can be in a couple different formats
		try:
			birthday = datetime.strptime(birthday, "%Y-%m-%dT%H:%M:%S") if birthday else None
		except ValueError:
			try:
				birthday = datetime.strptime(birthday, "%m/%d/%Y") if birthday else None
			except ValueError:
				birthday = None
		ts = datetime.utcfromtimestamp(float(node["ts"]))
		return fbid, is_user, id, node.get("name"), node.get("username"), node.get("first_name"), node.get("last_name"), profile_image, node.get("hometown"), node.get("location.name"), node.get("email"), node.get("gender"), birthday, ts

	def friend_edge_insert(self, facebook_id, friend_id):
		self.friend_inserts.append((facebook_id, friend_id))

	def object_insert(self, id, node):
		self.object_inserts.append((id, node.get("url", ""), node.get("image", ""), node.get("title", ""), node.get("description"), node.get("price"), node.get("updated", ""), node.get("created", "")))

	def object_tag_insert(self, id, node):
		self.object_tag_inserts.append((id, node["en"], node["ns"] == "user"))

	def action_insert(self, id, node):
		self.action_inserts.append((node["uid"], node["oid"], node["action"], node["created"], node.get("deleted")))

	def sale_insert(self, id, node):
		self.action_inserts.append((node["user"], node["id"], "__sale__", node["created"], None))
		self.sale_inserts.append((node["id"], node["user"], node.get("order_number"), price(node["total"]), node["created"]))

	def sale_object_insert(self, id, object):
		self.sale_object_inserts.append((id, object["id"], price(object["price"]), object["qty"]))

	def board_insert(self, id, node, is_brand_board):
		self.board_inserts.append((id, is_brand_board, node["name"], node.get("user_id"), node.get("created"), node.get("deleted")))

	def board_object_insert(self, id, object_id):
		self.board_object_inserts.append((id, object_id))

	def board_action_insert(self, id, node):
		# Set oid to an empty string if it is missing or None
		oid = node.get("oid") or ""
		self.board_action_inserts.append((node["board_id"], node["uid"], oid, node["action"], node["created"], node.get("deleted")))

	def follow_insert(self, id, node):
		self.follow_inserts.append((id, node.get("follower_id", ""), node.get("created"), node.get("deleted")))
		
	def user_like_insert(self, id, node):
		self.user_like_inserts.append((id, node["id"], node["category"], node.get("name"), node.get("created_time")))
		
	def like_insert(self, id, node):
		self.like_inserts.append((id, node["id"], node["category"], node.get("name"), node.get("created_time")))
		
	def commit(self):
		# MySQLdb runs *much* faster if we use executemany() to bulk insert.
		self.cursor.execute("BEGIN")
		if self.user_profile_inserts:
			self.cursor.executemany("""
				REPLACE INTO profile(facebook_id, is_user, user_id, name, username, first_name, last_name, profile_image, hometown, location, email, gender, birthday, ts)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
				""", self.user_profile_inserts)
		if self.friend_profile_inserts:
			# Don't overwrite any existing row if friend is also a user
			self.cursor.executemany("""
				INSERT IGNORE INTO profile(facebook_id, is_user, user_id, name, username, first_name, last_name, profile_image, hometown, location, email, gender, birthday, ts)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
				""", self.friend_profile_inserts)
		if self.friend_inserts:
			self.cursor.executemany("""
				INSERT IGNORE INTO friend(facebook_id, friend_id)
				VALUES (%s, %s)
				""", self.friend_inserts)
		if self.object_inserts:
			self.cursor.executemany("""
				REPLACE INTO object(id, url, image, title, description, price, ts, created)
				VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
		if self.sale_inserts:
			self.cursor.executemany("""
				INSERT IGNORE INTO sale(sale_id, user_id, order_number, total, ts)
				VALUES (%s, %s, %s, %s, %s)
				""", self.sale_inserts)
		if self.sale_object_inserts:
			self.cursor.executemany("""
				INSERT IGNORE INTO sale_object(sale_id, object_id, price, quantity)
				VALUES (%s, %s, %s, %s)
				""", self.sale_object_inserts)
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
		if self.user_like_inserts:
			self.cursor.executemany("""
				REPLACE INTO `user_like`(facebook_id, like_id, category, name, created)
				VALUES (%s, %s, %s, %s, %s)
				""", self.user_like_inserts)
		if self.like_inserts:
			self.cursor.executemany("""
				REPLACE INTO `like`(facebook_id, like_id, category, name, created)
				VALUES (%s, %s, %s, %s, %s)
				""", self.like_inserts)
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

