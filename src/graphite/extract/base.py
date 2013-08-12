"""
IGAPIExtractor loads data from the 8thBridge Graphite Interest Graph API. By default all data is laoded, you can optionaly pass in an offset to only load data that is newer than the offset.
"""
import itertools
import sys
import time
import urllib
import urlparse

import requests
from requests.exceptions import ConnectionError

from graphite import NODE_TYPE_USER, NODE_TYPE_FRIEND, NODE_TYPE_ACTION, NODE_TYPE_OBJECT, NODE_TYPE_USER_BOARD, NODE_TYPE_BRAND_BOARD, NODE_TYPE_FOLLOW, NODE_TYPE_LIKE


class IGAPIExtractor(object):
	_options = {
		"API_HOST": "api.strcst.net",
		"API_VERSION": "v1",
		"pages_to_load": 0,
		"limit_per_page": None,
		"checkpoint": None,
	}

	def __init__(self, **options):
		# TODO: checkpoint should be specified in load_*_into() methods, not constructor options 
		self._options.update(options)

	def _load_feed(self, feed):
		url = "https://%(API_HOST)s/%(API_VERSION)s/igapi/%(API_KEY)s/" % self._options
		url += feed
		params = {}
		if self._options["limit_per_page"]:
			params["bl"] = self._options["limit_per_page"]
		if self._options["checkpoint"]:
			params["after"] = self._options["checkpoint"]
		if params:
			url += "?" + urllib.urlencode(params) 
		return self._load_data_from(url, feed)

	def _load_data_from(self, url, feed):
		# Make multiple attempts, since the server can sometimes timeout the 
		# transaction after 60 seconds.
		for i in itertools.count(start=1):
			try:
				print >> sys.stderr, url, feed
				response = requests.get(url)
				break
			except ConnectionError:
				if i == 10:
					raise
				time.sleep(60)
		if response.status_code == 200:
			json = response.json()
			if json.get("status") == "OK":
				if feed in ["users", "friends", "objects", "user_boards", "brand_boards", "likes"]:
					return json.get(feed, []), json.get("next")
				elif feed == "actions":
					return json.get("users", []), json.get("next")
				elif feed == "user_likes":
					return json.get("likes", []), json.get("next")
				elif feed == "curate_follows":
					return json.get("follows", []), json.get("next")
				else:
					print >> sys.stderr, "feed was not what we thought", feed
			else:
				print >> sys.stderr, "unexpected status code", json.get("status")
		else:
			print >> sys.stderr, "unexpected response code", response.status_code
		return [], None

	def load_users_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("users", NODE_TYPE_USER, transformer, output, checkpoint_callback)

	def load_friends_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("friends", NODE_TYPE_FRIEND, transformer, output, checkpoint_callback)

	def load_objects_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("objects", NODE_TYPE_OBJECT, transformer, output, checkpoint_callback)

	def load_actions_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("actions", NODE_TYPE_ACTION, transformer, output, checkpoint_callback)

	def load_user_boards_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("user_boards", NODE_TYPE_USER_BOARD, transformer, output, checkpoint_callback)

	def load_brand_boards_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("brand_boards", NODE_TYPE_BRAND_BOARD, transformer, output, checkpoint_callback)

	def load_follows_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("curate_follows", NODE_TYPE_FOLLOW, transformer, output, checkpoint_callback)

	def load_likes_into(self, transformer, output, checkpoint_callback=None):
		self._load_feed_into("user_likes", NODE_TYPE_LIKE, transformer, output, checkpoint_callback)

	def _load_feed_into(self, feed, node_type, transformer, output, checkpoint_callback):
		print >> sys.stderr, ".. loading %s feed" % feed
		output.start(node_type)
		data, next = self._load_feed(feed)
		self.process_set(node_type, data, transformer, output)
		pages_loaded = 0
		pages_to_load = self._options.get("pages_to_load", 0)
		while next and len(data)>0:
			print >> sys.stderr, "loading another page"
			if checkpoint_callback:
				checkpoint_callback(self.extract_checkpoint(next))
			if self._options["limit_per_page"]:
				next += "&bl=%d" % self._options["limit_per_page"]
			data, next = self._load_data_from(next, feed)
			self.process_set(node_type, data, transformer, output)
			pages_loaded += 1

			if pages_to_load > 0 and pages_to_load >= pages_loaded:
				# limiter
				next = None
		output.complete()

	def process_set(self, type, data, transformer, output):
		print >> sys.stderr, "processing %s data, %s records" % (type, len(data),)
		for item in data:
			if type == NODE_TYPE_LIKE:
				id = item.get("user")
			else:
				id = item.get("id")
			result = transformer.handle(type, id, item)
			if result is not None:
				if isinstance(result, dict):
					output.handle(type, id, result)
				else:

					for item in result:
						output.handle(type, id, item)
		output.commit()
		
	@staticmethod
	def extract_checkpoint(next_url):
		query = urlparse.urlparse(next_url).query
		return str(urlparse.parse_qs(query)["after"][0])
