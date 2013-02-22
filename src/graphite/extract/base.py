"""
IGAPIExtractor loads data from the 8thBridge Graphite Interest Graph API. By default all data is laoded, you can optionaly pass in an offset to only load data that is newer than the offset.
"""
import itertools
import sys

import requests
from requests.exceptions import ConnectionError

from graphite import NODE_TYPE_USER, NODE_TYPE_ACTION, NODE_TYPE_OBJECT, NODE_TYPE_USER_BOARD, NODE_TYPE_BRAND_BOARD, NODE_TYPE_FOLLOW


class IGAPIExtractor(object):
	_options = {
		"API_HOST": "api.strcst.net",
		"API_VERSION": "v1",
		"pages_to_load": 0,
		"limit_per_page": 25,
	}

	def __init__(self, **options):
		self._options.update(options)

	def _load_feed(self, feed):
		url = "https://%(API_HOST)s/%(API_VERSION)s/igapi/%(API_KEY)s/" % self._options
		url += feed
		url += "?bl=%d" % self._options["limit_per_page"] 
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
				if i == 5:
					raise
		if response.status_code == 200:
			json = response.json()
			if json.get("status") == "OK":
				if feed in ["users", "objects", "user_boards", "brand_boards"]:
					return json.get(feed, []), json.get("next")
				elif feed == "actions":
					return json.get("users", []), json.get("next")
				elif feed == "curate_follows":
					return json.get("follows", []), json.get("next")
				else:
					print >> sys.stderr, "feed was not what we thought", feed
			else:
				print >> sys.stderr, "unexpected status code", json.get("status")
		else:
			print >> sys.stderr, "unexpected response code", response.status_code
		return [], None

	def load_users_into(self, transformer, output):
		self._load_feed_into("users", NODE_TYPE_USER, transformer, output)

	def load_objects_into(self, transformer, output):
		self._load_feed_into("objects", NODE_TYPE_OBJECT, transformer, output)

	def load_actions_into(self, transformer, output):
		self._load_feed_into("actions", NODE_TYPE_ACTION, transformer, output)

	def load_user_boards_into(self, transformer, output):
		self._load_feed_into("user_boards", NODE_TYPE_USER_BOARD, transformer, output)

	def load_brand_boards_into(self, transformer, output):
		self._load_feed_into("brand_boards", NODE_TYPE_BRAND_BOARD, transformer, output)

	def load_follows_into(self, transformer, output):
		self._load_feed_into("curate_follows", NODE_TYPE_FOLLOW, transformer, output)

	def _load_feed_into(self, feed, node_type, transformer, output):
		print >> sys.stderr, ".. loading %s feed" % feed
		output.start(node_type)
		data, next = self._load_feed(feed)
		self.process_set(node_type, data, transformer, output)
		pages_loaded = 0
		pages_to_load = self._options.get("pages_to_load", 0)
		while next and len(data)>0:
			print >> sys.stderr, "loading another page"
			data, next = self._load_data_from(next + "&bl=%d" % self._options["limit_per_page"], feed)
			self.process_set(node_type, data, transformer, output)
			pages_loaded += 1

			if pages_to_load > 0 and pages_to_load >= pages_loaded:
				# limiter
				next = None
		output.complete()

	def process_set(self, type, data, transformer, output):
		print >> sys.stderr, "processing %s data, %s records" % (type, len(data),)
		for item in data:
			id = item.get("id")
			result = transformer.handle(type, id, item)
			if result is not None:
				if isinstance(result, dict):
					output.handle(type, id, result)
				else:

					for item in result:
						output.handle(type, id, item)
		output.commit()
