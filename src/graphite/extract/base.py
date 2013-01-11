"""
IGAPIExtractor loads data from the 8thBridge Graphite Interest Graph API. By default all data is laoded, you can optionaly pass in an offset to only load data that is newer than the offset.
"""
import requests
from graphite import NODE_TYPE_USER, NODE_TYPE_ACTION, NODE_TYPE_OBJECT
import sys


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
		print >> sys.stderr, url, feed
		response = requests.get(url)
		if response.status_code == 200:
			json = response.json()
			if json.get("status") == "OK":
				if feed == "users" or feed == "actions":
					return json.get("users", []), json.get("next")
				elif feed == "objects":
					return json.get("objects", []), json.get("next")
				else:
					print >> sys.stderr, "feed was not what we thought", feed
			else:
				print >> sys.stderr, "unexpected status code", json.get("status")
		else:
			print >> sys.stderr, "unexpected response code", response.status_code
		return [], None

	def load_users_into(self, transformer, output):
		print >> sys.stderr, ".. loading users feed"
		output.start(NODE_TYPE_USER)
		data, next = self._load_feed("users")
		self.process_set(NODE_TYPE_USER, data, transformer, output)
		pages_loaded = 0
		pages_to_load = self._options.get("pages_to_load", 0)
		while next and len(data)>0:
			print >> sys.stderr, "loading another page"
			data, next = self._load_data_from(next + "&bl=%d" % self._options["limit_per_page"], "users")
			self.process_set(NODE_TYPE_USER, data, transformer, output)
			pages_loaded += 1

			if pages_to_load > 0 and pages_to_load >= pages_loaded:
				# limiter
				next = None
		output.complete()

	def load_objects_into(self, transformer, output):
		print >> sys.stderr, ".. loading users feed"
		output.start(NODE_TYPE_OBJECT)
		data, next = self._load_feed("objects")
		self.process_set(NODE_TYPE_OBJECT, data, transformer, output)
		pages_loaded = 0
		pages_to_load = self._options.get("pages_to_load", 0)
		while next and len(data)>0:
			print >> sys.stderr, "loading another page"
			data, next = self._load_data_from(next, "objects")
			self.process_set(NODE_TYPE_OBJECT, data, transformer, output)
			pages_loaded += 1

			if pages_to_load > 0 and pages_to_load >= pages_loaded:
				# limiter
				next = None
		output.complete()

	def load_actions_into(self, transformer, output):
		print >> sys.stderr, ".. loading actions feed"
		output.start(NODE_TYPE_ACTION)
		data, next = self._load_feed("actions")
		self.process_set(NODE_TYPE_ACTION, data, transformer, output)
		pages_loaded = 0
		pages_to_load = self._options.get("pages_to_load", 0)
		while next and len(data)>0:
			print >> sys.stderr, "loading another page"
			data, next = self._load_data_from(next, "actions")
			self.process_set(NODE_TYPE_ACTION, data, transformer, output)
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
