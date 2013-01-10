from graphite.transform import AbstractTransformer
from datetime import datetime
from graphite import NODE_TYPE_USER


def flatten_dict(source, prefix=None):
	result = {}
	path = []
	if prefix:
		path.append(prefix)
	for name in source:
		value = source[name]
		pathed = ".".join(path + [name])
		if isinstance(value, dict):
			result.update(flatten_dict(value, prefix=pathed))
		else:
			result[pathed] = value
	return result


class FlatMapper(AbstractTransformer):
	"""
	Turns the node into a flat dict with . separated key paths.
	Optionally pass in includes or excludes to filter the results.
	"""
	includes = None
	excludes = None

	def __init__(self, includes=None, excludes=None):
		if includes:
			self.includes = includes
		if excludes:
			self.excludes = excludes
		if includes and excludes:
			raise Exception("can't include when you are also excluding")

	def handle(self, node_type, id, node):
		flat = flatten_dict(node)
		result = dict()
		# I suspect there is a better way to do this, but for now this is what we get.
		if self.includes:
			for key in flat:
				if key in self.includes:
					result[key] = flat[key]
		elif self.excludes:
			for key in flat:
				if key not in self.excludes:
					result[key] = flat[key]
		else:
			result = flat
		return result


class UsersFriendsTransformer(AbstractTransformer):
	def handle(self, node_type, id, node):
		if node_type == NODE_TYPE_USER:
			user_id = node.get("id")
			friends = node.get("friends", [])
			for friend in friends:
				if isinstance(friend, dict):
					yield {"id": user_id, "friend": friend.get("id")}
				else:
					yield {"id": user_id, "friend": friend}

		else:
			yield node


class SQLDateFormatTransform(AbstractTransformer):
	"""
	Turns specified fields into SQL formatted dates from the ISO date format.
	"""
	fields = None

	def __init__(self, fields=None):
		if fields:
			self.fields = fields
		else:
			self.fields = list()

	def handle(self, node_type, id, node):
		if self.fields:
			for field in self.fields:
				if field in node:
					node[field] = self.reformat(node[field])
		return node

	def get_datetime(self, val):
		"""
		Tiny helper to let us use microsecond precision unix timestamps and
		conver to a python datetime.
		"""
		parts = val.split(".")
		ts = datetime.utcfromtimestamp(float(parts[0]))
		if len(parts) == 2:
			ts = ts.replace(microsecond=int(parts[1]))
		return ts
	def parsedate(self, date_string):
		try:
			return self.get_datetime(date_string)
		except:
			pass
		try:
			return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
		except:
			return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")

	def reformat(self, value):
		if value is None:
			return ""
		if not isinstance(value, datetime):
			dte = self.parsedate(value)
		else:
			dte = value
		return datetime.strftime(dte, '%Y-%m-%d %H:%M:%S')
