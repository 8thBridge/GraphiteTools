from graphite.transform import AbstractTransformer


class ActionsSerializer(AbstractTransformer):
	transform = None

	def __init__(self, transform):
		self.transform = transform

	def handle(self, node_type, id, node):
		user_id = node.get("id")
		objects = node.get("objects", [])
		for object_id in objects:
			actions = objects.get(object_id, [])
			for action in actions:
				action_name = action.get("name")
				result = {
					"action_id": "%s_%s_%s" % (user_id, action_name, object_id,),
					"oid": object_id,
					"uid": user_id,
					"action": action_name,
					"created": action.get("created"),
					"deleted": action.get("deleted")
				}
				if self.transform:
					result = self.transform.handle(node_type, id, result)
				yield result
