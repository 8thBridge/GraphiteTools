import sys

class AbstractTransformer(object):
	def handle(self, node_type, id, node):
		"""
		transformers return the node or a transformed node or None
		"""
		print >> sys.stderr, "%s %s node noticed" % (node_type, id)
		return node


class TransformerChain(AbstractTransformer):
	transformers = None

	def __init__(self, transformers=None):
		self.transformers = transformers
		if self.transformers == None:
			self.transformers = list()

	def add_transformer(self, transformer):
		self.transformers.append(transformer)

	def handle(self, node_type, id, node):
		current_node = node
		for transformer in self.transformers:
			current_node = transformer.handle(node_type, id, current_node)
			if current_node == None:
				# short circuit, stop the chain if a transformer returns none
				return current_node
		return current_node
