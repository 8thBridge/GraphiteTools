

class AbstractOutputFormat(object):

	def start(self, node_type):
		raise Exception("unimplemented")
		pass

	def handle(self, node_type, id, node):
		raise Exception("unimplemented")
		pass

	def complete(self):
		raise Exception("unimplemented")
		pass


class MultipleOutputFormat(object):
	outputs = None

	def __init__(self, outputs=None):
		self.outputs = outputs
		if self.outputs == None:
			self.outputs = list()

	def add_output(self, output):
		self.outputs.append(output)

	def start(self, node_type):
		for output in self.outputs:
			output.start(node_type)

	def handle(self, node_type, id, node):
		for output in self.outputs:
			output.handle(node_type, id, node)

	def complete(self):
		for output in self.outputs:
			output.complete()


class TransformedOutput(object):
	output = None
	transform = None

	def __init__(self, transform=None, output=None):
		self.output = output
		self.transform = transform

	def start(self, node_type):
		self.output.start(node_type)

	def handle(self, node_type, id, node):
		results = self.transform.handle(node_type, id, node)
		for result in results:
			self.output.handle(node_type, id, result)

	def complete(self):
		self.output.complete()
