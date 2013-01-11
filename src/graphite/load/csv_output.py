from graphite.load import AbstractOutputFormat

try:
	import unicodecsv as csv
except ImportError:
	import csv

import codecs
import cStringIO
import sys


class CSVOutput(AbstractOutputFormat):
	def __init__(self, f, columns, auto_flush=True, dialect=csv.excel, encoding="utf-8", **kwds):
		# Redirect output to a queue
		self.queue = cStringIO.StringIO()
		self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
		self.stream = f
		self.auto_flush = auto_flush
		self.columns = columns
		self.encoder = codecs.getincrementalencoder(encoding)()

	def start(self, node_type):
		self.write_row(self.columns)

	def _format_row(self, id, row_data):
		row = list()
		for heading in self.columns:
			if heading == "id":
				row.append(id)
			else:
				try:
					row.append(unicode(row_data.get(unicode(heading, "UTF-8").encode("utf-8"), u""), "UTF-8"))
				except:
					row.append(row_data.get(unicode(heading, "UTF-8").encode("utf-8"), u""))
		return row

	def handle(self, node_type, id, node):
		formatted = self._format_row(id, node)
		self.write_row(formatted)

	def write_row(self, row):
		try:
			self.writer.writerow(row)
			# Fetch UTF-8 output from the queue ...
			data = self.queue.getvalue()
			data = data.decode("utf-8")
			# ... and reencode it into the target encoding
			data = self.encoder.encode(data)
			# write to the target stream
			self.stream.write(data)
			# empty queue
			self.queue.truncate(0)
		except Exception as e:
			print >> sys.stderr, "failed to write row", row, e

	def commit(self):
		if self.auto_flush:
			self.stream.flush()

	def complete(self):
		if self.auto_flush:
			self.stream.flush()
			self.stream.close()
