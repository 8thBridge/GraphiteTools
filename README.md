GraphiteTools
=============

Tools for accessing and analyzing data in 8thBridge Graphite


The Library is divided into 3 parts, graphite.extract, graphite.transform and graphite.load.  Extract does the pull from the Interest Graph API. Transform applies filters and transforms to the data.  Lastly Load will load the resulting data into a new datasource, such as sqllite or csv files. Transform and load are extensible and the whole process is open ended.  The extraction is the only part that doesn't really need any modifications to just run.

To process your interest graph data you need at the IGAPIExtractor a transformer and an output. Future releases may make transform optional, current output formats require at least one.


Requirements:
=============
Requests - currently tested on version 0.14.2.
unicodecsv(optional) - CSV output does better using unicodecsv, will fall back on csv from the library, but some records may fail to be exported.


Examples:
=============
TODO
