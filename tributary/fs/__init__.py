

class FileDataSource(bases.BaseDataSource):
    """FileDataSource is a base class for all data sources which come from files."""
    def __init__(self, name, filename):
        super(FileDataSource, self).__init__(name)
        self.name = name
        self.filename = filename
        self.results.setParam("filename", self.filename)

class DatabaseDataSource(bases.BaseDataSource):
    """This is the base class for any/all data source which are derived from a database."""
    def __init__(self, login_string, query, name="DatabaseDataSource"):
        super(DatabaseDataSource, self).__init__(name)
        self.query = query
        self.login = login_string

    def preProcess(self, _input=None):
        self.session = new_session(self.login)
        self.query_results = self.session.execute(self.query).fetchall()

    def postProcess(self, input=None):
        self.session.close()

class ConcatenateDataSource(ParallelConcatenateDataSource):
    """This data source concatenates several data sources together into one executing them concurrently."""
    """ParallelConcatenateDataSource"""

    def postProcess(self, input):
            self.sortByTime()

class RecursiveFileDataSource(FileDataSource):
    """Recursively fetches a list of files from a directory."""
    def __init__(self, name, root_dir):
        super(RecursiveFileDataSource, self).__init__(name, root_dir)

    def _add_file(self, arg, dirname, fnames):
        for name in fnames:
            self.addDataPoint(bases.DataPoint(filename="%s/%s" % (dirname, name)))
            #self.logfiles.append("%s/%s" % (dirname, name))

    def process(self, _input):
        os.path.walk(self.filename, self._add_file, 0)


class GlobDataSource(FileDataSource):
    """GlobDataSource fetches all files matching the glob string"""
    def __init__(self, name, glob_string):
        super(GlobDataSource, self).__init__(name, glob_string)

    def process(self, input):
        import glob
        self.log("Globbing %s"% self.filename)
        filenames = glob.glob(self.filename)
        self.log("Found %i matching files" % len(filenames))
        for filename in filenames:
            self.addDataPoint(bases.DataPoint(filename=os.path.abspath(filename)))


class DelimFileDataSource(FileDataSource):
        """Basic data source for delimited files. Supports both structured and unstructured data files.

                :param name: Name of the processing node
                :type name: str
                :param filename: Name of file to be processed
                :type filename: str
                :param delim: Field separator (default - ",")
                :type delim: str
                :param cols: List of column names (None - columns are auto-named)
                :type cols: List or Tuple
                :param skip_pre_header_lines: Number of lines to skip before reading header (has_header can be false)
                :type skip_pre_header_lines: int
                :param skip_post_header_lines: Number of lines to skip after reading header (has_header can be false)
                :type skip_post_header_lines: int
                :param has_header: Does the file have a header?
                :type has_header: bool
                :param comment_char: Lines that start with this are skipped
                :type comment_char: str
                :param skip_lines_without_delim: Skip lines that do not contain the specified delimiter
                :type skip_lines_without_delim: bool

                .. note::

                        * If both `skip_pre_header_lines` and `skip_post_header_lines` are specified and has_header is **False**, then the summation of them is skipped.
                        * If `cols` is given, then `has_header` is automatically set to **False**.
                        * `comment_char` can be set to None to forfeit the functionality.
                        * If `cols` is **None** and `has_header` is **False**, the data is loaded as 'Column_1', 'Column_2', etc.

        """
        def __init__(self, name, filename, delim=",", cols=None, skip_pre_header_lines=0,
                        skip_post_header_lines=0, has_header=True, comment_char="#", skip_lines_without_delim=True):
            super(DelimFileDataSource, self).__init__(name, filename)

            self.delim = delim
            self.cols = cols
            self.skip_pre_header_lines = skip_pre_header_lines
            self.skip_post_header_lines = skip_post_header_lines
            self.has_header = has_header
            self.comment_char = comment_char
            self.skip_lines_without_delim = skip_lines_without_delim

            if cols is not None:
                self.has_header = False

        def split(self, line):
            if line is None:
                return ""
            if self.delim is not None:
                return re.split(self.delim, line.strip())
            else:
                return line.strip().split()

        def process(self, input):

                # open file
                with open(self.filename, "r") as fh:

                        # skip pre header lines
                        if self.skip_pre_header_lines > 0:
                                for i in xrange(self.skip_pre_header_lines):
                                        fh.readline()

                        #read header
                        if self.has_header == True:
                                self.cols = [col.strip() for col in self.split(fh.readline())]

                        self.results.cols = self.cols #why was this line before the 'with' block? --andy

                        # skip post header lines
                        if self.skip_post_header_lines > 0:
                                for i in xrange(self.skip_post_header_lines):
                                        fh.readline()

                        # compile regex of delim
                        regexp = re.compile(self.delim)
                        
                        # read lines in file
                        for line in fh:

                                line = line.replace("\x96","-")

                                # skip comments if applicable
                                if not self.comment_char is None and line.strip().startswith(self.comment_char):
                                        continue

                                # skip lines without delim if `skip_lines_without_delim` is True

                                if self.delim is not None:
                                    if self.skip_lines_without_delim is True and not regexp.search(line):
                                    # if self.skip_lines_without_delim is True and not self.delim in line:
                                            continue

                                # init data point, read fields and count them
                                point = bases.DataPoint()
                                fields = self.split(line.strip())
                                field_count = len(fields)

                                # determine what to call the data fields
                                if self.cols is not None:
                                        for i, col in enumerate(self.cols):
                                                if i < field_count:
                                                        point.setParam(col, fields[i].strip()) #I don't think it'll harm anything to strip whitespace here
                                                else:
                                                        point.setParam(col, None)
                                else:

                                        # for unstructured data, cols is None and has_header is False
                                        for i, field in enumerate(fields):
                                                point.setParam("Column_" + str(i + 1), fields[i].strip()) #I don't think it'll harm anything to strip whitespace here

                                self.addDataPoint(point)


class StubDataSource(bases.BaseDataSource):
    """StubDataSource is an empty shell which requires the
       coder to add the data points manually. This class can
       be used to create dummy data points."""
    def process(self, _input=None):
        pass

class ConcatenateDataSourceSerial(bases.BaseDataSource):
    def __init__(self, name):
        super(bases.BaseDataSource, self).__init__(name)
        self.sources = []
        self.modifiers = []
        self.states = []
        self.processed = False

    def process(self, _input=None):
        for ds in self.sources:
            ds.execute()
            dps = ds.getDataPoints()
            for dp in dps:
                self.addDataPoint(dp)
        self.processed = True

    def addDataSource(self, ds):
        self.sources.append(ds)

