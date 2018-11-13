import apache_beam as beam
import logging
from apache_beam.io import iobase
from apache_beam.io.iobase import SourceBundle
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

from google.cloud import bigtable

from google.cloud.bigtable.batcher import MutationsBatcher


class WriteToBigtable(beam.DoFn):
    """ Creates the connector can call and add_row to the batcher using each
    row in beam pipe line

    :type beam_options: class:`~bigtable_configuration.BigtableConfiguration`
    :param beam_options: Class `~bigtable_configuration.BigtableConfiguration`.

    :type flush_count: int
    :param flush_count: (Optional) Max number of rows to flush. If it
    reaches the max number of rows it calls finish_batch() to mutate the
    current row batch. Default is FLUSH_COUNT (1000 rows).

    :type max_mutations: int
    :param max_mutations: (Optional)  Max number of row mutations to flush.
    If it reaches the max number of row mutations it calls finish_batch() to
    mutate the current row batch. Default is MAX_MUTATIONS (100000 mutations).

    :type max_row_bytes: int
    :param max_row_bytes: (Optional) Max number of row mutations size to
    flush. If it reaches the max number of row mutations size it calls
    finish_batch() to mutate the current row batch. Default is MAX_ROW_BYTES
    (5 MB).

    :type app_profile_id: str
    :param app_profile_id: (Optional) The unique name of the AppProfile.
    """

    def __init__(self, beam_options, flush_count=None, max_row_bytes=None,
                 app_profile_id=None):
        super(WriteToBigtable, self).__init__(beam_options)
        self.beam_options = beam_options
        self.client = None
        self.instance = None
        self.table = None
        self.batcher = None
        self._app_profile_id = app_profile_id
        self.flush_count = flush_count
        self.max_row_bytes = max_row_bytes

    def start_bundle(self):
        if self.beam_options.credentials is None:
            self.client = bigtable.Client(project=self.beam_options.project_id,
                                          admin=True)
        else:
            self.client = bigtable.Client(
                project=self.beam_options.project_id,
                credentials=self.beam_options.credentials,
                admin=True)
        self.instance = self.client.instance(self.beam_options.instance_id)
        self.table = self.instance.table(self.beam_options.table_id,
                                         self._app_profile_id)
        self.batcher = MutationsBatcher(
            self.table, flush_count=self.flush_count,
            max_row_bytes=self.max_row_bytes)

    def process(self, row):
        # row.table = self.table
        self.batcher.mutate(row)

    def finish_bundle(self):
        return self.batcher.flush()


class ReadFromBigtable(iobase.BoundedSource):
    """ Bigtable apache beam read source
    :type split_keys: dict
                        '~end_key'}`.
    :type beam_options:
    class:`~bigtable_configuration.BigtableReadConfiguration`
    :param beam_options: Class
    `~bigtable_configuration.BigtableReadConfiguration`.
    """

    def __init__(self, beam_options):
        logging.info("init ReadFromBigtable")
        self.beam_options = beam_options
        self.table = None

    def _getTable(self):
        if self.table is None:
            options = self.beam_options
            client = bigtable.Client(
                project=options.project_id,
                credentials=self.beam_options.credentials)
            instance = client.instance(options.instance_id)
            self.table = instance.table(options.table_id)
        return self.table

    def __getstate__(self):
        return self.beam_options

    def __setstate__(self, options):
        self.beam_options = options
        self.table = None

    def estimate_size(self):
        logging.info("ReadFromBigtable estimate_size")
        size = [k.offset_bytes for k in self._getTable().sample_row_keys()][-1]
        logging.info(size)
        return size

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        logging.info("ReadFromBigtable split")
        sample_row_keys = self._getTable().sample_row_keys()
        start_key = b''
        for sample_row_key in sample_row_keys:
            yield iobase.SourceBundle(1, self, start_key, sample_row_key.row_key)
            start_key = sample_row_key.row_key
        if start_key != b'':
           yield iobase.SourceBundle(1, self, start_key, b'')

    def get_range_tracker(self, start_position, stop_position):
        logging.info("ReadFromBigtable get_range_tracker")
        return LexicographicKeyRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        logging.info("ReadFromBigtable read")
        read_rows = self._getTable().read_rows(
            start_key=range_tracker.start_position(),
            end_key=range_tracker.stop_position(),
            # row_set=self.beam_options.row_set, # This needs to be handled in split
            filter_=self.beam_options.filter_)

        for row in read_rows:
            logging.info("yielding " + row.row_key)
            yield row


class BigtableConfiguration(object):
    """ Bigtable configuration variables.

    :type project_id: :class:`str` or :func:`unicode <unicode>`
    :param project_id: (Optional) The ID of the project which owns the
                        instances, tables and data. If not provided, will
                        attempt to determine from the environment.

    :type instance_id: str
    :param instance_id: The ID of the instance.

    :type table_id: str
    :param table_id: The ID of the table.
    """

    def __init__(self, project_id, instance_id, table_id):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.credentials = None


class BigtableReadConfiguration(BigtableConfiguration):
    """ Bigtable read configuration variables.

    :type configuration: :class:`~BigtableConfiguration`
    :param configuration: class:`~BigtableConfiguration`

    :type row_set: :class:`row_set.RowSet`
    :param row_set: (Optional) The row set containing multiple row keys and
                    row_ranges.

    :type filter_: :class:`.RowFilter`
    :param filter_: (Optional) The filter to apply to the contents of the
                    specified row(s). If unset, reads every column in
                    each row.
    """

    def __init__(self, project_id, instance_id, table_id, row_set=None, filter_=None):
        super(BigtableReadConfiguration, self).__init__(project_id, instance_id, table_id)
        self.row_set = row_set
        self.filter_ = filter_
