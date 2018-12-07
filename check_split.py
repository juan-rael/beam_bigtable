from __future__ import absolute_import
from __future__ import division

import logging
from builtins import range

import apache_beam as beam
from apache_beam.io import iobase
from beam_bigtable.bigtable import BigtableConfiguration,ReadFromBigtable

project_id = 'grass-clump-479'
#instance_id = 'quickstart-instance-php'
#table_id = 'bigtable-php-table'
instance_id = 'endurance'
table_id = 'perf1DFN4UF2'

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

    def __init__(self, project_id, instance_id, table_id, row_set=None, filter_=None, ranges=None):
        super(BigtableReadConfiguration, self).__init__(project_id, instance_id, table_id)
        self.row_set = row_set
        self.filter_ = filter_
        self.ranges = ranges
class RangeSource(iobase.BoundedSource):

  __hash__ = None

  def __init__(self, start, end, split_freq=1):
    assert start <= end
    self._start = start
    self._end = end
    self._split_freq = split_freq

  def _normalize(self, start_position, end_position):
    return (self._start if start_position is None else start_position,
            self._end if end_position is None else end_position)

  def _round_up(self, index):
    """Rounds up to the nearest mulitple of split_freq."""
    return index - index % -self._split_freq

  def estimate_size(self):
    return self._end - self._start

  def split(self, desired_bundle_size, start_position=None, end_position=None):
    start, end = self._normalize(start_position, end_position)
    for sub_start in range(start, end, desired_bundle_size):
      sub_end = min(self._end, sub_start + desired_bundle_size)
      yield iobase.SourceBundle(
          sub_end - sub_start,
          RangeSource(sub_start, sub_end, self._split_freq),
          None, None)

  def get_range_tracker(self, start_position, end_position):
    start, end = self._normalize(start_position, end_position)
    return range_trackers.OffsetRangeTracker(start, end)

  def read(self, range_tracker):
    for k in range(self._round_up(range_tracker.start_position()),
                   self._round_up(range_tracker.stop_position())):
      if k % self._split_freq == 0:
        if not range_tracker.try_claim(k):
          return
      yield k

  # For testing
  def __eq__(self, other):
    return (type(self) == type(other)
            and self._start == other._start
            and self._end == other._end
            and self._split_freq == other._split_freq)

  def __ne__(self, other):
    # TODO(BEAM-5949): Needed for Python 2 compatibility.
    return not self == other
class ReadSplitFromBigtable(ReadFromBigtable):
	def __init__(self, beam_options):
		super(ReadSplitFromBigtable, self).__init__(beam_options)
		self.estimated_size_bytes = None
	def get_sample_row_keys(self):
		return self._getTable().sample_row_keys()
	def split(desiredBundleSizeBytes, options):
		maximumNumberOfSplits = 4000
		sizeEstimate = get_estimated_size_bytes(options)
		self.desired_bundle_size_bytes = max( sizeEstimate / maximumNumberOfSplits, self.desired_bundle_size_bytes)
		# Delegate to testable helper.
		splits = self.split_based_on_samples(desired_bundle_size_bytes, self.get_sample_row_keys(options))
	def get_estimated_size_bytes(self, options):
		# Delegate to testable helper.
		if self.estimated_size_bytes is None:
			self.estimated_size_bytes = self.get_estimated_size_bytes_based_on_samples( self._getTable().sample_row_keys() )
		return self.estimated_size_bytes
	def no_overlap(a,b):
		return not a.intersection(b)
	# Computes the estimated size in bytes based on the total size of all samples that overlap the
	# key ranges this source will scan.
	def get_estimated_size_bytes_based_on_samples(self,sample_row):
		estimated_size_bytes = 0
		last_offset = 0
		current_start_key = RangeSource(0, 0)
		# Compute the total estimated size as the size of each sample that overlaps the scan range.
	    # TODO: In future, Bigtable service may provide finer grained APIs, e.g., to sample given a
	    # filter or to sample on a given key range.
		for response in sample_row:
			current_end_key = response.ByteSize()
			current_offset = response.offset_bytes

			if current_start_key is not None and (current_start_key == current_end_key):
				# Skip an empty region.
				last_offset = current_offset
				continue
			else:
				for _ in self.beam_options.ranges:
					if self.no_overlap(current_start_key, current_end_key):
						estimated_size_bytes += current_offset - last_offset
						# We don't want to double our estimated size if two ranges overlap this sample
						# region, so exit early.
						break
			current_start_key = current_end_key
			last_offset = current_offset
		return estimated_size_bytes

ranges = []
config = BigtableReadConfiguration(project_id, instance_id, table_id, ranges=ranges)
read_from_bigtable = ReadSplitFromBigtable(config)

for sample_row_key in read_from_bigtable.get_sample_row_keys():
	print( dir( sample_row_key ) )

	print( sample_row_key. )
	print( sample_row_key. )
	print( sample_row_key. )
	print( sample_row_key. )
	print( sample_row_key. )
#size = read_from_bigtable.estimate_size()

#size_size = read_from_bigtable.get_estimated_size_bytes(None)
#print( size )
#print( size_size )