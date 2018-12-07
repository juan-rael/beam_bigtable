def split(desiredBundleSizeBytes, options):
	maximumNumberOfSplits = 4000
	sizeEstimate = get_estimated_size_bytes(options)
	self.desired_bundle_size_bytes = max( sizeEstimate / maximumNumberOfSplits, self.desired_bundle_size_bytes)
	# Delegate to testable helper.
	splits = self.split_based_on_samples(desired_bundle_size_bytes, self.get_sample_row_keys(options))
def get_estimated_size_bytes(self, options):
	# Delegate to testable helper.
	if self.estimated_size_bytes is None:
		self.estimated_size_bytes = self.get_estimated_size_bytes_based_on_samples( self._getTable().sample_row_keys(options) )
	return self.estimated_size_bytes
# Computes the estimated size in bytes based on the total size of all samples that overlap the
# key ranges this source will scan.
def get_estimated_size_bytes_based_on_samples(sample_row):
	estimated_size_bytes = 0
	last_offset = 0
	current_start_key = None
	# Compute the total estimated size as the size of each sample that overlaps the scan range.
    # TODO: In future, Bigtable service may provide finer grained APIs, e.g., to sample given a
    # filter or to sample on a given key range.
	for respose in samples:
		current_end_key = int.from_bytes(response.row_key(), byteorder='big')
		current_offset = response.offset_bytes()
		if current_start_key is not None and current_start_key == current_end_key:
			# Skip an empty region.
			last_offset = current_offset
			continue
		else:
			for range in ranges:
				if overlaps(current_start_key, current_end_key):
				estimated_size_bytes += current_offset - last_offset
				# We don't want to double our estimated size if two ranges overlap this sample
              	# region, so exit early.
				break
		current_start_key = current_end_key
		last_offset = current_offset
	return estimated_size_bytes