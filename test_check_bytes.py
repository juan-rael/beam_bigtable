class BBigTableSource(BigTableSource):
  def __init__(self, *argv, **kwargv):
    super(BBigTableSource, self).__init__(*argv, **kwargv)

  def split_range_subranges(self,
                            sample_size_bytes,
                            desired_bundle_size,
                            ranges):
    ''' This method split the range you get using the
    ``desired_bundle_size`` as a limit size, It compares the
    size of the range and the ``desired_bundle size`` if it is necessary
    to split a range, it uses the ``fraction_to_position`` method.
    :param sample_size_bytes: The size of the Range.
    :param desired_bundle_size: The desired size to split the Range.
    :param ranges: the Range to split.
    '''
    start_key = ranges.start_position()
    end_key = ranges.stop_position()
    last_key = ranges.stop_position()

    #yield iobase.SourceBundle(sample_size_bytes, self, start_key, end_key)
    

    split_ = float(desired_bundle_size)/float(sample_size_bytes)
    split_count = int(math.ceil(split_))
    portions = (1/split_)
    print(portions)
    if portions > 1:
      for i in range(split_count):
        estimate_position = ((i + 1) * split_)

        for i in range(int(portions)):
          temp_estimate = estimate_position*(i+1)
          position = self.fraction_to_position(temp_estimate,
                                               ranges.start_position(),
                                               ranges.stop_position())
          end_key = position
          yield iobase.SourceBundle(sample_size_bytes * split_,
                                    self,
                                    start_key,
                                    end_key)
          start_key = position
      yield iobase.SourceBundle(sample_size_bytes, self, start_key, last_key)
    else:
      yield iobase.SourceBundle(sample_size_bytes, self, start_key, end_key)