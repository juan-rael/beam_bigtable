ranges = tuple([(str(i),str(i+row_count/10000)) for i in xrange(0, 100000000, row_count/10000)])

print(ranges)