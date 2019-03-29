from google.cloud.bigtable.row_set import RowSet, RowRange

def overlap_it(element, other):
	start_key = min(element.start_key, other.start_key)
	end_key = max(element.end_key, other.end_key)
	return RowRange(start_key, end_key)

def is_overlaped(element, other):
	if (element.start_key < other.start_key and element.end_key > other.start_key) or \
	   (other.start_key < element.start_key and other.end_key > element.start_key):
		return [overlap_it(element, other)]
	else:
		return [element, other]
def list_overlap(ranges):
	for element in ranges:
		print(element)


lista = [
	RowRange(b'beam_key0038',b'beam_key004'),
	RowRange(b'beam_key0010', b'beam_key0011'),
	RowRange(b'beam_key0037',b'beam_key0039'),
	RowRange(b'beam_key0020', b'beam_key0025'),
	RowRange(b'beam_key0035',b'beam_key0036'),	
	RowRange(b'beam_key0023', b'beam_key0024'),
]


check_list = is_overlaped(lista[4], lista[5])
print(check_list)


check_list = is_overlaped(lista[0], lista[1])
print(check_list)