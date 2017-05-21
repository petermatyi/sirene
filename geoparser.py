from Bio import Geo
handle = open('./GDS3292/GDS3292_full.soft')
records = Geo.parse(handle)
for record in records:
    print record
handle.close()
