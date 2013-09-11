import MapReduce
import sys

"""
Remove the last 10 characters from each string of nucleotides, then remove any duplicates generated in the Simple Python MapReduce Framework

"""

mr = MapReduce.MapReduce()

def mapper(record):
    # value: string of nucleotides with last 10 characters removed    
    length_of_string = len(record[1]) - 10
    value = record[1][:length_of_string]
    mr.emit_intermediate(value, value)

def reducer(key, list_of_values):
    # key: string of nucleotides
    mr.emit(key)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)