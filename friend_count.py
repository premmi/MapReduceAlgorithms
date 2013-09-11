import MapReduce
import sys

"""
MapReduce algorithm to count the number of friends each person has in a social network dataset consisting of key-value pairs where each key is a person and each value is a friend of that person
implemented in the Simple Python MapReduce Framework

"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: Person name
    # value: friend count
    key = record[0]
    mr.emit_intermediate(key, 1)

def reducer(key, list_of_values):
    # key: Person name
    # value: list of occurrence counts
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
