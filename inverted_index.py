import MapReduce
import sys

"""
Inverted Index (an inverted index is a dictionary where each word is associated with a list of the document identifiers in which that word appears) in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
    # key: word
    # value: list of document identifiers
    mr.emit((key, list(set(list_of_values))))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
