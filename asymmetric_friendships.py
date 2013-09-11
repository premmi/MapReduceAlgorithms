import MapReduce
import sys

"""
Generate a list of all non-symmetric friend relationships in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: friend pair
    # value: friend pair
    
    sortedList = sorted(record)
    key = sortedList[0] + sortedList[1]
    mr.emit_intermediate(key, record)
    

def reducer(key, list_of_values):
    # key: friend pair
    # value: friend pairs - list size is 2 if friendship is symmetric

    if(len(list_of_values) == 1):
      mr.emit((list_of_values[0][0], list_of_values[0][1]))
      mr.emit((list_of_values[0][1], list_of_values[0][0]))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
