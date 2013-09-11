import MapReduce
import sys

"""
Relational join as a MapReduce query in the Simple Python MapReduce Framework
SELECT *

FROM Orders, LineItem

WHERE Order.order_id = LineItem.order_id
"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: Order id 
    # value: The entire record with all data
    key = record[1]
    value = record    
    
    mr.emit_intermediate(key, value)

def reducer(key, list_of_records):
    # key: Order id
    # value: The records corresponding to rows in Order and LineItem
    for i in range(1, len(list_of_records)):
      mr.emit(list_of_records[0] + list_of_records[i])

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
