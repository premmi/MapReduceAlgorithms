import MapReduce
import sys

"""
A MapReduce algorithm to compute matrix multiplication: A x B in the Simple Python MapReduce Framework

"""

mr = MapReduce.MapReduce()

def mapper(record):
    # key: row,col
    # value: records in A corresponding to row and in B corresponding to col
    for i in range(0, 5):
      if (record[0] == "a"):
        key = str(record[1]) + str(i)
      else:
        key = str(i) + str(record[2]) 
      mr.emit_intermediate(key, record)
    

def reducer(key, list_of_values):
    # key: row,col
    # value: records in A corresponding to row and in B corresponding to col
            
    a = list()
    b = list()
    for v in list_of_values:
      if v[0] == "a":
        a.append(v)
      else:
        b.append(v)  
    i = int(key[0])
    j = int(key[1])
    sum = 0
    for k in range(0, len(a)):
      for l in range(0, len(b)):
        if (a[k][2] == b[l][1]):
          sum += (a[k][3] * b[l][3])
    mr.emit((i, j, sum)) 
 

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)