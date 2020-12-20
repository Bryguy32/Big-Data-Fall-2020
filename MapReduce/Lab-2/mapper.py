#mapper.py
#Lab 2
# input.txt is format <userid, movieid, rating>
import sys

for line in sys.stdin:
    val = line.strip()
    user, movie, rating = line.split(',')
    print('%s\t%s\t%s') % (user,movie,rating))
