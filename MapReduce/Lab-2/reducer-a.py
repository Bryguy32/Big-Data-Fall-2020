#reducer.py
#Lab 2 part a
# Get userID and favorite movie
import sys

# users
keyCount = {}
#best movie
valCount = {}

for line in sys.stdin:
    line = line.strip()
    key, movie, rating = line.split('\t')

    movie = float(movie)
    rating = float(rating)

    keyCount[key] = key
    if rating > valCount[rating]:
        valCount[rating] = rating
        valCount[key] = movie



for key in keyCount.keys():

    print('%s\t%s' % (key, valCount[key]))
