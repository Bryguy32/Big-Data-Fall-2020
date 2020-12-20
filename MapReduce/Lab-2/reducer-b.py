#reducer.py part b
# Lab 2
# Get average rating for each movie


import sys

# ratings
valCount = {}
# num of occurances
keyCount = {}

for line in sys.stdin:
    line = line.strip()
    user,key,rating = line.split('\t')

    key = float(key)
    rating = float(rating)

    try:
        keyCount[key] = keyCount[key] + 1
        valCount[key] = valCount[key] + rating

    except:
        keyCount[key] = 1
        valCount[key] = rating

for key in keyCount.keys():
    avg = valCount[key] / keyCount[key]
    print('%s\t%s' % (key, avg))
