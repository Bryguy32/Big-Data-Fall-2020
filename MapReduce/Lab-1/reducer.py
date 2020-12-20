# reducer.py
# Find max temp for each month
import sys

lastKey = None
maxVal = 0

for line in sys.stdin:
    (key, value) = line.strip().split('\t')
    value = int(value)

    if lastKey == key:
        maxVal = max(maxVal, value)

    else:
        if lastKey:
            print '%s\t%s' % (lastKey, maxVal)
        lastKey = key
        maxVal = value

if lastKey == key:
    print '%s\t%s' % (lastKey, maxVal)




