#reducer.py
# Project 1
#Part 1
# Get average radius of maligant tumor cells
import sys

count = 0
radiusAvg = 0

for line in sys.stdin:
    case, diagnosis, radius = line.strip().split('\t')

    diagnosis = str(diagnosis)
    radius = float(radius)


    try:
        if diagnosis == 'M':
            radiusAvg += radius
            count += 1

    except:
        print("ERROR Reducer")


# Get average
average = radiusAvg / count
print("Average = %f" % (average))
