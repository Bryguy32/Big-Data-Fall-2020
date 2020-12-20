#reducer.py
# Project 1
#Part 2
#Get number of cases where benign tumor cells
# have more radius than average malignant tumor cells

import sys

count = 0
mRadiusAvg = 0
benignTumor = []

for line in sys.stdin:
    case, diagnosis, radius = line.strip().split('\t')

    diagnosis = str(diagnosis)
    radius = float(radius)


    try:
        if diagnosis == 'M':
            mRadiusAvg += radius
            count += 1

        elif diagnosis == 'B':
            benignTumor.append(radius)


    except:
        print("ERROR Reducer")


# Get average
average = mRadiusAvg / count

# Get num of cases where benign tumor cells have
# more radius than average malignat tumor cells
numofCases = 0

for i in benignTumor:
    if i > average:
        numofCases += 1

print("Number of cases = %i" % (numofCases))
