# mapper
import sys

for line in sys.stdin:
        x = line.strip()
        (month, temp) = x.split(',')
        print '%s\t%s' % (month,temp)
