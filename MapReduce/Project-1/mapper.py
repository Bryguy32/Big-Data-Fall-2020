#mapper.py
#Project 1

import sys

for line in sys.stdin:
    val = line.strip()
    try:
        case,diagnosis,radius, everythingElse = val.split(',', 3)
        print("%s\t%s\t%s" % (case, diagnosis,radius))

    except:
        print("ERROR MAPPER")
~                                                                                                                                                                                                           
~                                                                                                                                                                                                           
~                                            
