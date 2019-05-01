import sys
file1 = sys.argv[1]

import os

directory_in_str = sys.argv[2]

directory = os.fsencode(directory_in_str)

results = []

for file in os.listdir(directory):
    fileTemp = os.fsdecode(file)
    filename = sys.argv[2] + "/" + fileTemp  
    #print(filename)
    f1Ranks = []
    f2Ranks = []

    
    with open(file1,newline='') as f1:
        lines = f1.readlines()
        for line in lines:
            country = line.split(',')[0][1:]
            if country != 'Vanuatu' and country != 'Vatican City':
                f1Ranks.append(country)


    with open(filename,newline='') as f2:
        lines = f2.readlines()
        for line in lines:
            country = line.split(',')[0][1:]
            if country != 'Vanuatu' and country != 'Vatican City':
                f2Ranks.append(country)

    offsetSum = 0
            
    for i in range (0,len(f1Ranks)):
        country = f1Ranks[i]
        f1Location = i
        #print(i)
        f2Location = 0
        for k in range (0,len(f2Ranks)):
            #print(country + " " + f2Ranks[k])
            if country == f2Ranks[k]: 
                f2Location = k
                #print(str(i) + " " + str(k))
        
        offsetSum += abs(f1Location - f2Location)
        #print(offsetSum)
            
    results.append(str(offsetSum) + " \t" + fileTemp )
    

for x in sorted(results):
    print(x)
