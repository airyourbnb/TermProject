
countries = []
amens = []
with open("CountryOnly_AverageAmenities.csv",newline='') as f1:
    lines = f1.readlines()
    for x in range (0,len(lines)):
        countries.append([])
        
        entries = lines[x].replace("[","").replace("]","").replace("\"","").replace(" ","").split(",")
        country = entries[0];
        countries[x].append(country)
        for y in range(1, len(entries)):
            countries[x].append(entries[y].lower())
            if(not entries[y].lower() in amens ): 
                amens.append(entries[y].lower())

amens = sorted(amens)
for am in amens:
    print(am)

results = []

header = "Country"
for am in amens:
    header += "," + am.replace("\n","")


results.append(header)
    
for country in countries:
    res = country[0]
    for am in amens:
        if am in country:
            res += ",yes"
        else: res += ",no"
    results.append(res)

import csv
with open('res.csv', 'w') as writer:
    for line in results:
        writer.write(line + "\n")
