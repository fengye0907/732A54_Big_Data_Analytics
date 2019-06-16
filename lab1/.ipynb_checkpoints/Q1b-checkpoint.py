import csv
with open("temperature-readings-tiny.csv","r") as csvFile:
    reader = csv.reader(csvFile,delimiter=';')
    data = [(row[1][0:4],(float(row[3]),row[0])) for row in reader if int(row[1][0:4])>=1950 and int(row[1][0:4])<=2014]

year = range(1950,2015)

max_temperature = []
for j in range(len(year)):
#     data_temp is all temperatures for a specific year
    data_temp = [x[1] for x in data if int(x[0])==year[j]]
    maxValue = data_temp[0][0]
    maxIndex = 0
    for i in range(len(data_temp)-1):
        tempIndex = i+1
        if(data_temp[tempIndex][0]>maxValue):
            maxValue = data_temp[tempIndex][0]
            maxIndex = tempIndex
#             add the station number and temperature for a specific year
    max_temperature.append( (maxValue,data_temp[maxIndex][1]) )
    
max_temperatureSorted = sorted(dict(zip(year, max_temperature)).items(), key = lambda x:x[1][0], reverse = True)
max_temperatureSorted.saveAsTextFile("max_temperatureSorted2")

min_temperature = []
for j in range(len(year)):
#     data_temp is all temperatures for a specific year
    data_temp = [x[1] for x in data if int(x[0])==year[j]]
    minValue = data_temp[0][0]
    minIndex = 0
    for i in range(len(data_temp)-1):
        tempIndex = i+1
        if(data_temp[tempIndex][0]<minValue):
            minValue = data_temp[tempIndex][0]
            minIndex = tempIndex
#             add the station number and temperature for a specific year
    min_temperature.append( (minValue,data_temp[minIndex][1]) )
    
min_temperatureSorted = sorted(dict(zip(year, min_temperature)).items(), key = lambda x:x[1][0], reverse = True)
min_temperatureSorted.saveAsTextFile("min_temperatureSorted2")