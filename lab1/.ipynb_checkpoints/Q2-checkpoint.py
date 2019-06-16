### Q2
# sc.stop()
from pyspark import SparkContext
sc = SparkContext(appName="exercise")

# read csv file
temperature_file=sc.textFile("temperature-readings-tiny.csv")
lines = temperature_file.map(lambda line:line.split(";"))

# map necessary keys and values
# (year-month, temperature), stationNumber
year_temperature = lines.map(lambda x:(x[1][0:7],float(x[3]),x[0]))
year_temperature = year_temperature.filter(lambda x: int(x[0][0:4])>=1950 and int(x[0][0:4])<=2014 and int(x[1])>=10)
countsource1 = year_temperature.map(lambda x:(tuple([x[0],x[2]]),1))

# if a station reported a reading above 10 degrees in some month, 
# then it appears only once in the count for that month.
unique1 = countsource1.distinct()
# print(*unique1.take(10), sep="\n")

# year, month, #readings
countsource2 = unique1.map(lambda x:(x[0][0],1))
counts2 = countsource2.reduceByKey(lambda v1,v2:v1+v2)
counts = counts2.map(lambda x:(x[0][0:4],x[0][5:7],x[1]))
# print(*counts.take(30),sep="\n")
counts.saveAsTextFile("counts")