###Q4 final
# sc.stop()
from pyspark import SparkContext
sc = SparkContext(appName="exercise")
def max_temperature(a,b):
    if a>=b:
        return a
    else:
        return b
    
precipitation_file=sc.textFile("precipitation-readings-tiny.csv")
linesp = precipitation_file.map(lambda line:line.split(";"))

temperature_file=sc.textFile("temperature-readings-tiny.csv")
linest = temperature_file.map(lambda line:line.split(";"))

temperatures = linest.map(lambda x:((x[0],x[1][0:4]),float(x[3])))
precipitations = linesp.map(lambda x:((x[0],x[1][0:4]),float(x[3])))

max_temperatures = temperatures.reduceByKey(max_temperature)  # for each year
max_precipitations = precipitations.reduceByKey(max_temperature)
# print(*max_precipitations.take(10), sep="\n")

station = max_temperatures.join(max_precipitations)
# print(*station.take(10), sep="\n")

station = station.filter(lambda x: x[1][0]>=25 and x[1][0]<=30 and x[1][1]>=100 and x[1][1]<=200)
station = station.map(lambda x:(x[0],x[1][0],x[1][1]))
# print(*station.take(10), sep="\n")

station.saveAsTextFile("station")