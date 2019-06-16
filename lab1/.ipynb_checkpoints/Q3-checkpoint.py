###Q3 final
# sc.stop()
from pyspark import SparkContext
sc = SparkContext(appName="exercise")

# read csv file
temperature_file=sc.textFile("temperature-readings-tiny.csv")
lines = temperature_file.map(lambda line:line.split(";"))

# map necessary keys and values
# (year-month, temperature), stationNumber
year_temperature = lines.map(lambda x:(x[1][0:7],float(x[3]),x[0]))
year_temperature = year_temperature.filter(lambda x: int(x[0][0:4])>=1960 and int(x[0][0:4])<=2014)

# (year-month, stationNumber), #readings
countSource = year_temperature.map(lambda x:((x[0],x[2]),1))
counts=countSource.reduceByKey(lambda x,y: x+y)
# print(*unique.take(10), sep="\n")

# (year-month, stationNumber), tempSum
temperature = year_temperature.map(lambda x:((x[0],x[2]),x[1]))
tempSum=temperature.reduceByKey(lambda v1,v2:v1+v2)
# print(*tempSum.take(10), sep="\n")

# (year, month, stationNumber, TempAve)
aveSource=tempSum.join(counts)
aveTemp=aveSource.map(lambda x:(x[0][0][0:4],x[0][0][5:7],x[0][1],round(x[1][0]/x[1][1],2)))
# print(*aveTemp.take(30),sep="\n")

averagetemp.saveAsTextFile("aveTemp")