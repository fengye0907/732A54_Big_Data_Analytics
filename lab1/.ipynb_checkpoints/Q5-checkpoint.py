###Q5 final
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

stations_Ostergotland=sc.textFile("stations-Ostergotland.csv")
liness = stations_Ostergotland.map(lambda line:line.split(";"))

######## the list of all the stations
stations = liness.map(lambda x:x[0]).distinct().collect()   # by collect(), output is a list
# print(*stations)

######## the daily-temperatures we need for each stations in list
daily_precipitation = linesp.map(lambda x:((x[1][0:7],x[0]),float(x[3])))
daily_precipitation = daily_precipitation.filter(lambda x: int(x[0][0][0:4])>=1996 and int(x[0][0][0:4])<=2016 and str(x[0][1]) in stations)
# print(*daily_precipitation.take(10),sep="\n")

######## the month-temperatures we need for each stations
month_precipitationSum = daily_precipitation.reduceByKey(lambda v1,v2:v1+v2)
month_precipitationSum = month_precipitationSum.map(lambda x:(x[0][1],(x[0][0],x[1])) )
# print(*month_precipitationSum.take(10),sep="\n")

monthPrecValue = month_precipitationSum.map(lambda x:(x[1]))
monthPrecValue = monthPrecValue.reduceByKey(lambda v1,v2:v1+v2)
# print(*monthPrecValue.take(2),sep="\n")

########## # of dates (year-month)
months = month_precipitationSum.map(lambda x:(x[1][0],1))
months = months.reduceByKey(lambda v1,v2:v1+v2)
# print(*months.take(10),sep="\n")

########## average
monthAve = monthPrecValue.union(months).reduceByKey(lambda v1,v2:v1/v2)
monthAve = monthAve.map(lambda x: (x[0][0:4],x[0][5:7],round(x[1],2)))
# print(*monthAve.take(10),sep="\n")

aveprec.saveAsTextFile("aveprec")