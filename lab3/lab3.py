from future import division
from math import radians, cos, sin, asin, sqrt, exp
from pyspark import SparkContext
from datetime import datetime
sc = SparkContext(appName="lab_kernel")

def haversine(lon1, lat1, lon2, lat2): 
    """
    Calculate the great circle distance between two points on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2]) 
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2 
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km
    
def timek(time1,time2):
 time1= datetime.strptime(time1, '%H:%M:%S')
 time2= datetime.strptime(time2, '%H:%M:%S')
 k=(time1-time2).seconds/(60**2)
 return abs(k)
 
    
def dayk(day1,day2):
 day1=datetime.strptime(day1, '%Y-%m-%d')
 day2=datetime.strptime(day2, '%Y-%m-%d')
 k=(day1-day2).days
 return abs(k)
 
def sumkernel(d1, d2, d3):
    """Computes a gaussian kernel."""
    k_dist = exp(- d1**2 / (2*(h_distance**2)))
    k_day = exp(- d2**2 / (2*(h_date**2)))
    k_hour = exp(- d3**2 / (2*(h_time**2)))
    
    return k_dist + k_day + k_hour
    
def mulkernel(d1, d2, d3):
    """Computes a gaussian kernel."""
    k_dist = exp(- d1**2 / (2*(h_distance**2)))
    k_day = exp(- d2**2 / (2*(h_date**2)))
    k_hour = exp(- d3**2 / (2*(h_time**2)))
    
    return k_dist * k_day * k_hour

times = ["04:00:00", "06:00:00", "08:00:00","10:00:00","12:00:00",
         "14:00:00", "16:00:00", "18:00:00","20:00:00","22:00:00",'00:00:00']

h_distance = 80# Up to you 
h_date = 7# Up to you 
h_time = 3# Up to you
la = 58.4274 # Up to you
lo = 14.826 # Up to you
date1 = "2000-07-15" # Up to you
date2 = "2000-01-15" # Up to you
stations = sc.textFile("/user/x_jiawu/data/stations.csv")
lines_stations = stations.map(lambda line:line.split(";"))
stations = lines_stations.map(lambda x:(x[0],haversine(lo,la,float(x[4]),float(x[3]))))
m=sc.parallelize(stations.collect()).collectAsMap()
stations=sc.broadcast(m)


temps = sc.textFile("/user/x_jiawu/data/temperature-readings.csv")
lines_temps = temps.map(lambda line:line.split(";"))
year_temperature = lines_temps.map(lambda x: (stations.value[str(x[0])],x[1],x[2],float(x[3])))
#year_temperature = year_temperature.sample(False,0.00001)
year_temperature1 = year_temperature.filter(lambda x: x[1]<=date1)


y_hats1=[]
for time in times:
    k_temp=year_temperature1.map(lambda x: (x[0],dayk(date1,x[1]),timek(time,x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(sumkernel(x[0],x[1],x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(x[0],x[0]*x[1]))
    k_temp=k_temp.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    y_hats1.append(k_temp[1] / k_temp[0])
    
y_hatm1=[]
for time in times:
    k_temp=year_temperature1.map(lambda x: (x[0],dayk(date1,x[1]),timek(time,x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(mulkernel(x[0],x[1],x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(x[0],x[0]*x[1]))
    k_temp=k_temp.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    y_hatm1.append(k_temp[1] / k_temp[0])
    
year_temperature2 = year_temperature.filter(lambda x: x[1]<=date2)

y_hats2=[]
for time in times:
    k_temp=year_temperature1.map(lambda x: (x[0],dayk(date2,x[1]),timek(time,x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(sumkernel(x[0],x[1],x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(x[0],x[0]*x[1]))
    k_temp=k_temp.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    y_hats2.append(k_temp[1] / k_temp[0])
    
y_hatm2=[]
for time in times:
    k_temp=year_temperature1.map(lambda x: (x[0],dayk(date2,x[1]),timek(time,x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(mulkernel(x[0],x[1],x[2]),x[3]))
    k_temp=k_temp.map(lambda x:(x[0],x[0]*x[1]))
    k_temp=k_temp.reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    y_hatm2.append(k_temp[1] / k_temp[0])

hour = [int(t[:2]) for t in times]
hour[-1]=24

import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(8, 5))

ax.plot(hour, y_hat) #y_hat can be changed by y_hats1 y_hats2 y_hatm1 y_hatm2 
ax.set_xlabel("Hour")
ax.set_ylabel("Temperature")
ax.set_title(r"$Temperature of the Day by multiply the kernels$")
ax.set_xticks(hour)

