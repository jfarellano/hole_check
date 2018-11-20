# import xml.etree.ElementTree as ET
# from math import radians, cos, sin, asin, sqrt
# import time
# start_time = time.time()

# #Var declaration
# class trip:
# 	def __init__(self, id, length):
# 		self.id = int(id)
# 		self.length = length
# 	def __str__(self):
# 		return "Trip %s: length=%s" % (self.id, self.length)

# class point:
# 	def __init__(self, lon, lat, intensity, trip_id, id):
# 		self.id = id
# 		self.trip_id = int(trip_id)
# 		self.lon = float(lon)
# 		self.lat = float(lat)
# 		self.intensity = float(intensity)

# 	def __str__(self):
# 		return "id:%s trip=%s long=%s lati=%s inten=%s" % (self.id, self.trip_id, self.lon, self.lat, self.intensity)
# points = []
# trips = []
# holes = []
# diameter = 20
# threshold = 0.8

# def inRage(P, point):
# 	return haversine(P.lon, P.lat, point.lon, point.lat) <= diameter

# def avrage(list):
# 	sum = 0
# 	for l in list:
# 		sum += l.intensity
# 	return float(sum / max(len(list), 1))

# def haversine(lon1, lat1, lon2, lat2):
#     lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
#     # haversine formula 
#     dlon = lon2 - lon1 
#     dlat = lat2 - lat1 
#     a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
#     c = 2 * asin(sqrt(a)) 
#     r = 6371000
#     return c * r
	
# #parse xml data
# tree = ET.parse('raw_min.xml')
# root = tree.getroot()
# #Load to objects xml data
# maxId = 0
# c_trip = 0
# l_lat = 0#11.02038629
# l_lon = 0#-74.85099406
# length = 0
# print 'Loading data'
# for child in root:
# 	for i, rows in enumerate(child):
# 		if i != 0:
# 			trip_id = int(rows[1].text)
# 			lon = float(rows[4].text)
# 			lat = float(rows[3].text)
# 			inten = float(rows[7].text)
# 			points.append(point(lon, lat, inten, trip_id, i))
# 			if trip_id > maxId:
# 				maxId = trip_id
# 			if c_trip != trip_id:
# 				trips.append(trip(c_trip, length))
# 				length = 0
# 				c_trip = trip_id
# 			else:
# 				length += haversine(l_lon, l_lat, lon, lat)
# 			l_lon = lon
# 			l_lat = lat
# trips.append(trip(c_trip, length))

# # 1 trip_id
# # 3 lat
# # 4 lon
# # 7 intensity

# print 'Normalizing data'
# #normalize trip 
# for i in xrange(1,maxId +1):
# 	filtered = filter(lambda x: x.trip_id == i, points)
# 	if len(filtered) > 0:
# 		big = max(filtered, key=lambda p: p.intensity).intensity
# 		for f in filtered:
# 			f.intensity = f.intensity/big

# # for p in points:
# 	#print p
# print 'Sorting data'
# #sort points from big to small
# points.sort(key=lambda x: x.intensity, reverse=True)


# print 'Procesing data'
# #Get holes and markdown each that is taken for a hole
# iter = 0
# while points[iter].intensity > threshold:
# 	point = points[iter]
# 	print points[iter]
# 	filtered = filter(lambda x: inRage(point, x), points)
# 	if avrage(filtered) > threshold:
# 		holes.append(point)
# 		for f in filtered:
# 			points.remove(f)
# 	iter = iter + 1
# print("--- %s seconds ---" % (time.time() - start_time))

# #Print results
# print 'Huecos'
# for h in holes:
# 	print h
# print 'Viajes'
# for t in trips:
# 	print t




import xml.etree.ElementTree as ET
from math import radians, cos, sin, asin, sqrt
import time
import numpy as np
start_time = time.time()

#Var declaration
class trip:
	def __init__(self, id, length):
		self.id = int(id)
		self.length = length
	def __str__(self):
		return "Trip %s: length=%s" % (self.id, self.length)

class point:
	def __init__(self, lon, lat, intensity, trip_id, id):
		self.id = id
		self.trip_id = int(trip_id)
		self.lon = float(lon)
		self.lat = float(lat)
		self.intensity = float(intensity)

	def __str__(self):
		return "id:%s trip=%s long=%s lati=%s inten=%s" % (self.id, self.trip_id, self.lon, self.lat, self.intensity)
points = []
trips = []
holes = []
diameter = 20
threshold = 0.8

def inRage(P, point):
	return haversine(P.lon, P.lat, point.lon, point.lat) <= diameter

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000
    return c * r
	
#parse xml data
tree = ET.parse('raw.xml')
root = tree.getroot()
#Load to objects xml data
maxId = 0
c_trip = 0
l_lat = 0#11.02038629
l_lon = 0#-74.85099406
length = 0
print 'Loading data'
for child in root:
	for i, rows in enumerate(child):
		if i != 0:
			trip_id = int(rows[1].text)
			lon = float(rows[4].text)
			lat = float(rows[3].text)
			inten = float(rows[7].text)
			points.append(point(lon, lat, inten, trip_id, i))
			if trip_id > maxId:
				maxId = trip_id
			if c_trip != trip_id:
				trips.append(trip(c_trip, length))
				length = 0
				c_trip = trip_id
			else:
				length += haversine(l_lon, l_lat, lon, lat)
			l_lon = lon
			l_lat = lat
trips.append(trip(c_trip, length))

# 1 trip_id
# 3 lat
# 4 lon
# 7 intensity

print 'Normalizing data'
#normalize trip 
for i in xrange(1,maxId +1):
	filtered = filter(lambda x: x.trip_id == i, points)
	if len(filtered) > 0:
		big = max(filtered, key=lambda p: p.intensity).intensity
		for f in filtered:
			f.intensity = f.intensity/big

# for p in points:
	#print p
print 'Sorting data'
#sort points from big to small
points.sort(key=lambda x: x.intensity, reverse=True)


print 'Procesing data'
#Get holes and markdown each that is taken for a hole
iter = 0
while points[iter].intensity > threshold:
	point = points[iter]
	print points[iter]
	filtered = []
	intensities = []
	i = 0
	while i < len(points):
		if inRage(point, points[i]):
			filtered.append(points[i])
			intensities.append(points[i].intensity)
			del points[i]
			i = i - 1
		i = i + 1

	if np.mean(intensities) > threshold:
		holes.append(point)
		iter = iter - 1
	iter = iter + 1

time = time.time() - start_time

#Print results
print 'Huecos'
for h in holes:
	print h
print 'Viajes'
for t in trips:
	print t


print time
