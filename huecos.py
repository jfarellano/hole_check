from math import radians, cos, sin, asin, sqrt
from mpi4py import MPI
from sys import argv
import xml.etree.ElementTree as ET
import numpy as np
import time
import sys	

start_time = time.time()

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

points = []
trips = []
holes = []

results = []
#data = []

diameter = int(argv[1]) #20 #Parametro uno..!
threshold = float(argv[2]) #0.8 #Parametro dos..!

maxId = 0
c_trip = 0
l_lat = 0 #11.02038629
l_lon = 0 #-74.85099406
length = 0

iter = 0
procesing = 1
start = False

#Var declaration
class trip:
	def __init__(self, id, length):
		self.id = int(id)
		self.length = length
	def __str__(self):
		return "Trip %s: length=%s" % (self.id, self.length)

class point:
	def __init__(self, lon, lat, intensity, trip_id):
		self.trip_id = int(trip_id)
		self.lon = float(lon)
		self.lat = float(lat)
		self.intensity = float(intensity)
		self.available = True

	def __str__(self):
		return "Trip %s: long=%s lati=%s inten=%s av=%s" % (self.trip_id, self.lon, self.lat, self.intensity, self.available)

def inRage(P, point):
	return haversine(P.lon, P.lat, point.lon, point.lat) <= diameter

def avrage(list):
	sum = 0
	for l in list:
		sum += l.intensity
	return float(sum / max(len(list), 1))

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

#points = []
#trips = []
#holes = []
#diameter = 20 #Parametro uno..!
#threshold = 0.8 #Parametro dos..!

if rank == 0:
	#MAIN CORE
	sys.stdout.write('Loading data \n')
	for child in root:
		for i, rows in enumerate(child):
			if i != 0:
				trip_id = int(rows[1].text)
				lon = float(rows[4].text)
				lat = float(rows[3].text)
				inten = float(rows[7].text)
				points.append(point(lon, lat, inten, trip_id))
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

	sys.stdout.write('Normalizing data \n') #Could be Par
	#normalize trip 
	for i in xrange(1,maxId +1):
		filtered = filter(lambda x: x.trip_id == i, points)
		if len(filtered) > 0:
			big = max(filtered, key=lambda p: p.intensity).intensity
			for f in filtered:
				f.intensity = f.intensity/big

	# for p in points:
		#print p
	sys.stdout.write('Sorting data \n') #Could be Par
	#sort points from big to small
	points.sort(key=lambda x: x.intensity, reverse=True)

	sys.stdout.write('Size - '+str(size)+'\n')
	sys.stdout.write('Rank - '+str(rank)+'\n')
	#MAIN CORE - FINISHED!
	
	#ORQUESTER
	start = True
	comm.bcast(start, root=0)
	while points[iter].intensity > threshold: #SEARCHING POINTS TO SEND...
		point = points[iter]
		filtered = []
		intensities = []
		i = 0
		while i < len(points):
			#if points[i].available:
			if inRage(point, points[i]):
				filtered.append(points[i])
				intensities.append(points[i].intensity)
				#points[i].available = False	
				del points[i]
				i = i - 1
			i = i + 1
		sys.stdout.write('Sending '+str(point.lon)+','+str(point.lat)+' to rank '+str(procesing)+' on Rank: '+str(rank)+' With intensity '+str(point.intensity)+'\n')
		array = [intensities, point.lon, point.lat, point.intensity, point.trip_id]
		comm.send(array, dest=procesing) 
		procesing = (procesing + 1) % size
		if procesing == 0: procesing = 1;
		iter = iter + 1
	start = False
	for i in range(1,size): comm.send(start, dest=i)
	for i in range(1,size):
		results = comm.recv(source=i)		
		for x in range(0,len(results)): holes.append(results[x])
	#sys.stdout.write(str(holes)+'\n')	

	#Print results
	print 'Huecos'
	for h in holes:
		print h
	print 'Viajes'
	for t in trips:
		print t	

	#sys.stdout.write('Huecos \n')
	#for h in holes:
	#	sys.stdout.write(h)
	#	sys.stdout.write('\n')	
	#sys.stdout.write('Viajes \n')
	#for t in trips:
	#	sys.stdout.write(t)
	#	sys.stdout.write('\n')

else:
	start = comm.bcast(start, root=0) #WAIT FOR MAIN CORE TO FINISH...
	sys.stdout.write('Rank: '+ str(rank)+' Alive\n')
	while start:
		array = None
		array = comm.recv(array, source=0) #WAITING FOR POINT TO BE SEND...
		sub_time = time.time()
		if isinstance(array, bool):
			start = False
		else:
			filtered = array[0]
			point_lon =  array[1]
			point_lat = array[2]
			point_intensity =  array[3]
			point_trip_id = array[4]
			if np.mean(filtered) > threshold:
				results.append(np.array([point_lon, point_lat, point_intensity, point_trip_id]))
			sys.stdout.write('Rank - ' + str(rank) + ' ready on ' + str(sub_time - time.time())+'\n')	
	comm.send(results, dest=0)