from pyspark import SparkContext, SparkConf

import sys, os, string, queue
from multiprocessing import Process, Manager
import random as random
from threading import Thread, Lock

def create_filler(fillers):
	fillers.append("".join((random.choice(string.ascii_uppercase) for x in range(78))))

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage:")
		print("teragen.py <number of rows> <output directory>")
		exit()

	home_dir = os.getenv("HOME")
	out_dir = str(home_dir) + str(sys.argv[2])


	if os.path.isdir(out_dir) == False:
		print("Please enter an existing path.")
		exit()

	file_name = "demofile2.txt"
	complete_path = os.path.join(out_dir, file_name)

	conf = SparkConf().setAppName("Teragen")
	sc = SparkContext(conf=conf)

	f = open(complete_path, "w")
	for i in range(int(sys.argv[1])):
		filler = "".join((random.choice(string.ascii_uppercase) for x in range(78)))
		f.write(filler + "\r\n")
	f.close()

"""	with Manager() as manager:
		fillers = manager.list()  # <-- can be shared between processes.
		processes = []
		for i in range(5):
			p = Process(target=create_filler, args=(fillers,))  # Passing the list
			p.start()
			processes.append(p)
		for p in processes:
			p.join()
		print(len(fillers))
"""
"""	f = open(complete_path, "w")
	for i in range(len(fillers)):
		f.write(fillers[i] + "\r\n")

	f.close()
"""
"""	filler = fillers.get()
	f = open(complete_path, "w")
#	for i in range(int(sys.argv[1])):
	f.write(filler + "\r\n")
	f.close()

	if t1.is_alive():
        	print('Still running')
	else:
        	print('Completed')
"""
"""	for i in range(int(sys.argv[1])):
		fillers.append(filler)

	f = open(complete_path, "w")

	for i in range(int(sys.argv[1])):
		f.write(fillers[i] + "\r\n")
	f.close()
"""
"""	f = open(complete_path, "w")
	for i in range(int(sys.argv[1])):
		filler = create_filler()
		f.write(filler + "\r\n")
	f.close()
"""

"""	fillers = list()
	for i in range(int(sys.argv[1])):
		filler = create_filler()
		fillers.append(filler)

	f = open(complete_path, "w")

	for i in range(int(sys.argv[1])):
		f.write(fillers[i] + "\r\n")
	f.close()
"""
"""	t1 = Thread(target=parallel, args=(string.ascii_uppercase, complete_path, 1))
	t2  = Thread(target=parallel, args=(string.ascii_uppercase, complete_path, 2))
	t3 =  Thread(target=parallel, args=(string.ascii_uppercase, complete_path, 3))
	t4 =  Thread(target=parallel, args=(string.ascii_uppercase, complete_path, 4))
	t1.start()
	t2.start()
	t3.start()
	t4.start()
"""

"""	rdd = sc.textFile(complete_path)
	lineLengths = rdd.map(lambda s: len(s))
	totalLength = lineLengths.reduce(lambda a, b: a + b)"""
