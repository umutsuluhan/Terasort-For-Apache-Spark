from pyspark import SparkContext, SparkConf

import sys, os, string, random

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage:")
		print("terasort.py <input directory> <output directory>")
		exit()

	home_dir = os.getenv("HOME")
	
	# Get the Input Directory
	
	in_dir = str(home_dir) + str(sys.argv[1])
	
	in_file_name = "teragen"
	in_complete_path = os.path.join(in_dir, in_file_name)
	
	if os.path.isdir(in_complete_path) == False:
		print("Input path doesn't exist. Please run Teragen or enter new file path")
		exit()
	
	# Get the Output Directory
	
	out_dir = str(home_dir) + str(sys.argv[2])

	out_file_name = "terasort"
	out_complete_path = os.path.join(out_dir, out_file_name)

	if os.path.isdir(out_complete_path) == True:
		print("Output path already exists. Please remove it or enter new file path")
		exit()

	conf = SparkConf().setAppName("Terasort")
	sc = SparkContext(conf=conf)

	# Select the files 
	
	teragen_file_name = "part-*"
	teragen_file_paths = os.path.join(in_complete_path, teragen_file_name)

	lines = sc.textFile(teragen_file_paths)
	
	
	rdd = lines.map(lambda x:(x.split("'")[1],x.split(", ")[1]," " + x.split("'")[3]))

	# Generate the sample keys
	
	N = 10
	sample_keys = list(range(N-1))
	llines = lines.take(N)
	for i in  range(N-1):
		sample_keys[i] = llines[i].split("'")[1]
		
	sample_keys.sort()
	
	# Partition the Data and Sort
	
	rdd2 = rdd.filter(lambda x: x[0] < sample_keys[0])
	rdd3 = (rdd2.map(lambda x: (x[0], x) )).sortByKey().map(lambda x: (x[1][0],x[1][1],x[1][2]) )
	
	for i in  range(1,N-1):
		rdd2 = rdd.filter(lambda x: sample_keys[i-1] <= x[0] and x[0] < sample_keys[i])	
		rdd3 = rdd3.union((rdd2.map(lambda x: (x[0], x) )).sortByKey().map(lambda x: (x[1][0],x[1][1],x[1][2]) ))
	
	rdd2 = rdd.filter(lambda x: sample_keys[N-2] <= x[0])
	rdd3 = rdd3.union((rdd2.map(lambda x: (x[0], x ) )).sortByKey().map(lambda x: (x[1][0],x[1][1],x[1][2]) ))
	
	# Output the data
	
	rdd3.saveAsTextFile(out_complete_path)
	
