from pyspark import SparkContext, SparkConf

import sys, os, string, random

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage:")
		print("terasort.py <input directory> <output directory>")
		exit()

	home_dir = os.getenv("HOME")
	in_dir = str(home_dir) + str(sys.argv[1])
	
	in_file_name = "teragen"
	in_complete_path = os.path.join(in_dir, in_file_name)
	
	if os.path.isdir(in_complete_path) == False:
		print("Input path doesn't exist. Please run Teragen or enter new file path")
		exit()
	
	out_dir = str(home_dir) + str(sys.argv[2])

	out_file_name = "terasort"
	out_complete_path = os.path.join(out_dir, out_file_name)

	if os.path.isdir(out_complete_path) == True:
		print("Output path already exists. Please remove it or enter new file path")
		exit()

	conf = SparkConf().setAppName("Terasort")
	sc = SparkContext(conf=conf)

	# rowId = list(range(0, int(sys.argv[1])))

	# rdd = sc.parallelize(rowId)
	# rdd2 = rdd.map(lambda x :( "".join((random.choice(string.ascii_uppercase) for i in range(10))), x, "".join((random.choice(string.ascii_uppercase) for i in range(78)))))
	# rdd2.saveAsTextFile(complete_path)

	# https://www.tutorialkart.com/apache-spark/read-multiple-text-files-to-single-rdd/ 
	# read input text files present in the directory to RDD

	teragen_file_name = "part-*"
	teragen_file_paths = os.path.join(in_complete_path, teragen_file_name)

	lines = sc.textFile(teragen_file_paths)
	
	# collect the RDD to a list
	llist = lines.collect()
	
	# print the list
	ii = 0
	for line in llist:
		print(str(ii) + " Key: " + line[ 1:13 ] + " RowID and Filler: " + line[ 15:(len(line)-1) ])
		ii = ii + 1
		
	print(teragen_file_paths)
	
	
	
	
	
	
	
	# Ammount of sampled keys that I choose
	N = int(len(llist) / 100)
	sample_keys = [N - 1]
	for i in range(N - 1):
		sample_keys.append(llist[i][ 2:12 ])
		print(sample_keys[i])
	
	rdd = sc.parallelize(sample_keys)
	rdd2 = sample_keys.map(lambda a, b : sample_keys[a] <= sample_keys[b])
	rdd2.saveAsTextFile(out_complete_path)
	#raaa = sample_keys.map(lambda n : )
	
	
	
	#row = list(range(0, len(llist))
	#rdd = sc.parallelize(row)
	
	
	
	#reduce = rdd.reduce(lambda a,b:a+b)
	
	#lines = sc.textFile(in_file_name)
	#lineLengths = lines.map(lambda s: len(s))
	#totalLength = lineLengths.reduce(lambda a, b: a + b)
	