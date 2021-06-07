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
	
	
	rdd = lines.map(lambda x:(x.split("'")[1],x.split(", ")[1]," " + x.split("'")[3]))

	N = 8
	sample_keys = ['BBBBBBBBBB','EEEEEEEEEE','HHHHHHHHHH','LLLLLLLLLL','PPPPPPPPPP', 'TTTTTTTTTT', 'VVVVVVVVVV']
	
	
	
	
	rdd2 = rdd.filter(lambda x: x[0] < sample_keys[0])
	rdd3 = (rdd2.map(lambda x: (x[0], sample_keys[0] ) )).sortByKey()
	

	
	
	for i in  range(1,N-1):
		rdd2 = rdd.filter(lambda x: sample_keys[i-1] <= x[0] and x[0] < sample_keys[i])	
		rdd3 = rdd3.union((rdd2.map(lambda x: (x[0], sample_keys[i]) )).sortByKey())
	
	rdd2 = rdd.filter(lambda x: sample_keys[N-2] <= x[0])
	rdd3 = rdd3.union((rdd2.map(lambda x: (x[0], "> "+ sample_keys[N-2] ) )).sortByKey())
	
	
	rdd3.saveAsTextFile(out_complete_path)
	
	
	# collect the RDD to a list
	#llist = rdd.collect()
	#llist = lines.collect()
	# print the list
	#ii = 0
	#for line in llist:
	#	print(str(ii) +": Key: " + line[0] + ", RowID: "+ line[1]+ ", Filler: "+line[2])
	#	
	#	ii = ii + 1
	#	
	#print(teragen_file_paths)
	
	
	
	
	
	
	# Ammount of sampled keys that I choose
	#N = int(len(llist) / 100)
	#sample_keys = [N - 1]
	#for i in range(N - 1):
	#	sample_keys.append(llist[i][0])
	#	print(sample_keys[i])
	
	#rdd2 = sc.parallelize(sample_keys)
	#rdd3 = rdd2.sortByKey(ascending=True).map(lambda k, v, a: k).collect()
	#rdd3.saveAsTextFile(out_complete_path)
	
	
	#row = list(range(0, len(llist))
	#rdd4 = sc.parallelize(row)
	
	
	
	#reduce = rdd.reduce(lambda a,b:a+b)
	
	#lines = sc.textFile(in_file_name)
	#lineLengths = lines.map(lambda s: len(s))
	#totalLength = lineLengths.reduce(lambda a, b: a + b)
	
