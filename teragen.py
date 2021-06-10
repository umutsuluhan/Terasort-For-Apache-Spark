from pyspark import SparkContext, SparkConf

import sys, os, string, random


if __name__ == "__main__":                                  
	#Getting arguments and checking if conditions are met
	if len(sys.argv) != 3:
		print("Usage:")
		print("teragen.py <number of rows> <output directory>")
		exit()

	home_dir = os.getenv("HOME")
	out_dir = str(home_dir) + str(sys.argv[2])

	file_name = "teragen"
	complete_path = os.path.join(out_dir, file_name)

	if os.path.isdir(complete_path) == True:
		print("This path already exists. Please remove it or enter new file path")
		exit()

	#Creating Spark Context 
	conf = SparkConf().setAppName("Teragen")
	sc = SparkContext(conf=conf)

	#Generating rowID's
	rowId = list(range(0, int(sys.argv[1])))

	#Teragen operation 
	rdd = sc.parallelize(rowId)
	rdd2 = rdd.map(lambda x :( "".join((random.choice(string.ascii_uppercase) for i in range(10))), x, "".join((random.choice(string.ascii_uppercase) for i in range(78)))))
	rdd2.saveAsTextFile(complete_path)


