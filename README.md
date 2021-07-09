# Terasort-For-Spark

## USAGE

Prerequisites:
- Apache Spark 3.1.1

Go to installation folder for Apache Spark 3.1.1, 

To generate data using Teragen

./bin/spark-submit teragen.py "# of rows" "outputdirectory"  

To sort the generated using Terasort

./bin/spark-submit terasort.py "inputdirectory" "outputdirectory"
