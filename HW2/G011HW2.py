from pyspark import SparkContext, SparkConf
import sys
import math
from pyspark import SparkContext
from SequentialFFT import SequentialFFT, d
from approximateAlgorithm import MRApproxOutliers
import time

def parse_point(line):
    # Parse a line of input to extract the x, y coordinates as floats
    x, y = map(float, line.split(','))
    return (x, y)

def MRFFT(P, K):
    start_time = time.time()
    
    # R1
    def FFT(iterator):
        points = list(iterator)
        return SequentialFFT(points, K)

    coreset = P.mapPartitions(FFT).collect()

    r1_time = time.time() - start_time
    print("Running time of MRFFT Round 1 =", r1_time)
    
    # R2
    start_time = time.time()
    centers = SequentialFFT(coreset, K)
    r2_time = time.time() - start_time
    print("Running time of MRFFT Round 2 =", r2_time)
    
    broadcast_centers = sc.broadcast(centers)
    
    # R3
    start_time = time.time()
    max_distance = P.map(lambda x: max(d(x, c) for c in broadcast_centers.value)).reduce(max)
    r3_time = time.time() - start_time
    print("Running time of MRFFT Round 3 =", r3_time)

    return max_distance

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: spark-submit script.py <input_file> <M> <K> <L>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    M = int(sys.argv[2])
    K = int(sys.argv[3])
    L = int(sys.argv[4])
    
    # Create a Spark context
    conf = SparkConf().setAppName("OutlierDetection")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    
    # Read input points and partition into L partitions
    rawData = sc.textFile(input_file)
    inputPoints = rawData.map(parse_point).repartition(L).persist()
    
    # Print the command-line arguments
    print(input_file, "M="+str(M)+" K="+str(K)+" L="+str(L))
    
    # Print the total number of points
    print("Total number of points =", inputPoints.count())
    
    # Execute MRFFT
    D = MRFFT(inputPoints, K)
    print("Radius =", D)
    
    # Execute MRApproxOutliers
    start_time = time.time()
    MRApproxOutliers(inputPoints, D, M)
    total_time = time.time() - start_time
    print("Running time of MRApproxOutliers =", total_time)
    
    # Stop the Spark context
    sc.stop()
