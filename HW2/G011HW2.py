from pyspark import SparkContext, SparkConf
import math
import time
import sys 
from approximateAlgorithm import MRApproxOutliers

# euclidean distance
def distance(p1, p2):
    return math.sqrt((p1[0] - p2[0]) **2 + (p1[1] - p2[1]) **2)


# Farthest-First Traversal algorithm
def SequentialFFT(P, K):
    S = [P[0]] # first point 
    for i in range(1, K):
        farthest_point = None
        max_d= float('-inf')
        for point in P:
            min_d = min(distance(point, center) for center in S)
            if min_d > max_d:
                max_d= min_d
                farthest_point = point
        S.append(farthest_point)  
    return S

# Map Reduce algorithm
def MRFFT(P, K):
    
    start_time = time.time()
    # R1
    def FFT(iterator):
        points = list(iterator)
        return SequentialFFT(points, K)

    coreset = P.mapPartitions(FFT).collect()

    time_r1 = time.time() - start_time
    print("Running time of MRFFT Round 1 =", time_r1)

    # R2
    start_time = time.time()
    centers = SequentialFFT(coreset, K)
    time_r2 = time.time() - start_time
    print("Running time of MRFFT Round 2 =", time_r2)
    
    # R3
    start_time = time.time()
    broadcast_centers = sc.broadcast(centers)
    centers_list = broadcast_centers.value 
    max_radius = P.map(lambda x: min(distance(x, c) for c in centers_list)).reduce(lambda x, y: max(x, y))
    r3_time = time.time() - start_time
    print("R3 time:", r3_time)
    return max_radius


# Parse a line of input to extract the x, y coordinates as floats
def parse_point(line):
    x, y = map(float, line.split(','))
    return (x, y)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: spark-submit G011HW2.py <input_file> <M> <K> <L>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    M = int(sys.argv[2])
    K = int(sys.argv[3])
    L = int(sys.argv[4])
    
    # Create a Spark context
    conf = SparkConf().setAppName("G011HW2.py")
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

