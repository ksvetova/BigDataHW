from pyspark import SparkContext, SparkConf
import math
import time
import sys 

def MRApproxOutliers(points_rdd, D, M):
    #count points in each cell
    def map_to_cell(point):
        x, y = point
        cell_i = math.floor(x // (D / (2 ** 1.5)))
        cell_j = math.floor(y // (D / (2 ** 1.5)))
        return ((cell_i, cell_j), 1)

    cells_rdd = points_rdd.map(map_to_cell).reduceByKey(lambda a, b: a + b).cache()
   
    #collect cells_rdd locally
    cells = cells_rdd.collectAsMap()

    #compute N3 and N7 for each cell
    def map_to_N3_N7(cell):
        (cell_i, cell_j), count = cell
        N3 = sum([cells.get((i, j), 0) for i in range(cell_i - 1, cell_i + 2) for j in range(cell_j - 1, cell_j + 2)])
        N7 = sum([cells.get((i, j), 0) for i in range(cell_i - 3, cell_i + 4) for j in range(cell_j - 3, cell_j + 4)])
        return ((cell_i, cell_j), (count, N3, N7))

    cell_info_rdd = cells_rdd.map(map_to_N3_N7).cache()
    sure_outliers_count = 0
    uncertain_points_count = 0
    #count sure and uncertain outliers
    for element in cell_info_rdd.filter(lambda x: x[1][2] <= M).collect():
        sure_outliers_count += element[1][0]
    for element in cell_info_rdd.filter(lambda x: x[1][1] <= M and x[1][2] > M).collect():
        uncertain_points_count += element[1][0]

    print("Number of sure outliers = {}".format(sure_outliers_count))
    print("Number of uncertain points = {}".format(uncertain_points_count))



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
    
    start_time = time.monotonic()
    # R1
    def FFT(iterator):
        points = list(iterator)
        return SequentialFFT(points, K)

    coreset = P.mapPartitions(FFT).collect()

    time_r1 = time.monotonic() - start_time
    print("Running time of MRFFT Round 1 =", int(time_r1*1000), "ms")

    # R2
    start_time = time.monotonic()
    centers = SequentialFFT(coreset, K)
    time_r2 = time.monotonic() - start_time
    print("Running time of MRFFT Round 2 =", int(time_r2*1000), "ms")
    
    # R3
    start_time = time.monotonic()
    broadcast_centers = sc.broadcast(centers)
    centers_list = broadcast_centers.value 
    max_radius = P.map(lambda x: min(distance(x, c) for c in centers_list)).reduce(lambda x, y: max(x, y))
    r3_time = time.monotonic() - start_time
    print("Running time of MRFFT Round 3 =", int(r3_time*1000), "ms")
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
    conf.set("spark.locality.wait", "0s")
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
    start_time = time.monotonic()
    MRApproxOutliers(inputPoints, D, M)
    total_time = time.monotonic() - start_time
    print('Running time of MRApproxOutliers =', int(total_time*1000), "ms")
    
    # Stop the Spark context
    sc.stop()

