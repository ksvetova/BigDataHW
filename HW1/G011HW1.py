from pyspark import SparkContext, SparkConf
import sys
import time
import math

# function to compute distance between 2 points
def distance(a, b):
    # a = [x1, y1]
    # b = [x2, y2]
    d = math.sqrt((b[0] - a[0])**2 + (b[1] - a[1])**2)
    return d 

# function to implement the exact algorithm
def ExactOutliers(points, D, M, K):

    outliers = []

    for point in points:

        set_points = 0 
        for i in points:
            if distance(i, point) <= D:
                set_points += 1

        if set_points <= M:
            outliers.append((point, set_points))

    print('Number of ouliers =', len(outliers))

    ouliers_sort = sorted(outliers, key=lambda a: a[1])

    k = 0

    if K > len(outliers):
        K = len(outliers)
    
    for i in range(k, K):
        print("Point:", "({},{})".format(ouliers_sort[i][0][0], ouliers_sort[i][0][1]))

# function to read points from the file 
def read_coordinates(file_path):

    points = []
    file = open(file_path, "r")
    for line in file:
        coord = line.strip().split(',')
        points.append([float(i) for i in coord])
    
    return points

def isfloat(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def main():
    
	# CHECKING NUMBER OF CMD LINE PARAMTERS
    assert len(sys.argv) == 6, "Usage: python G011HW1.py <file_name> <D> <M> <K> <L>"

    # SPARK SETUP
    conf = SparkConf().setAppName('G011HW1')
    sc = SparkContext(conf=conf)

    # INPUT READING
    file_path = sys.argv[1]
    D = sys.argv[2]
    assert isfloat(D), "K must be a float"
    D = float(D)
    M = sys.argv[3]
    assert M.isdigit(), "M must be an integer"
    M = int(M)
    K = sys.argv[4]
    assert K.isdigit(), "K must be an integer"
    K = int(K)
    L = sys.argv[5]
    assert L.isdigit(), "L must be an integer"
    L = int(L)

    #Prints the command-line arguments and stores D,M,K,L into suitable variables
    print(file_path, "D="+ str(D), "M="+ str(M), "K="+ str(M), "L="+ str(L))

    #Reads the input points into an RDD of strings (called rawData) and transform it into an RDD of points (called inputPoints), represented as pairs of floats, subdivided into L partitions.
    #TODO

    #Prints the total number of points
    pointsCount = 0 #TODO from RDD
    print('Number of points =', pointsCount)

    #Executes ExactOutliers and running time
    if (pointsCount<200000):
        listOfPoints = read_coordinates(file_path)
        start_time_ExactOutliers = time.monotonic()
        ExactOutliers(listOfPoints, D, M, K)
        end_time_ExactOutliers = time.monotonic()
        print('Running time of ExactOutliers =', int((end_time_ExactOutliers - start_time_ExactOutliers)*1000), "ms")

    #Executes MRApproxOutliers and running time
    listOfPoints = read_coordinates(file_path)
    start_time_MRApproxOutliers = time.monotonic()
    #TODO MRApproxOutliers(inputPoints, D, M, K)
    end_time_MRApproxOutliers = time.monotonic()
    print('Running time of MRApproxOutliers =', int((end_time_MRApproxOutliers - start_time_MRApproxOutliers)*1000), "ms")
	
if __name__ == "__main__":
	main()