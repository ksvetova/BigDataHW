from pyspark import SparkContext, SparkConf
import sys, time, math
from approximateAlgorithm import MRApproxOutliers
from ExactOutliers import ExactOutliers

# function to read points from the file 
def parse_point(line):
    parts = line.strip().split(',')
    return (float(parts[0]), float(parts[1]))

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
    conf = SparkConf().setAppName('G011HW1').setMaster("local[*]")
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
    print(f"{file_path} D={D} M={M} K={K} L={L}")

    #Reads the input points into an RDD of strings (called rawData) and transform it into an RDD of points (called inputPoints), represented as pairs of floats, subdivided into L partitions.
    rawData = sc.textFile(file_path)
    inputPoints = rawData.map(parse_point).repartition(L).cache()
    
    #Prints the total number of points
    num_points = inputPoints.count()
    print(f"Number of points = {num_points}")
    
    #Only if the number of points is at most 200000
    if (num_points<200000):
        #Downloads the points into a list called listOfPoints 
        listOfPoints = inputPoints.collect()
        #Performs ExactOutliers while keeping the time
        start_time_ExactOutliers = time.monotonic()
        ExactOutliers(listOfPoints, D, M, K)
        end_time_ExactOutliers = time.monotonic()
        print('Running time of ExactOutliers =', int((end_time_ExactOutliers - start_time_ExactOutliers)*1000), "ms")

    #Performs MRApproxOutliers while keeping the time
    start_time_MRApproxOutliers = time.monotonic()
    MRApproxOutliers(inputPoints, D, M, K)
    end_time_MRApproxOutliers = time.monotonic()
    print('Running time of MRApproxOutliers =', int((end_time_MRApproxOutliers - start_time_MRApproxOutliers)*1000), "ms")

    sc.stop()
	
if __name__ == "__main__":
	main()