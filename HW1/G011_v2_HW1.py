import sys
import time
from pyspark import SparkContext, SparkConf
from approximateAlgorithm import MRApproxOutliers
from ExactOutliers import distance, ExactOutliers

def parse_point(line):
    parts = line.strip().split(',')
    return (float(parts[0]), float(parts[1]))

def main():

    # input_file = sys.argv[1]
    # D = float(sys.argv[2])
    # M = int(sys.argv[3])
    # K = int(sys.argv[4])
    # L = int(sys.argv[5])

    input_file = 'uber-10k.csv'
    D = 0.2
    M = 10
    K = 50
    L = 2

    print(f"{input_file} D={D} M={M} K={K} L={L}")

    conf = SparkConf().setAppName("OutliersDetection")
    sc = SparkContext(conf=conf)

    rawData = sc.textFile(input_file)
    inputPoints = rawData.map(parse_point).repartition(L).cache()

    #count number of points
    num_points = inputPoints.count()
    print(f"Number of points = {num_points}")

    #get the points to list for ExactOutliers function, afterwards, start counting the time
    listOfPoints = inputPoints.collect()
    start_time_exact = time.time()

    ExactOutliers(listOfPoints, D, M, K)

    #calculate the time needed to execute the function and print it, repeat the process for the approx case
    elapsed_time_exact = int((time.time() - start_time_exact) * 1000)
    print(f"Running time of ExactOutliers = {elapsed_time_exact} ms")

    start_time_approx = time.time()
    MRApproxOutliers(inputPoints, D, M, K)
    elapsed_time_approx = int((time.time() - start_time_approx) * 1000)
    print(f"Running time of MRApproxOutliers = {elapsed_time_approx} ms")

    sc.stop()

if __name__ == "__main__":
    main()
