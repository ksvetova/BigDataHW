from pyspark import SparkContext, SparkConf
import csv
import math

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
    
    # sorted_rdd = cell_info_rdd.sortBy(lambda x: x[1][0])
    # sorted_cells = sorted_rdd.collect()

    # for cell, info in sorted_cells:
    #     print("Cell:", str(cell), "Size =", info[0])


if __name__ == "__main__":

    file_path = 'uber-10k.csv'
    D = 0.02
    M = 10

    conf = SparkConf().setAppName("MRApproxOutliers").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    points = []
   
    with open(file_path, "r") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            coord = tuple(map(float, row))
            points.append(coord)

    points_rdd = sc.parallelize(points)

    MRApproxOutliers(points_rdd, D, M)

    sc.stop()
