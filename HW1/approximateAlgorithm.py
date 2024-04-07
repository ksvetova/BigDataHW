from pyspark import SparkContext, SparkConf
import csv
import math

def MRApproxOutliers(points_rdd, D, M, K):
    #count points in each cell
    def map_to_cell(point):
        x, y = point
        cell_i = math.floor(x / (D / (2 * (2 ** 0.5))))
        cell_j = math.floor(y / (D / (2 * (2 ** 0.5))))
        return ((cell_i, cell_j), 1)

    cells_rdd = points_rdd.map(map_to_cell).reduceByKey(lambda a, b: a + b).cache()
    print(cells_rdd)

    #collect cells_rdd locally
    cells = cells_rdd.collectAsMap()

    #compute N3 and N7 for each cell
    def map_to_N3_N7(cell):
        (cell_i, cell_j), count = cell
        N3 = sum([cells.get((i, j), 0) for i in range(cell_i - 1, cell_i + 2) for j in range(cell_j - 1, cell_j + 2)])
        N7 = sum([cells.get((i, j), 0) for i in range(cell_i - 3, cell_i + 4) for j in range(cell_j - 3, cell_j + 4)])
        return ((cell_i, cell_j), (count, N3, N7))

    cell_info_rdd = cells_rdd.map(map_to_N3_N7).cache()

    #count sure and uncertain outliers
    sure_outliers_count = cell_info_rdd.filter(lambda x: x[1][2] <= M).count()
    uncertain_points_count = cell_info_rdd.filter(lambda x: x[1][1] <= M and x[1][2] > M).count()

    print("Sure outliers ({}-outliers): {}".format(M, sure_outliers_count))
    print("Uncertain points: {}".format(uncertain_points_count))

    #get the first K non-empty cells sorted by size
    top_cells = cell_info_rdd.sortBy(lambda x: x[1][0], ascending=True).take(K)

    print("Top {} non-empty cells:".format(K))
    for cell, info in top_cells:
        print("Cell ", str(cell)+": Size ", info[0])

if __name__ == "__main__":

    conf = SparkConf().setAppName("MRApproxOutliers").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    points = []
    with open('test.txt', "r") as file:
        for line in file:
            coord = line.strip().split(',')
            points.append((float(coord[0]), float(coord[1])))

    # with open('uber-10k.csv', "r") as file:
    #     csv_reader = csv.reader(file)
    #     for row in csv_reader:
    #         coord = tuple(map(float, row))
    #         points.append(coord)


    points_rdd = sc.parallelize(points)

    D = 0.2
    M = 10  
    K = 5

    MRApproxOutliers(points_rdd, D, M, K)

    sc.stop()
