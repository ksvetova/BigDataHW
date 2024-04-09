import csv
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


if __name__ == "__main__":

    file_path = 'uber-10k.csv'
    D = 0.02
    M = 10
    K = 5
        
    points = []

    with open(file_path, "r") as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            coord = tuple(map(float, row))
            points.append(coord)

    ExactOutliers(points, D, M, K)
