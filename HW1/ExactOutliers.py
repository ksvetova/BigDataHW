import csv
import math

# function to implement the exact algorithm
def ExactOutliers(points, D, M, K):

    points_count = []

    for i in range(0, len(points)):
        points_count.append(1)

    for i in range(0, len(points)):
        for j in range(0+i+1, len(points)):
            if math.dist(points[i], points[j]) <= D:
                points_count[i] = points_count[i] + 1
                points_count[j] = points_count[j] + 1

    outliers = []

    for i in range(0, len(points)):
        if points_count[i] <= M:
            outliers.append((points[i], points_count[i]))

    print('Number of ouliers =', len(outliers))

    ouliers_sort = sorted(outliers, key=lambda a: a[1])

    k = 0

    if K > len(outliers):
        K = len(outliers)
    
    for i in range(k, K):
        print("Point:", "({},{})".format(ouliers_sort[i][0][0], ouliers_sort[i][0][1]))

def ExactOutliersSlow(points, D, M, K):

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
