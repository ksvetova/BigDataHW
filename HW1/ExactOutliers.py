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

    print('Number of ouliers = ', len(outliers))

    ouliers_sort = sorted(outliers, key=lambda a: a[1])

    k = 0

    if K > len(outliers):
        K = len(outliers)
    
    print("first K outliers: ")
    for i in range(k, K):
        print(ouliers_sort[i][0])



# function to read points from the file 
def read_coordinates(file_path):

    points = []
    file = open(file_path, "r")
    for line in file:
        coord = line.strip().split(',')
        points.append([float(i) for i in coord])
    
    return points



if __name__ == "__main__":

    file_path = 'test.txt'
    points = read_coordinates(file_path)

    D = float(input("D value: "))
    M = int(input("M value: "))
    K = int(input("K value: "))

    ExactOutliers(points, D, M, K)


