import random

# euclidean distance
def d(p1, p2):
    return sum((x - y) ** 2 for x, y in zip(p1, p2)) ** 0.5

def SequentialFFT(P, K):
    S = [random.choice(P)] # centers with an arbitrary point in P

    for i in range(2, K + 1):
        
        # Find ci in P-S that maximizes d(ci, S)
        max_d  = -1
        farthest = None
        
        for point in P:
            if point not in S:
                min_d = min(d(point, c) for c in S)
                if min_d > max_d:
                    max_d = min_d
                    farthest = point


        S.append(farthest)

    return S


P = [(1,2),(6,2),(3, 4),(5,6),(8,1),(9,6),(100,4) ]
K = 2

centers = SequentialFFT(P, K)
print("Centers:", centers)

