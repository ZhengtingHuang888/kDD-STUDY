from math import radians, cos, sin, acos
from geopy.distance import vincenty
R = 6371000


def euclid(dot_a, dot_b):
    """
    global R
    # dot = [lat, lng]
    dot_a = list(map(radians, dot_a))
    dot_b = list(map(radians, dot_b))
    # dot_a, dot_b = [113.2643735 ,  22.99152498], [113.27263378,  22.9966462 ]
    cos_ab = cos(dot_a[1]) * cos(dot_b[1]) * cos(dot_a[0] - dot_b[0]) + sin(dot_a[1]) * sin(dot_b[1])
    print(cos_ab)
    dist = R * acos(cos_ab)
    # sometimes the cos_ab may be bigger than 1
    """
    dist = vincenty(dot_a[::-1], dot_b[::-1]).meters

    return dist


def manhattan(dot_a, dot_b):
    return abs(dot_a[0] - dot_b[0]) + abs(dot_a[1] - dot_b[1])


def subway_price(dist):
    dist /= 1e3
    price = 2
    if 4 <= dist < 12:
        price += (dist - 4) / 4
    elif 12 <= dist < 24:
        price += 3 + (dist - 12) / 6
    elif dist >= 24:
        price += 5 + (dist - 24) / 8

    return int(price)




