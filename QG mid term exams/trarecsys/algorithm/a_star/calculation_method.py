import math
from geopy.distance import geodesic
R = 6371000


def euclid(dot_a, dot_b):
    """
    global R
    # dot = [lng, lat]
    dot_a = list(map(radians, dot_a))
    dot_b = list(map(radians, dot_b))
    # dot_a, dot_b = [113.2643735 ,  22.99152498], [113.27263378,  22.9966462 ]
    cos_ab = cos(dot_a[1]) * cos(dot_b[1]) * cos(dot_a[0] - dot_b[0]) + sin(dot_a[1]) * sin(dot_b[1])
    print(cos_ab)
    dist = R * acos(cos_ab)
    # sometimes the cos_ab may be bigger than 1
    """
    dist = geodesic(dot_a[::-1], dot_b[::-1]).meters

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


def transformlat(lng, lat):
    pi = 3.1415926535897932384626  # π

    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
          0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
            math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
            math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret


def transformlng(lng, lat):
    pi = 3.1415926535897932384626  # π
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
          0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 *
            math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 *
            math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 *
            math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret


def out_of_china(lng, lat):
    """
    判断是否在国内，不在国内不做偏移
    :param lng:
    :param lat:
    :return:
    """
    if lng < 72.004 or lng > 137.8347:
        return True
    if lat < 0.8293 or lat > 55.8271:
        return True
    return False


def wgs84_to_bd09(lng, lat):
    x_pi = 3.14159265358979324 * 3000.0 / 180.0
    pi = 3.1415926535897932384626  # π
    a = 6378245.0  # 长半轴
    ee = 0.00669342162296594323  # 扁率
    if out_of_china(lng, lat):  # 判断是否在国内
        return lng, lat
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    z = math.sqrt(mglng * mglng + mglat * mglat) + 0.00002 * math.sin(mglat * x_pi)
    theta = math.atan2(mglat, mglng) + 0.000003 * math.cos(mglng * x_pi)
    bd_lng = z * math.cos(theta) + 0.0065
    bd_lat = z * math.sin(theta) + 0.006
    # print('转换成百度坐标:%s,%s' % (bd_lng, bd_lat))
    return bd_lng, bd_lat


