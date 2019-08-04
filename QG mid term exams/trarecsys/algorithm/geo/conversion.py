import numpy as np
import math

PI = math.pi

def mercator_to_wgs84(coordinates):
    """

    :param coordinates: a numpy array just like "[[lat, lng]]" or
    "[[lat1, lng1]
     [lat2, lng2]]"
    :return: a numpy array which shows the wgs84 geo
    """
    return bd09_to_wgs84(mercator_to_bd09(coordinates))
def _transformlat(coordinates):
    lng = coordinates[:, 0] - 105
    lat = coordinates[:, 1] - 35
    ret = -100 + 2 * lng + 3 * lat + 0.2 * lat * lat + \
          0.1 * lng * lat + 0.2 * np.sqrt(np.fabs(lng))
    ret += (20 * np.sin(6 * lng * PI) + 20 *
            np.sin(2 * lng * PI)) * 2 / 3
    ret += (20 * np.sin(lat * PI) + 40 *
            np.sin(lat / 3 * PI)) * 2 / 3
    ret += (160 * np.sin(lat / 12 * PI) + 320 *
            np.sin(lat * PI / 30.0)) * 2 / 3
    return ret
def _transformlng(coordinates):
    lng = coordinates[:, 0] - 105
    lat = coordinates[:, 1] - 35
    ret = 300 + lng + 2 * lat + 0.1 * lng * lng + \
          0.1 * lng * lat + 0.1 * np.sqrt(np.fabs(lng))
    ret += (20 * np.sin(6 * lng * PI) + 20 *
            np.sin(2 * lng * PI)) * 2 / 3
    ret += (20 * np.sin(lng * PI) + 40 *
            np.sin(lng / 3 * PI)) * 2 / 3
    ret += (150 * np.sin(lng / 12 * PI) + 300 *
            np.sin(lng / 30 * PI)) * 2 / 3
    return ret
def gcj02_to_wgs84(coordinates):
    """
    GCJ-02转WGS-84
    :param coordinates: GCJ-02坐标系的经度和纬度的numpy数组
    :returns: WGS-84坐标系的经度和纬度的numpy数组
    """
    ee = 0.006693421622965943  # 偏心率平方
    a = 6378245  # 长半轴
    lng = coordinates[:, 0]
    lat = coordinates[:, 1]
    is_in_china = (lng > 73.66) & (lng < 135.05) & (lat > 3.86) & (lat < 53.55)
    _transform = coordinates[is_in_china]  # 只对国内的坐标做偏移

    dlat = _transformlat(_transform)
    dlng = _transformlng(_transform)
    radlat = _transform[:, 1] / 180 * PI
    magic = np.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = np.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * PI)
    dlng = (dlng * 180.0) / (a / sqrtmagic * np.cos(radlat) * PI)
    mglat = _transform[:, 1] + dlat
    mglng = _transform[:, 0] + dlng
    coordinates[is_in_china] = np.array([
        _transform[:, 0] * 2 - mglng, _transform[:, 1] * 2 - mglat
    ]).T
    return coordinates
def bd09_to_gcj02(coordinates):
    """
    BD-09转GCJ-02
    :param coordinates: BD-09坐标系的经度和纬度的numpy数组
    :returns: GCJ-02坐标系的经度和纬度的numpy数组
    """
    x_pi = PI * 3000 / 180
    x = coordinates[:, 0] - 0.0065
    y = coordinates[:, 1] - 0.006
    z = np.sqrt(x * x + y * y) - 0.00002 * np.sin(y * x_pi)
    theta = np.arctan2(y, x) - 0.000003 * np.cos(x * x_pi)
    lng = z * np.cos(theta)
    lat = z * np.sin(theta)
    coordinates = np.array([lng, lat]).T
    return coordinates
def bd09_to_wgs84(coordinates):
    """
    BD-09转WGS-84
    :param coordinates: BD-09坐标系的经度和纬度的numpy数组
    :returns: WGS-84坐标系的经度和纬度的numpy数组
    """
    return gcj02_to_wgs84(bd09_to_gcj02(coordinates))
def mercator_to_bd09(mercator):
    """
    BD-09MC转BD-09
    :param coordinates: GCJ-02坐标系的经度和纬度的numpy数组
    :returns: WGS-84坐标系的经度和纬度的numpy数组
    """
    MCBAND = [12890594.86, 8362377.87, 5591021, 3481989.83, 1678043.12, 0]
    MC2LL = [[1.410526172116255e-08, 8.98305509648872e-06, -1.9939833816331,
              200.9824383106796, -187.2403703815547, 91.6087516669843,
              -23.38765649603339, 2.57121317296198, -0.03801003308653,
              17337981.2],
             [-7.435856389565537e-09, 8.983055097726239e-06, -0.78625201886289,
              96.32687599759846, -1.85204757529826, -59.36935905485877,
              47.40033549296737, -16.50741931063887, 2.28786674699375,
              10260144.86],
             [-3.030883460898826e-08, 8.98305509983578e-06, 0.30071316287616,
              59.74293618442277, 7.357984074871, -25.38371002664745,
              13.45380521110908, -3.29883767235584, 0.32710905363475,
              6856817.37],
             [-1.981981304930552e-08, 8.983055099779535e-06, 0.03278182852591,
              40.31678527705744, 0.65659298677277, -4.44255534477492,
              0.85341911805263, 0.12923347998204, -0.04625736007561,
              4482777.06],
             [3.09191371068437e-09, 8.983055096812155e-06, 6.995724062e-05,
              23.10934304144901, -0.00023663490511, -0.6321817810242,
              -0.00663494467273, 0.03430082397953, -0.00466043876332,
              2555164.4],
             [2.890871144776878e-09, 8.983055095805407e-06, -3.068298e-08,
              7.47137025468032, -3.53937994e-06, -0.02145144861037,
              -1.234426596e-05, 0.00010322952773, -3.23890364e-06,
              826088.5]]

    x = np.abs(mercator[:, 0])
    y = np.abs(mercator[:, 1])
    coef = np.array([
        MC2LL[index] for index in
        (np.tile(y.reshape((-1, 1)), (1, 6)) < MCBAND).sum(axis=1)
    ])
    return converter(x, y, coef)
def converter(x, y, coef):
    x_temp = coef[:, 0] + coef[:, 1] * np.abs(x)
    x_n = np.abs(y) / coef[:, 9]
    y_temp = coef[:, 2] + coef[:, 3] * x_n + coef[:, 4] * x_n ** 2 + \
             coef[:, 5] * x_n ** 3 + coef[:, 6] * x_n ** 4 + coef[:, 7] * x_n ** 5 + \
             coef[:, 8] * x_n ** 6
    x[x < 0] = -1
    x[x >= 0] = 1
    y[y < 0] = -1
    y[y >= 0] = 1
    x_temp *= x
    y_temp *= y
    coordinates = np.array([x_temp, y_temp]).T
    return coordinates
def get_lonlat(lon,lat):
    a = mercator_to_bd09(np.array([[lon,lat]]))
    return a
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
def wgs84tobd09(lng, lat):
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

