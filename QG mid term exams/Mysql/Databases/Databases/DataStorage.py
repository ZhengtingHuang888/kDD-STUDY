import pymysql.cursors  # 请求
import pymysql
import pandas as pd

class MySQLCommand(object):
    def __init__(self, user='root', passwords = 'hzt123'):  # 连接数据库
        self.config = {
            'host': "localhost",
            'user': user,
            'password': passwords,
            'db': 'trarecsys',
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        try:
            self.db = pymysql.connect(**self.config)
            self.cursor = self.db.cursor()
            #print("connect mysql success.")
        except:
            print("connect mysql error.")

# 存储站点信息
    def subway_stations(self, dataSet):
        """
                存入地铁相关数据的函数
                :param dataSet:  ['station_name', 'next_stations', lon, lat]
                其中station_uid是浮点数next_station = '{ '地铁站的id': 号线（int）, 'id':号线，}，  lon，lat为浮点数，  start_time等为时间变量
                :return:
        """
        try:
            # 检查导入的站点数据否存在相同的站点
            sqlExit = "SELECT station_name FROM subway_stations WHERE station_name = '%s'" % (dataSet[0])
            res = self.cursor.execute(sqlExit)
        except pymysql.Error as e:
            print("检查相同的站点时错误")
            return 0
        # 正常执行，合并表
        if res == 1:
            try:
                with self.db.cursor() as cursor:
                    # 提取到下一站信息
                    sql = "SELECT next_stations FROM subway_stations WHERE station_name = '%s'" % (dataSet[0])
                    cursor.execute(sql)
                    # 返回所有结果，fetchone()只能返回一个结果
                    result = cursor.fetchall()
                    result = result[0]['next_stations'].replace("'",'"')
                    result = eval(result)     # 转化为字典
                try:
                    if result.keys() != dataSet[1].keys():  # 判断两个字典相不相等
                        next_stations = {**result, **dataSet[1]}  # 两个字典拼接
                        try:
                            sql = 'UPDATE subway_stations SET next_stations = "%s" WHERE  station_name = "%s" ;' % (str(next_stations), dataSet[0])  # 更新下一站信息
                            print(sql)
                            try:
                                result = self.cursor.execute(sql)
                                self.db.commit()
                            except pymysql.Error as e:
                                # 回滚
                                self.db.rollback()
                                print("出错", e)
                        except pymysql.Error as e:
                            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                except AttributeError:
                    if result == None:
                        next_stations = dataSet[1]
                    elif dataSet[1] == None:
                        next_stations = result
                    try:
                        sql = 'UPDATE subway_stations SET next_stations = "%s" WHERE  station_name = "%s" ;' % (
                        str(next_stations), dataSet[0])  # 更新下一站信息
                        print(sql)
                        try:
                            result = self.cursor.execute(sql)
                            self.db.commit()
                        except pymysql.Error as e:
                            # 回滚
                            self.db.rollback()
                            print("出错", e)
                    except pymysql.Error as e:
                        print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
            except pymysql.Error as e:
                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
            return 0
        try:
            sql = 'INSERT INTO subway_stations(station_name, next_stations, lon, lat) VALUES ("{}", "{}", "{}", "{}");'.format(dataSet[0], dataSet[1], dataSet[2], dataSet[3])
            try:
                result = self.cursor.execute(sql)
                self.db.commit()
            except pymysql.Error as e:
                # 回滚
                self.db.rollback()
                if "key 'PRIMARY'" in e.args[1]:
                    print("数据已经存在，未插入数据")
                else:
                    print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except pymysql.Error as e:
            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))

# 存入算法数据库
    def subway_edges(self, dataSet):
        """
        存入地铁算法相关的数据库
        :param dataSet: [start_id, end_id, weight, line]
        :return: start_id  当前站点的uid； end_id  结尾站点的uid, weight为1， line当前所属的号线
        """
        try:
            sql = 'INSERT INTO subway_edges(now_station, next_station, weight, line) VALUES ("{}", "{}", "{}", "{}");'.format(dataSet[0], dataSet[1], dataSet[2], dataSet[3])
            try:
                result = self.cursor.execute(sql)
                self.db.commit()
            except pymysql.Error as e:
                # 回滚
                self.db.rollback()
                if "key 'PRIMARY'" in e.args[1]:
                    print("数据已经存在，未插入数据")
                else:
                    print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except pymysql.Error as e:
            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))

# 地铁属性
    def subway_feature(self, dataSet):
        # 地铁属性操作
        """
        start_id 起始站点， end_id终点站点, price价格, wait_time大约等待时间,
        :param dataSet:
        :return:
        """
        try:
            sql = 'INSERT INTO subwayfeature_line(start_name, end_name, wait_time,  work_endtime, line) VALUES ("{}", "{}", "{}", "{}", "{}");'.format(dataSet[0], dataSet[1], dataSet[2], dataSet[3], dataSet[4])
            try:
                result = self.cursor.execute(sql)
                self.db.commit()
            except pymysql.Error as e:
                # 回滚
                self.db.rollback()
                if "key 'PRIMARY'" in e.args[1]:
                    print("数据已经存在，未插入数据")
                else:
                    print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except pymysql.Error as e:
            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))


# 存储站点信息
    def bus_stations(self, dataSet):
        """
                        存入地铁相关数据的函数
                        :param dataSet:  ['station_name', 'next_stations', lon, lat]
                        其中station_uid是浮点数next_station = '{ '地铁站的id': 号线（int）, 'id':号线，}，  lon，lat为浮点数，  start_time等为时间变量
                        :return:
        """
        try:
            sqlExit = "SELECT station_name FROM bus_stations WHERE station_name = '%s'" % (dataSet[0])  # 检查导入的站点数据否存在相同的站点
            res = self.cursor.execute(sqlExit)
        except pymysql.Error as e:
            print("检查相同的站点时错误")
            return 0
        if res == 1:
            try:
                with self.db.cursor() as cursor:
                    sql = "SELECT next_stations FROM bus_stations WHERE station_name = '%s'" % (dataSet[0])   #提取到下一站信息
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    result = result[0]['next_stations'].replace("'",'"')
                    result = eval(result)     #转化为字典
                try:
                    if result.keys() != dataSet[1].keys():  # 判断两个字典相不相等
                        next_stations = {**result, **dataSet[1]}  # 两个字典拼接
                        try:
                            sql = 'UPDATE bus_stations SET next_stations = "%s" WHERE  station_name = "%s" ;' % (str(next_stations), dataSet[0])  # 更新下一站信息
                            print(sql)
                            try:
                                result = self.cursor.execute(sql)
                                self.db.commit()
                            except pymysql.Error as e:
                                # 回滚
                                self.db.rollback()
                                print("出错", e)
                        except pymysql.Error as e:
                            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                except AttributeError:
                    if result == None:
                        next_stations = dataSet[1]
                    elif dataSet[1] == None:
                        next_stations = result
                    try:
                        sql = 'UPDATE bus_stations SET next_stations = "%s" WHERE  station_name = "%s" ;' % (str(next_stations), dataSet[0])  # 更新下一站信息
                        print(sql)
                        try:
                            result = self.cursor.execute(sql)
                            self.db.commit()
                        except pymysql.Error as e:
                            # 回滚
                            self.db.rollback()
                            print("出错", e)
                    except pymysql.Error as e:
                        print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
            except pymysql.Error as e:
                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
            return 0
        try:
            sql = 'INSERT INTO bus_stations(station_name, next_stations, lon, lat, line) VALUES ("{}", "{}", "{}", "{}", "{}");'.format(dataSet[0], dataSet[1], dataSet[2], dataSet[3], dataSet[4])
            try:
                result = self.cursor.execute(sql)
                self.db.commit()
            except pymysql.Error as e:
                # 回滚
                self.db.rollback()
                if "key 'PRIMARY'" in e.args[1]:
                    print("数据已经存在，未插入数据")
                else:
                    print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except pymysql.Error as e:
            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))

 #存入算法数据库
    def bus_edges(self, dataSet):
        """
        存入地铁算法相关的数据库
        :param dataSet: [start_id, end_id, weight, line]
        :return: start_id  当前站点的uid； end_id  结尾站点的uid, weight为1， line当前所属的号线
        """
        try:
            sql = 'INSERT INTO bus_edges(now_station, next_station, weight, line) VALUES ("{}", "{}", "{}", "{}");'.format(dataSet[0], dataSet[1], dataSet[2], dataSet[3])
            try:
                result = self.cursor.execute(sql)
                self.db.commit()
            except pymysql.Error as e:
                # 回滚
                self.db.rollback()
                if "key 'PRIMARY'" in e.args[1]:
                    print("数据已经存在，未插入数据")
                else:
                    print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except pymysql.Error as e:
            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))

 #地铁属性
    def bus_feature(self, dataSet):
        # 地铁属性操作
        """
        start_id 起始站点， end_id终点站点, price价格, wait_time大约等待时间,
        :param dataSet:
        :return:
        """
        try:
            sql = 'INSERT INTO busfeature_line(start_name, end_name, wait_time,  end_time, line) VALUES ("{}", "{}", "{}", "{}", "{}");'.format(dataSet[0], dataSet[1], dataSet[2], dataSet[3], dataSet[4])
            try:
                result = self.cursor.execute(sql)
                self.db.commit()
            except pymysql.Error as e:
                # 回滚
                self.db.rollback()
                if "key 'PRIMARY'" in e.args[1]:
                    print("数据已经存在，未插入数据")
                else:
                    print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
        except pymysql.Error as e:
            print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))

#将站的名称转化为id
    def subwaychange_name_to_id(self):
        """
        :return:
        """
        try:
            with self.db.cursor() as cursor:
                sql = "SELECT now_station, next_station  FROM subway_edges ;"
                cursor.execute(sql)
                result = cursor.fetchall()
                nations = pd.DataFrame(result).values.tolist()
                for nation in nations:
                    try:
                        with self.db.cursor() as cursor:
                            sql = "SELECT id  FROM subway_stations WHERE station_name = '%s';" % nation[0]
                            print(sql)
                            cursor.execute(sql)
                            nowstation_id = cursor.fetchall()[0]['id']
                            try:
                                sql = 'UPDATE subway_edges SET nowstation_id = "%d" WHERE  now_station = "%s" ;' % (nowstation_id, nation[0])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
                    try:    #传入下一站的id
                        with self.db.cursor() as cursor:
                            sql = "SELECT id  FROM subway_stations WHERE station_name = '%s';" % nation[1]
                            print(sql)
                            cursor.execute(sql)
                            nowstation_id = cursor.fetchall()[0]['id']
                            try:
                                sql = 'UPDATE subway_edges SET nextstation_id = "%d" WHERE  next_station = "%s" ;' % (nowstation_id, nation[1])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
        except pymysql.Error as e:
            print("数据库错误")

#添加算法站点的地理信息位置
    def subwayadd_loaction(self):
        try:
            with self.db.cursor() as cursor:
                sql = "SELECT now_station, next_station  FROM subway_edges ;"
                cursor.execute(sql)
                result = cursor.fetchall()
                nations = pd.DataFrame(result).values.tolist()
                for nation in nations:
                    try:
                        with self.db.cursor() as cursor:
                            sql = "SELECT lon, lat  FROM subway_stations WHERE station_name = '%s';" % nation[0]
                            cursor.execute(sql)
                            nowlocation = cursor.fetchall()
                            nowlocation_lon = nowlocation[0]['lon']
                            nowlocation_lat = nowlocation[0]['lat']
                            try:
                                sql = 'UPDATE subway_edges SET nowStation_lon = "{}", nowStation_lat = "{}" WHERE  now_station = "{}" ;' .format(nowlocation_lon, nowlocation_lat, nation[0])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
                    try:
                        with self.db.cursor() as cursor:
                            sql = "SELECT lon, lat  FROM subway_stations WHERE station_name = '%s';" % nation[1]
                            cursor.execute(sql)
                            nextlocation = cursor.fetchall()
                            nextlocation_lon = nextlocation[0]['lon']
                            nextlocation_lat = nextlocation[0]['lat']
                            try:
                                sql = 'UPDATE subway_edges SET nextStation_lon = "{}", nextStation_lat = "{}" WHERE  now_station = "{}" ;' .format( nextlocation_lon, nextlocation_lat, nation[1])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
        except pymysql.Error as e:
            print("数据库错误")

#将站的名称转化为id
    def buschange_name_to_id(self):
        """
        :return:
        """
        try:
            with self.db.cursor() as cursor:
                sql = "SELECT now_station, next_station  FROM bus_edges ;"
                cursor.execute(sql)
                result = cursor.fetchall()
                nations = pd.DataFrame(result).values.tolist()
                for nation in nations:
                    try:
                        with self.db.cursor() as cursor:
                            sql = "SELECT id  FROM bus_stations WHERE station_name = '%s';" % nation[0]
                            print(sql)
                            cursor.execute(sql)
                            nowstation_id = cursor.fetchall()[0]['id']
                            try:
                                sql = 'UPDATE bus_edges SET nowstation_id = "%d" WHERE  now_station = "%s" ;' % (nowstation_id, nation[0])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
                    try:    #传入下一站的id
                        with self.db.cursor() as cursor:
                            sql = "SELECT id  FROM bus_stations WHERE station_name = '%s';" % nation[1]
                            print(sql)
                            cursor.execute(sql)
                            nowstation_id = cursor.fetchall()[0]['id']
                            try:
                                sql = 'UPDATE bus_edges SET nextstation_id = "%d" WHERE  next_station = "%s" ;' % (nowstation_id, nation[1])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
        except pymysql.Error as e:
            print("数据库错误")

# 添加算法站点的地理信息位置
    def busadd_loaction(self):
        try:
            with self.db.cursor() as cursor:
                sql = "SELECT now_station, next_station  FROM bus_edges ;"
                cursor.execute(sql)
                result = cursor.fetchall()
                nations = pd.DataFrame(result).values.tolist()
                for nation in nations:
                    try:
                        with self.db.cursor() as cursor:
                            sql = "SELECT lon, lat  FROM bus_stations WHERE station_name = '%s';" % nation[0]
                            cursor.execute(sql)
                            nowlocation = cursor.fetchall()
                            nowlocation_lon = nowlocation[0]['lon']
                            nowlocation_lat = nowlocation[0]['lat']
                            try:
                                sql = 'UPDATE bus_edges SET nowstation_lon = "{}", nowstation_lat = "{}" WHERE  now_station = "{}" ;'.format(
                                    nowlocation_lon, nowlocation_lat, nation[0])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
                    try:
                        with self.db.cursor() as cursor:
                            sql = "SELECT lon, lat  FROM bus_stations WHERE station_name = '%s';" % nation[1]
                            cursor.execute(sql)
                            nextlocation = cursor.fetchall()
                            nextlocation_lon = nextlocation[0]['lon']
                            nextlocation_lat = nextlocation[0]['lat']
                            try:
                                sql = 'UPDATE bus_edges SET nextstation_lon = "{}", nextstation_lat = "{}" WHERE  now_station = "{}" ;'.format(
                                    nextlocation_lon, nextlocation_lat, nation[1])  # 更新下一站信息
                                try:
                                    result = self.cursor.execute(sql)
                                    self.db.commit()
                                except pymysql.Error as e:
                                    # 回滚
                                    self.db.rollback()
                                    print("出错", e)
                            except pymysql.Error as e:
                                print("数据库错误，原因%d: %s" % (e.args[0], e.args[1]))
                    except pymysql.Error as e:
                        print("数据库错误")
        except pymysql.Error as e:
            print("数据库错误")

        # 从move_my中得到准确的电影（用于电影搜索）
    def change_nexttion(self, movie_name):
        """搜索电影"""
        try:
            with self.db.cursor() as cursor:
                sql = "SELECT *  FROM movie_my WHERE  title_Chinese = '%s';" % (movie_name)
                cursor.execute(sql)
                result = cursor.fetchall()
                df = pd.DataFrame(result)
                m = len(df)
                return df
        except pymysql.Error as e:
            print("数据库错误")

a = MySQLCommand()
a.busadd_loaction()
