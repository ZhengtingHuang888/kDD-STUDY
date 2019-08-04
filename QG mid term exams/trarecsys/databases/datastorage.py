import pymysql.cursors  #请求
import pymysql
from pygeohash import encode,decode
import conversion

# 使用时需修改数据库连接信息
class MySQLCommand(object):
    def __init__(self, user='root', passwords = 'hzt123'):
        self.config = {
            'host': "localhost",
            'user': user,
            'password': passwords,
            'db': 'trases',
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        try:
            self.db = pymysql.connect(**self.config)
            self.cursor = self.db.cursor()
        except:
            print("connect mysql error.")
        self.settings()

    def settings(self):
        self.sId = 0    #station_id
        self.lId = 0    #line_id
        self.d_lId = {} #dict of station_id
        self.d_sId = {}
        self.d_sPos = {}    #dict of station_position
        self.table = {
            'subways': {'stype': 0, 'edges': 'subway_edges', 'line_feature': 'subway_line'},
            'bus': {'stype': 1, 'edges': 'bus_edges', 'line_feature': 'bus_line'},
        }
        self.etypes = {
            'subways':0,
            'bus':1,
            'bus-subways':2,
            'subways-bus':3
        }
    def check_id(self):
        sql = 'SELECT id,line FROM bus_line'
        self.cursor.execute(sql)
        for item in self.cursor.fetchall():
            id,line = item.values()
            self.d_lId[line] =id
        sql = 'SELECT id,station_name,lon,lat FROM stations'
        self.cursor.execute(sql)
        for item in self.cursor.fetchall():
            id,station_name,lon,lat = item.values()
            self.d_sId[station_name] =id
            self.d_sPos[id] = lon,lat

    def clear(self):
        tables =('stations','subway_line','bus_line' ,'edges_')
        for i in tables:
            self.cursor.execute(f'DELETE FROM {i};')
        self.db.commit()

    #存储站点信息
    def stations(self, dataSet,stype='subways'):
        """
                        存入地铁相关数据的函数
                        :param dataSet:  ['station_name', lon, lat,line]
                        :return:
        """

        stype = self.table[stype]['stype']
        station_name = dataSet[0]
        line_id = self.d_lId[dataSet[-1]]
        try:
            # 询问是否重复
            qupl = f'SELECT id FROM stations WHERE stype = {stype} and station_name = "{station_name}";' #规则组织
            rst = self.cursor.execute(qupl)
            if self.cursor.fetchone() is None:
                lon,lat = dataSet[1:-1]
                sql = f'INSERT INTO stations(id, station_name, lon, lat ,stype ,line_id) ' \
                    f'VALUES ({self.sId}, "{station_name}", {lon},{lat}, {stype},{line_id});'
                try:
                    result = self.cursor.execute(sql)
                    self.d_sId[station_name] = self.sId
                    self.d_sPos[self.sId] = (lon,lat)
                    self.sId +=1
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
    def stations_(self, dataSet,stype='subways'):
        """
                        存入地铁相关数据的函数
                        :param dataSet:  ['station_name', lon, lat,line]
                        :return:
        """
        stype = self.table[stype]['stype']
        station_name = dataSet[0]
        line_id = self.d_lId[dataSet[-1]]
        try:
            # 询问是否重复
            qupl = f'SELECT id FROM stations WHERE stype = {stype} and station_name = "{station_name}";' #规则组织
            rst = self.cursor.execute(qupl)
            if self.cursor.fetchone() is None:
                lon,lat = dataSet[1:-1]
                sql = f'INSERT INTO stations(id, station_name, lon, lat ,stype ,line_id) ' \
                    f'VALUES ({self.sId}, "{station_name}", {lon},{lat}, {stype},{line_id});'
                try:
                    result = self.cursor.execute(sql)
                    self.d_sId[station_name] = self.sId
                    self.d_sPos[(self.sId,line_id)] = (lon,lat)
                    self.sId +=1
                    self.db.commit()
                except pymysql.Error as e:
                    # 回滚
                    self.db.rollback()
                    if "key 'PRIMARY'" in e.args[1]:
                        print("数据已经存在，未插入数据")
                    else:
                        print("插入数据失败，原因 %d: %s" % (e.args[0], e.args[1]))
            else:
                #print(dataSet[-1],line_id)
                qupl = f'SELECT line_id FROM stations WHERE stype = {stype} and station_name = "{station_name}" and line_id = {line_id};' #规则组织
                rst = self.cursor.execute(qupl)
                if self.cursor.fetchone() is None:
                    lon, lat = dataSet[1:-1]
                    sId = self.d_sId[station_name]
                    sql = f'INSERT INTO stations(id, station_name, lon, lat ,stype ,line_id) ' \
                        f'VALUES ({sId}, "{station_name}", {lon},{lat}, {stype},{line_id});'
                    try:
                        result = self.cursor.execute(sql)
                        self.d_sPos[(sId, line_id)] = (lon, lat)
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
    def edges(self, dataSet,stype='subways',etype='subways'):
        """
        存入地铁算法相关的数据库
        :param dataSet: [start_id, end_id, weight, line]
        :return: start_id  当前站点的uid； end_id  结尾站点的uid, weight为1， line当前所属的号线
        """
        # table = self.table[stype]['edges']
        etype = self.etypes[etype]

        # 询问是否存在
        now_station = dataSet[0]
        next_station = dataSet[1]
        nowstation_id = self.d_sId[now_station]  # 这里可以选择加一个从数据库查询
        nextstation_id = self.d_sId[next_station]
        qupl = f'SELECT weight FROM edges_ WHERE nowstation_id = {nowstation_id} and nextstation_id = {nextstation_id};'
        self.cursor.execute(qupl)
        if self.cursor.fetchone() is None:
            line = dataSet[3]
            line_id = self.d_lId[line]
            sql = f'INSERT INTO edges_ (weight,nowstation_id,nextstation_id,etype,line_id) ' \
                f'VALUES ({dataSet[2]},{nowstation_id},{nextstation_id},{etype},{line_id});'
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
    #地铁属性
    def line_feature(self, dataSet,stype='subways'):
        # 地铁属性操作
        """
        start_id 起始站点， end_id终点站点, price价格, wait_time大约等待时间,
        :param dataSet:
        :return:
        """
        table = self.table[stype]['line_feature']
        line = dataSet[4]
        try:
            # 询问是否存在
            qupl = f'SELECT id FROM {table} WHERE line = "{line}"'
            self.cursor.execute(qupl)
            if self.cursor.fetchone() is None:
                sql = f'INSERT INTO {table}(id, start_station, end_station, wait_time,  work_endtime, line) VALUES ' \
                    f'({self.lId}, "{dataSet[0]}", "{dataSet[1]}", "{dataSet[2]}", "{dataSet[3]}","{line}");'
                try:
                    result = self.cursor.execute(sql)
                    self.d_lId[line] = self.lId
                    self.lId += 1
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

    def multi_edges(self,dataSet,etype='bus-subways'):
        """
        存入地铁算法相关的数据库
        :param dataSet: [start_id, end_id, now_station,next_station,weight]
        :return: start_id  当前站点的uid； end_id  结尾站点的uid, weight为1， line当前所属的号线
        """
        etype = self.etypes[etype]
        try:
            # 询问是否存在\
            nowstation_id = dataSet[0] # 这里可以选择加一个从数据库查询
            nextstation_id = dataSet[1]
            qupl = f'SELECT weight FROM edges_ WHERE nowstation_id = {nowstation_id} and nextstation_id = {nextstation_id};'
            self.cursor.execute(qupl)
            if self.cursor.fetchone() is None:

                sql = f'INSERT INTO edges_ (weight,nowstation_id,nextstation_id,etype) ' \
                      f'VALUES ({dataSet[2]},{nowstation_id},{nextstation_id},{etype});'
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


    def qStationId(self):
        sql = 'SELECT id,station_name  FROM stations;'
        self.cursor.execute(sql)
        # 如果不出现，单向起点
        self.d_sId = {}
        for i in self.cursor.fetchall():
            id,station_name = i.values()
            self.d_sId[station_name] = id

        return self.d_sId

    def qEdge(self,stype='subways'):
        stations = self.table[stype]['edges']
        sql = f'SELECT nowstation_id,nextstation_id  FROM {stations};'
        self.cursor.execute(sql)
        return [[i['nowstation_id'], i['nextstation_id']] for i in self.cursor.fetchall()]
    def qSql(self,sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def get_yg(self):
        sql = 'SELECT id,lon,lat  FROM stations ;'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        for dict in result:
            print(dict)
            print(dict['lat'],dict['lon'])
            lat,lon = conversion.get_lonlat(dict['lat'],dict['lon'])
            print(lat,lon)
            geocode = encode(lat,lon)
            a,b = decode(geocode)
            print(a,b)
            sql = 'UPDATE stations SET geohash = "%s" WHERE  id = %d ;' % (geocode, dict['id'])  # 更新下一站信息
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        print(result)

    def get_fiveyg(self):
        sql = 'SELECT id,lon,lat  FROM stations ;'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        for dict in result:
            print(dict)
            print(dict['lat'],dict['lon'])
            lat,lon = conversion.get_lonlat(dict['lat'],dict['lon'])
            print(lat,lon)
            geocode = encode(lat,lon)
            a,b = decode(geocode)
            print(a,b)
            #input(geocode)
            sql = 'UPDATE stations SET five_geohash = "%s" WHERE  id = %d ;' % (geocode[:5], dict['id'])  # 更新下一站信息
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        print(result)

    def get_fouryg(self):
        sql = 'SELECT id,lon,lat  FROM stations ;'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        for dict in result:
            print(dict)
            print(dict['lat'],dict['lon'])
            lat,lon = conversion.get_lonlat(dict['lat'],dict['lon'])
            print(lat,lon)
            geocode = encode(lat,lon)
            a,b = decode(geocode)
            print(a,b)
            #input(geocode)
            sql = 'UPDATE stations SET four_geohash = "%s" WHERE  id = %d ;' % (geocode[:4], dict['id'])  # 更新下一站信息
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        print(result)
