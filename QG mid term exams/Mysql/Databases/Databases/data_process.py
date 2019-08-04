import json
import DataStorage_total
import conversion
from myfunc import duplicate
from geopy.distance import distance

def load_json(f):
    with open(f,'r') as f:
        return json.load(f)
def single_uids(f):
    uids = []
    for i in load_json(f):
        uid = i['0']    #[uid1,uid2]
        if len(uid) == 1:
            uids.append(uid[0])
    return uids
#检查路的两个方向
def d_check(f='bus/Info__total.json',stype='subways'):
    info = load_json(f)
    Info = []
    # 通过站名、起点终点坐标检测
    i = 0
    while i < len(info)-1:
        a,b = [],[]
        # start_station
        a.append(info[i]['stations'][0]['name'])
        l, od = info[i]['name'][:-1].split('(',1)
        a.append(l)

        b = []
        # start_station
        b.append(info[1+i]['stations'][-1]['name'])
        # line
        l, od = info[i+1]['name'][:-1].split('(', 1)
        b.append(l)

        if a==b:
            print(l,"公交线测试通过")
            i += 2
            continue
        # Info.append(tuple(a)+(info[i]['name'],))    #元组可作为字典的键
        Info.append(info[i]['name'])        #存入id信息
        #Info.append(b) 前面可能只爬了一个
        i += 1
        #print(a)
        #print(b)
    if i <len(info):
        Info.append(info[i]['uid'])

    print("未通过数",len(Info))
    print("不重复数",len(set(Info)))
    from myfunc import duplicate
    return Info,duplicate(Info)
# 反转线路
def direction_check(info,keys):
    '''
    :param info:
    :param key: [uid]
    :return:
    '''
    # 反转线路
    if keys is None:return info
    info_ = info[:]
    for i in info:
        if i['uid'] in keys:
            stations =i['stations'][::-1]
            name = i['name']
            # 线名反转
            l, od = name[:-1].split('(')
            ori, des = od.split('-')
            name= l + '(' + des + '-' + ori + ')'
            wait_time = i['headway']
            endtime = l['endTime']
            info_.append({'stations':stations,'name':name,'headway':wait_time,'endTime':endtime})
    return info_

def update_line():
    with open('../bus/Info__total.json','r') as f: #../bus/Info_.json
        info = json.load(f)
    for i in range(len(info)):
        # 线路uid
        name = info[i]['name']
        #第j个途径站
        for j in range(len(info[i]['stations'])):
            # station name:
            # if info[i]['stations'][j]['name'] == '地铁从化客运站②':
            #     input('hao')
            if info[i]['stations'][j]['name'] == '地铁从化客运站2':
                print("修改")
                info[i]['stations'][j]['name'] = '地铁从化客运站2_'
                with open('../bus/Info__total.json', 'w') as f:  # ../bus/Info_.json
                    json.dump(info,f)
                return True
def uid_check(uids,f='bus/uid_.json'):
    info = load_json(f)
    uids = list(set(uids))
    #根据uid，查阅是否是uid列表是否有两个值
    l_uid = []
    l_uid_dou = []
    # 单向路的长度
    for i in info:
        uid = i['0']    #[uid1,uid2]
        if len(uid) == 1:
            l_uid.append(uid[0])
        else:
            l_uid_dou.extend(uid)
    print('单向路的长度',len(l_uid))
    nonSin = [];nonDub = []
    for u in uids:
        if u not in l_uid:
            #print(u)
            nonSin.append(u)
            if u in l_uid_dou:
                input(u)
                nonDub.append(u)

    print('未匹配的单向路长度',len(nonSin))
    print('未匹配的d单双向路长度', len(nonSin))
#检查uid对应的路线名
def line_check(f='bus/Info__total.json',uid='75d6abd2097fb5d8ce04ef0a'):
    with open(f,'r') as f: #../bus/Info_.json
        info = json.load(f)
        for i in info:
            if i['uid'] == uid:
                name = i['name']
                dbm.check_id()
                id = dbm.d_lId[name]
                print(name,id)
                break
#从uid文件找出重复值
def getDupliUid():
    info =load_json('bus/uid_.json')
    keys = [tuple(i["0"]) for i in info]
    depli = duplicate(keys)
    return depli



#从数据库中找出单向的线
def getSingleLine():
    info = []
    for i in dbm.qSql('SELECT line FROM bus_line;'):
        name = i['line']
        l, od = name[:-1].split('(',1)
        ori, des = od.split('-')
        info.append((ori,des,l))
    for s,e,l in info:
        if (e,s,l) in info:
            try:
                info.remove((s,e,l))
                info.remove((e,s,l))
            except Exception as e:
                print(e)
    dupl = duplicate(info)
    return dupl#[i for i in dupl if len(dupl[1]) ==2]







class Analysis:
    def __init__(self,dbm=DataStorage_total.MySQLCommand()):
        self.dbm =  dbm #数据管理器
        self.special  =('广佛线','知识城线')
        self.single_uids = []
    def insert_nodes(self,info,stype='subways'):
        info = direction_check(info,self.single_uids)
        for l in info:  # 每条路线
            # 1.线路信息
            line = l['name']
            stations = l['stations']
            start_station =stations[0]['name']
            end_station = stations[-1]['name']
            wait_time = l['headway']
            endtime = l['endTime']
            self.dbm.line_feature([start_station,end_station,wait_time,endtime,line],stype)
            for j in range(len(stations)): # 每个站
                # 2.站点信息
                name = stations[j]['name']
                lon = stations[j]['geo'][2:13]
                lat = stations[j]['geo'][14:24]
                lon, lat = conversion.get_lonlat(float(lon), float(lat))
                self.dbm.stations([name,lon,lat,line])  #1.存储站点信息;  注意：若单独调用，需要检查id信息
    def insert_edges(self,info,stype='subways', etype='subways'):
        self.dbm.check_id()  #检查内存id
        for l in info:  # 每条路线
            stations = l['stations']
            line = l['name']
            for j in range(len(stations)-1): # 每个站
                name = stations[j]['name']
                next_name = stations[j + 1]['name']
                weight = 1  # 权重
                self.dbm.edges([name, next_name, weight, line], stype, etype)


    def insert_network(self,f,stype='subways'):
        # 按顺序
        info = load_json(f)
        self.insert_nodes(info,stype)
        self.insert_edges(info,stype,stype)
    def multi_network(self,info):
        weight = 1
        self.dbm.check_id()
        Info = []
        success = []
        for l in info:  # 每条路线
            for s in l['stations']:  # 每个站
                if 'subways' in s:
                    b_station = s['name']
                    #  根据名字查询id
                    b_id = self.dbm.d_sId[b_station]
                    if b_id < 225:
                        print(s['name'])
                        input()
                    b_lon,b_lat = self.dbm.d_sPos[b_id]

                    # 调整
                    line = s['subways'][1:]
                    if line == '线':
                        line = 'apm线'
                    elif line == '3号线北延线':
                        line = '地铁' + line[:-1]
                    elif line in self.special:
                        pass
                    else:
                        line = '地铁' + line
                    #  根据名字查询地铁线id，正则
                    sql = f'SELECT id,line FROM subway_line WHERE line REGEXP "{line}"'
                    for item in self.dbm.qSql(sql):
                        line_id, line_name = item.values()
                        if line_name[:len(line)] == line:
                            #  根据地铁线id，查地铁站id
                            sql = f'SELECT nowstation_id FROM edges_ WHERE line_id ={line_id}'  ##不用必须保证，站点按(id,line_id)联合存储
                            best_dist = 1e9
                            best_sub_id = 0
                            for dic in self.dbm.qSql(sql):
                                sub_id = dic['nowstation_id']
                                sub_lon,sub_lat = self.dbm.d_sPos[sub_id]
                                dist = distance((b_lat,b_lon),(sub_lat,sub_lon)).m
                                if dist < best_dist :
                                    best_dist = dist
                                    best_sub_id = sub_id
                            # self.dbm.multi_edges([b_id,best_sub_id,weight])
                            # self.dbm.multi_edges([best_sub_id,b_id,weight],'subways-bus')
                            # Info.append([b_id,best_sub_id,weight])
                            #Info.append([best_sub_id,b_id,weight])
                            if line not in success:  # 调试
                                print(line)
                            success.append(line)
                            # 根据id查询所有的站点id
                    break

    def updateS(self,**kwargs):
        self.__dict__.update(kwargs)

if __name__ == '__main__':
    f = 'bus/Info__total.json'
    dbm = DataStorage_total.MySQLCommand()
    a = Analysis(dbm)
    # dbm.clear()
    # f ='subway/info3.json'
    # a.insert_network(f)
    # f = 'bus/Info__total.json'
    # a.updateS(single_uids=single_uids('bus/uid_1.json'))
    # a.insert_network(f,'bus')
    a =getSingleLine()
