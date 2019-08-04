# -*- coding: utf-8 -*-
from urllib.parse import quote
import scrapy
import json
lst = [None]
from Database.data_process import load_json
def get_line_name(file='./data/bus/bus_name.json'):
    lst = []
    with open(file, 'r') as f:  # 需要在命令行调用
        lst_ = json.load(f)
        lst_.pop(-2)    #去除地铁线路
        for l in lst_:
            lst.extend(l['0'])
    return lst


#lst = get_line_name()    #公交
#lst = get_line_name('./data/subways/name.json')   #地铁
lst = load_json('data/bus/name_miss_1.json')


class LineuidSpider(scrapy.Spider):
    name = 'lineUid'
    allowed_domains = ['map.baidu.com']
    #start_urls = ['https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=shareurl&wd=1%E8%B7%AF&c=257&src=0&pn=0&sug=0&l=13&biz_forward=%7B%22scaler%22%3A2%2C%22styles%22%3A%22pl%22%7D&device_ratio=2&auth=zI2YUWN3v2SxvQH7GwyXg3H4XKxVw9%3DBuxHLBTRzHVxtAmk5zC88y1GgvPUDZYOYIZuVt1cv3uVtcvY1SGpuEtJggagyYxPWv3GuVtUvhgMZSguxzBEHLNRTVtcEWe1GD8zv7u%40ZPuVteuVtegvcguxHLBTRzHVxtfiKKv7urZZWuV&tn=B_NORMAL_MAP&nn=0&u_loc=12624007%2C2621047&ie=utf-8&t=1563982500186']
    # 地铁
    start_urls = [f'https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd={quote(lst[-1])}&'+'c=257&src=0&wd2=&pn=0&sug=0&l=12&b=(12580484.850777779,2625722.8155555553;12637157.960111111,2634946.9544444443)&from=webmap&biz_forward=\{%22scaler%22:2,%22styles%22:%22pl%22\}&sug_forward=&auth=ZOyzyFIaL5K2w0OIGM9Z0XSWbzKCNKfGuxHLEVNNLETt0A%3DH73uzCywi04vy77uy1uVt1GgvPUDZYOYIZuVt1cv3uVtGccZcuVtPWv3GuzRt9DpnSeYnCEGHKNRTXZ%40BcEWe1GD8zv7u%40ZPuVteuxLttvJrvIKTXQgZgXKQHrvUU2PeGrZZWuxx&device_ratio=2&tn=B_NORMAL_MAP&nn=0&u_loc=12624060,2621043&ie=utf-8&t=1564077650446']
    def parse(self, response):
        uidItem = {'0': [], '1': lst[-1]}
        try:
            table = json.loads(response.body_as_unicode())
            text = table['content']
            uids = uidItem['0']
            #如果搜到了
            for i in (0, 1):
                if text[i]['acc_flag'] == 1:
                    uids.append(text[i]['uid'])
            # 如果只搜到了公交站
            if not uids:
                if 'blinfo' in text[0]:
                    for l in text[0]['blinfo']:
                        status = True
                        for w in lst[-1]:  # 所有词都需要在里
                            if w not in l['addr']:
                                status = False
                        if status:
                            uids.append(l['uid'])
                            uids.append(l['pair_uid'])
                            break
        except Exception as e:
            with open('error.txt', 'a') as f:
                f.write(response.url+'\n')
        lst.pop()
        yield uidItem
        if lst:
            url = f'https://map.baidu.com/?newmap=1&reqflag=pcmap&biz=1&from=webmap&da_par=direct&pcevaname=pc4.1&qt=s&da_src=searchBox.button&wd={quote(lst[-1])}&'+'c=257&src=0&wd2=&pn=0&sug=0&l=12&b=(12580484.850777779,2625722.8155555553;12637157.960111111,2634946.9544444443)&from=webmap&biz_forward=\{%22scaler%22:2,%22styles%22:%22pl%22\}&sug_forward=&auth=ZOyzyFIaL5K2w0OIGM9Z0XSWbzKCNKfGuxHLEVNNLETt0A%3DH73uzCywi04vy77uy1uVt1GgvPUDZYOYIZuVt1cv3uVtGccZcuVtPWv3GuzRt9DpnSeYnCEGHKNRTXZ%40BcEWe1GD8zv7u%40ZPuVteuxLttvJrvIKTXQgZgXKQHrvUU2PeGrZZWuxx&device_ratio=2&tn=B_NORMAL_MAP&nn=0&u_loc=12624060,2621043&ie=utf-8&t=1564077650446'
            yield scrapy.Request(url,self.parse,dont_filter=True)




#scrapy crawl lineUid -o ./data/subways/uid.json
#scrapy crawl lineUid -o ./data/bus/uid_.json
# 问题：查询关键词过长
# 排除倒数第二条，是地铁数据
#scrapy crawl lineUid -o ./data/bus/uid_1.json  此时将lst[-1][:6]   [:6]部分去掉
#scrapy crawl lineUid -o ./data/bus/uid_miss.json  此时将lst[-1][:6]   [:6]部分去掉
#scrapy crawl lineUid -o ./data/bus/uid_miss_1.json  此时将lst[-1][:6]   [:6]部分去掉





#缺失数据*************
# lst = list({'从化汽车站-上罗村委',
#  '从化汽车站-楼星村委',
#  '从化汽车站-横岭八队',
#  '从化汽车站-银林村委',
#  '从化汽车站-锦一',
#  '从化汽车站-龙新村委线',
#  '从化汽车站—广州南汽车客运站',
#  '从化汽车站－番禺市桥汽车站',
#  '太平开发区公交专线',
#  '广州南站-增城线',
#  '明珠管委会-华夏学院',
#  '花都炭步-市站线'})