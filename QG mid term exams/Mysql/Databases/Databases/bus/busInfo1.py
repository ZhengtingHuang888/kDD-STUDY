import requests
import re

"""
已修改版，也是返回字典
"""
#返回字典
def get_res():
    info = {}
    url = 'https://map.baidu.com/?'
    headers = {
    'Referer': '''https://map.baidu.com/search/1%E8%B7%AF/@12605568.38702564,2627977.4400000004,13.95z?querytype=s&da_src=shareurl&wd=1%E8%B7%AF&c=257&src=0&pn=0&sug=0&l=13&b=(12600151,2627495;12649303,2639239)&from=webmap&biz_forward=%7B%22scaler%22:2,%22styles%22:%22pl%22%7D&device_ratio=2''',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'}
    params = {'qt': 'bsl',
           'tps': 'newmap: 1',
           'uid': 'a0a25b0223e487e1d0ff2384',  # 修改你的id
           'c': '257'}
    rsp = requests.get(url, params, headers=headers)
    dict_ = rsp.json()
    text = dict_['content'][0]

    # 提取信息
    info['uid'] = text['uid']
    info['name'] = text['name']
    #info['stations'] = text['stations']
    # 深拷贝
    # input(type(text['stations']))
    a = text['stations'][:]
    #input(a)
    info['timetable_ext'] = text['timetable_ext']
    info['headway'] = text['headway']
    info['endTime'] = text['endTime']
    # 将途径站的信息转化

    delect = ['pre_open', 'rt_info', 'tri_rt_info']
    # 遍历每一个字典
    for i in range(len(text['stations'])):
        #input(len(text['stations']))
        keys = list(text['stations'][i].keys())
        for key in keys:
            #print(key)
            if key in delect:
                #删除无用的键；
                del(a[i][key])
            if key == 'subways':
                str = re.sub("[A-Za-z\"\#\=\<\>\/]", "", text['stations'][i][key][0]['name'])
                a[i][key] = str

    info['stations'] = a
    print(info)

if __name__ == '__main__':
    get_res()
