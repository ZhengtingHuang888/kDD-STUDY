# -*- coding: utf-8 -*-
import scrapy
import json

lst = []

class LinenameSpider(scrapy.Spider):
    name = 'lineName'
    allowed_domains = ['guangzhou.8684.cn']
    start_urls = ['https://guangzhou.8684.cn/line3']
    headers = {}

    def parse(self, response):
        bus_name = '//*[@id="con_site_1"]/a'
        quotes = response.xpath(bus_name)  #结点
        item = {0:[],1:lst.pop()['name']}  # 相当于字典
        for quote in quotes:
            bname = quote.xpath('./text()').extract_first()  #
            item[0].append(bname)    #添加
        yield item
        if lst:
            yield scrapy.Request(response.url[:25]+lst[-1]['url'],self.parse)   #后续请求


if __name__ =='__main__':
    # with open('./busLine_categ.json','rb') as f:    #需要在命令行调用
    #     lst = json.load(f)

    pass
# 爬地铁数据
#scrapy crawl lineName -o ./data/subways/name.json
