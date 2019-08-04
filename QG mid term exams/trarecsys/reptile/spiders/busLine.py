# -*- coding: utf-8 -*-
import scrapy
from scrapy.http.response import html
from bus_lines.items import BusLinesItem
# a = html.HtmlResponse.xpath(1,3)


class BusLineSpider(scrapy.Spider):
    name = 'BusLineSpider'
    allowed_domains = ['guangzhou.8684.cn']
    start_urls = ['https://guangzhou.8684.cn']

    def parse(self, response):
        bus_categ = '/html/body/div[6]/div[1]/div[4]/div/a'     #公交线类别
        quotes = response.xpath(bus_categ)  #结点
        for quote in quotes:
            item = BusLinesItem()   #相当于字典
            item['name'] = quote.xpath('./text()').extract_first()  #
            item['url'] = quote.xpath('./@href').extract_first()  #extract()
            yield item


