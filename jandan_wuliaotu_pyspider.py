#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2017-03-15 11:14:25
# Project: jandan_wowtu
import json
from pyspider.libs.base_handler import *
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
mq_name = "jandan_wuliaotu"
channel.queue_declare(queue=mq_name, durable=True)

class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes=24 * 60)
    def on_start(self):
        self.crawl('http://jandan.net/pic/page-637', callback=self.index_page)

    @config(age=10 * 24 * 60 * 60)
    def index_page(self, response):
        for each in response.doc('a[class="previous-comment-page"]').items():
            self.crawl(each.attr.href, callback=self.index_page)
        result_list = []
        page = response.doc('span[class="current-comment-page"]').text()        
        for each_div in response.doc('div[class=text]').items():          
            result = self.extract_info(each_div)
            result_list.append(result)
        final_result = {
        "page": page,
        "list": result_list,
        }

        return final_result
    
    def extract_info(self, div):
        text = div.find('p')
        if text:
            text = text.text().replace(u'[查看原图]', '')
        else:
            text = ""

         
        oo = div.find('span[id^=cos_support]')
        xx = div.find('span[id^=cos_unsupport]')
        if oo and xx:
            oo = oo.text()
            xx = xx.text()
        else:
            oo = 0
            xx = 0

        img_list = []
        img_a_list = div.find('a[class=view_img_link]')
        for a in img_a_list.items():
            img_list.append(a.attr.href)

        return {
            "img_list": img_list,
            "xx":xx,
            "oo":oo,
            "text": text,
        }
   
    
    
    def on_result(self, result):
        # 重写 on_result 函数
        if not result:
            return
        assert self.task, "on_result can't outside a callback."
        result['callback'] = self.task['process']['callback']
        if self.is_debugger():
            bodys = self.process_message_to_json(result)
            for body in bodys:
                print(body)
                channel.basic_publish(exchange='', 
                              routing_key=mq_name,
                              body=body,
                              properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                              ))        
        if self.__env__.get('result_queue'):            
            bodys = self.process_message_to_json(result)
            for body in bodys:                
                channel.basic_publish(exchange='', routing_key=mq_name, body=body)        
            self.__env__['result_queue'].put((self.task, result))
    
    
    def process_message_to_json(self, result):
        page_number = result.get('page')
        folder_name = page_number
        lst = result.get('list')
        bodys = []
        for idx, item in enumerate(lst):
            oo = item['oo']
            xx = item['xx']
            sub_foldername = oo + "_" + xx + "_" + str(idx)
            message = {
            "folder_name": folder_name,
            "sub_folder_name": sub_foldername,
            "img_list": item['img_list'],
            "text": item["text"],
            }
            body=json.dumps(message)    
            bodys.append(body)
        return bodys
    
    
    
    
    
    
    
    
    
    
    
    
    
    
