#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 26 02:06:19 2022

@author: ashish
"""

import logging
import yaml
from kafka import KafkaProducer

class fileTransfer:
    
    def __init__(self):
        
        logging.basicConfig(filename="event.log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',level = logging.DEBUG)
        self.logger = logging.getLogger()
        self.config = self.init_config()
        kafka = self.config["application"]["kafka"]
        self.topic = kafka["topic"]
        print(self.topic)
        self.producer = self.connect_kafka()
        
    
    def init_config(self):
        
       self.logger.info('Reading Configuration file')
       with open("config.yml", "r") as stream:
           try:
               return yaml.safe_load(stream)
               self.logger.info(self.config)
           except yaml.YAMLError:
               self.logger.error("Unable to read config file", exc_info=True)
               
    
    def connect_kafka(self):
        
        bootstrap_servers = [self.config["application"]["kafka"]["server"]]
        try:
            self.logger.info('Connecting to kafka')
            producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
            producer = KafkaProducer()
            self.logger.error("Unable to read config file", exc_info=True)
            return producer
        except:
            self.logger.error("Unable to ", exc_info=True)
        

  
    
    
if __name__ == "__main__":
    obj = fileTransfer()