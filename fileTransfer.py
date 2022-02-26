#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 26 02:06:19 2022

@author: ashish
"""

import logging
import yaml
import os
from kafka import KafkaProducer
from datetime import datetime

class fileTransfer:
    
    def __init__(self):
        
        logging.basicConfig(filename="event.log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',level = logging.DEBUG)
        self.logger = logging.getLogger()
        self.config = self.init_config()
        self.logger.info(self.config)
        kafka = self.config["application"]["kafka"]
        self.topic = kafka["topic"]
        self.producer = self.connect_kafka()
        self.now = datetime.now()
        
    
    def init_config(self):
        
       self.logger.info('Reading Configuration file')
       with open("config.yml", "r") as stream:
           try:
               return yaml.safe_load(stream)
           except yaml.YAMLError:
               self.logger.error("Unable to read config file", exc_info=True)
               
    
    def connect_kafka(self):
        
        bootstrap_servers = [self.config["application"]["kafka"]["server"]]
        try:
            self.logger.info('Connecting to kafka')
            producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
            producer = KafkaProducer()
            self.logger.info("Connected to kafka", exc_info=True)
            return producer
        except:
            self.logger.error("Unable to connect to kafka", exc_info=True)
            
    def transfer(self):
        base_path = os.getcwd()
        source = base_path+self.config["application"]["sourceDir"]
        destination = base_path+self.config["application"]["destinationDir"]
        try :
            files = os.listdir(source)
            self.logger.info('Reading files from the source directory')
        except:
            self.logger.error("Unable to read files from source folder", exc_info=True)
        try :
            for file in files:
                os.rename(source + file, destination + file)
                self.logger.info('%s moved to destination ', file)
                current_time = self.now.strftime("%H:%M:%S")
                data = {"time":current_time,"file_path":destination + file}
                ack = self.producer.send(self.topic, bytes(str(data), 'utf-8'))
                self.logger.info('Meta data pushed to kafka topic %s', data)
        except:
            self.logger.error("Error in file transfer", exc_info=True)
            
        
        
    
if __name__ == "__main__":
    obj = fileTransfer()
    obj.transfer()