# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
##from itemadapter import ItemAdapter
import pymongo

##from scrapy.conf import settings
from scrapy.exceptions import DropItem
import logging

class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            'mongodb+srv://dhiraj:kMhCIfTBzRoLx2Ur@dhirajtestcluster.61f6q.mongodb.net/stackoverflow?retryWrites=true&w=majority'
        )
        self.connection = connection['stackoverflow']
##        client = pymongo.client(settings['MONGO_URL'])
##        self.connection = client.questions

    def process_item(self, item, spider):
        
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem('MISSING {0}!'.format(data))

        if valid:
            self.connection.questions.insert_one(dict(item))
            logging.msg("Question added to MongoDB database!",
                    level=logging.DEBUG, spider=spider)

        return item
