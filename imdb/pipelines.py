# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import logging
import pymongo

class MongoDBPipeline:
    collection_name = "best_movies"
    @classmethod

    def open_spider(cls, self, spider):
        self.client = pymongo.MongoClient("mongodb+srv://yashmishra12:jgjjkkjhkj@cluster0.z847z.mongodb.net/<dbname>?retryWrites=true&w=majority")
        self.db = self.client["IMDB"] #IMDB --> database name


    def close_spider(self, spider):
        self.client.close()


    def process_item(self, item, spider):
        self.db[self.collection_name].insert(item)
        return item
