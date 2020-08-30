# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import logging
import sqlite3

# class MongoDBPipeline:
#     collection_name = "best_movies"
#     @classmethod

#     def open_spider(cls, self, spider):
#         self.client = pymongo.MongoClient("mongodb+srv://yashmishra12:jgjjkkjhkj@cluster0.z847z.mongodb.net/<dbname>?retryWrites=true&w=majority")
#         self.db = self.client["IMDB"] #IMDB --> database name


#     def close_spider(self, spider):
#         self.client.close()


#     def process_item(self, item, spider):
#         self.db[self.collection_name].insert(item)
#         return item


class SQLlite2Pipeline:

    def open_spider(self, spider):
        self.connection = sqlite3.connect("imdb.db")
        self.c = self.connection.cursor()
        self.c.execute('''
            CREATE TABLE best_movie(
                title TEXT,
                year TEXT,
                duration TEXT,
                genre TEXT,
                rating TEXT,
                release_date TEXT
            )
        ''')

        self.connection.commit()


    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):
        self.c.execute('''
            INSERT INTO best_movies( title,
                year,
                duration,
                genre,
                rating,
                release_date) values (?, ?, ?, ?, ?, ?)
        ''', (
            item.get('title'),
            item.get('year'),
            item.get('duration'),
            item.get('genre'),
            item.get('rating'),
            item.get('release_date')
        ))
        self.connection.commit()
        return item

class SQLlitePipeline(object):

    def open_spider(self, spider):
        self.connection = sqlite3.connect("imdb.db")
        self.c = self.connection.cursor()
        try:
            self.c.execute('''
                CREATE TABLE best_movies(
                    title TEXT,
                    year TEXT,
                    duration TEXT,
                    genre TEXT,
                    rating TEXT,
                    release_date TEXT
                )
            
            ''')
            self.connection.commit()
        except sqlite3.OperationalError:
            pass

    def close_spider(self, spider):
        self.connection.close()


    def process_item(self, item, spider):
        self.c.execute('''
            INSERT INTO best_movies (title,year,duration,genre,rating,release_date) VALUES(?,?,?,?,?,?)

        ''', (
            item.get('title'),
            item.get('year'),
            item.get('duration'),
            item.get('genre'),
            item.get('rating'),
            item.get('release_date')
        ))
        self.connection.commit()
        return item