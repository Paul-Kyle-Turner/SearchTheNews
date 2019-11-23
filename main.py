import argparse
import configparser
import datetime
from datetime import timedelta
import sqlite3
import json
import numpy as np
import pandas as pd
from newsapi import NewsApiClient
import http.client
import re

DEFAULT_CONFIG = 'config.ini'


class NewsGather:

    def __init__(self, api_key_news=None, api_key_rapid=None, database_path=None, json_path=None, pickle_path=None, verbose=False):
        self.verbose = verbose
        if api_key_news is None and api_key_rapid is None:
            print("No keys provided, fail")
            exit(1)
        if api_key_news is None:
            print("NO API_NEWS_KEY results only from rapid")
        else:
            self.news = NewsApiClient(api_key_news)
        if api_key_rapid is None:
            print("NO API_RAPID_KEY results only from newsapi")
        else:
            self.api_key_rapid = api_key_rapid
        self.database_path = database_path
        if self.database_path is not None:
            self.database = True
            self.database_setup()
        else:
            self.database = False
        self.json_path = json_path
        self.pickle_path = pickle_path

    # Taken from stack overflow
    @staticmethod
    def date_range(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    @staticmethod
    def date_to_string(date):
        return date.strftime("%Y-%m-%d")

    @staticmethod
    def source_to_name(results):
        for article in results['articles']:
            article['source'] = article['source']['name']
        return results

    def search_date_range(self, query, start_date, end_date, search_everywhere,
                          pages=10, database=False, json_f=True, pickle_f=False):
        for day in self.date_range(start_date, end_date):
            if self.verbose:
                print(f"Searching within date range {start_date}, {end_date}."
                      f"  At day {day}. Ending at day {day + timedelta(1)}")
            self.search_to_output(query, search_everywhere=search_everywhere, start_date=day,
                                  end_date=day + timedelta(1), database=database,
                                  json_f=json_f, pickle_f=pickle_f, pages=pages)

    def search_top(self, query):
        if self.verbose:
            print(f"Searching the top news headlines for {query}.")
        results = self.source_to_name(self.news.get_top_headlines(q=query, language='en', country='us'))
        return results

    def search_everywhere(self, query, start_time, end_time=datetime.datetime.now(), pages=10):
        results = dict()
        for i in range(1, pages):
            if self.verbose:
                print(f"Searching in page {i}, for query {query}.")
            result = self.news.get_everything(q=query, from_param=self.date_to_string(start_time),
                                              to=self.date_to_string(end_time), language='en', sort_by='relevancy')
            results.update(result)
        return self.source_to_name(results)

    def search_rapid(self, query, api_key_rapid=None, pages=10, start_date=None, end_date=None):
        if api_key_rapid is not None:
            self.api_key_rapid = api_key_rapid
        conn = http.client.HTTPSConnection("contextualwebsearch-websearch-v1.p.rapidapi.com")

        headers = {
            'x-rapidapi-host': "contextualwebsearch-websearch-v1.p.rapidapi.com",
            'x-rapidapi-key': self.api_key_rapid
        }

        result = []

        for i in range(1, 10):
            if start_date is not None and end_date is not None:
                conn.request("GET",
                             f"/api/Search/NewsSearchAPI?fromPublishedDate={start_date}&toPublishedDate={end_date}"
                             f"&autoCorrect=false&pageNumber={i}&pageSize={pages}&q={query}&safeSearch=false",
                             headers=headers)
            else:
                conn.request("GET",
                             f"/api/Search/NewsSearchAPI?autoCorrect=false&pageNumber={i}"
                             f"&pageSize={pages}&q={query}&safeSearch=false",
                             headers=headers)

            res = conn.getresponse()
            results = res.read().decode('utf-8')
            results = json.loads(self.replacer(results))['value']
            results = self.rapid_list_fix(results)
            result.append(results)
        return result

    def search_to_output(self, query, search_everywhere=False, start_date=None, end_date=datetime.datetime.now(),
                         database=False, json_f=False, pickle_f=False, pages=10, database_path=None):
        if not database and not json_f and not pickle_f:
            return
        else:
            if self.api_key_rapid is None and self.news is None:
                print("FAILED TO HAVE NEWS API")
                print("Please input an api key")
                return
            else:
                if self.news is not None:
                    if search_everywhere:
                        results = self.search_everywhere(query, start_date, end_date, pages=pages)
                    else:
                        results = self.search_top(query)
                    self.output(query, results, database, json_f, pickle_f, rapid=False, database_path=database_path)
                if self.api_key_rapid is not None:
                    results = self.search_rapid(query, pages=pages)
                    self.output(query, results, database, json_f, pickle_f, rapid=True, database_path=database_path)

    def output(self, query, results, database, json_f, pickle_f, database_path, rapid=False):
        if database:
            if self.database_path is None:
                self.set_database_path(database_path)
            if self.database_path is None:
                print("FAILED TO SETUP DATABASE PATH")
            else:
                if not rapid:
                    self.to_database_news(query, results, self.database_path)
                else:
                    self.to_database_rapid(query, results, self.database_path)
        if json_f:
            self.to_json(results)
        if pickle_f:
            self.to_pickle(results)

    def set_database_path(self, database_path):
        if database_path is not None:
            self.database_path = database_path

    def to_database_news(self, query, results, database_path=None):
        if self.database is False:
            self.database_setup(database_path)
        self.set_database_path(database_path)
        if self.verbose:
            print("Connect to database, placing documents in table.")
        connection = sqlite3.connect(self.database_path)
        cursor = connection.cursor()
        for result in results['articles']:
            cursor.execute('''
            INSERT OR IGNORE INTO documents(source, query, author, title, description, url, url_to_image, published_at, content)
            VALUES(?,?,?,?,?,?,?,?,?)''',
                           [result['source'],
                            query,
                            result['author'],
                            result['title'],
                            result['description'],
                            result['url'],
                            result['urlToImage'],
                            result['publishedAt'],
                            result['content']])
        connection.commit()
        cursor.close()

    def to_database_rapid(self, query, results, database_path=None):
        if self.database is False:
            self.database_setup(database_path)
        self.set_database_path(database_path)
        if self.verbose:
            print("Connect to database, placing documents in table.")
        connection = sqlite3.connect(self.database_path)
        cursor = connection.cursor()
        results = results[0]
        for result in results:
            cursor.execute('''
            INSERT OR IGNORE INTO documents(source, query, author, title, description, url, url_to_image, published_at, content)
            VALUES(?,?,?,?,?,?,?,?,?)''',
                           [result['provider'],
                            query,
                            None,
                            result['title'],
                            result['description'],
                            result['url'],
                            result['image'],
                            result['datePublished'],
                            result['body']])
        connection.commit()
        cursor.close()

    def database_setup(self, database_path=None):
        self.set_database_path(database_path)
        if self.database_path is None:
            print('FAILED AT CREATION OF DATABASE')
            print('NO PATH GIVEN')
            return
        if self.verbose:
            print(f"Setting up database {self.database_path}")
        connection = sqlite3.connect(self.database_path)
        cursor = connection.cursor()
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS documents(
                id integer PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                query TEXT,
                author TEXT,
                title TEXT,
                description TEXT,
                url TEXT,
                url_to_image TEXT,
                published_at TEXT NOT NULL,
                content BLOB
                )''')
        cursor.close()
        self.database = True

    def grab_from_documents_table(self, database_path=None):
        self.set_database_path(database_path)
        if self.database_path is None:
            print("FAILED AT CONNECTION OF DATABASE")
            return
        if self.verbose:
            print(f"Connecting database {self.database_path}")
        connection = sqlite3.connect(self.database_path)
        cursor = connection.cursor()
        cursor.execute('''
        SELECT id, description, content, title
        FROM documents;''')
        content = cursor.fetchall()
        return content

    def to_master_content(self, contents_table):

        content_without_id = list()

        for content in contents_table:
            content = [content[1], content[2], content[3]]
            content_without_id.append(content)

        content_without_id = self.to_single_content(content_without_id)

        content_with_id = list()

        for i in range(len(contents_table)):
            content_with_id.append([contents_table[i][0], content_without_id[i]])

        return content_with_id

    @staticmethod
    def to_single_content(contents_table):
        space = " "
        for content in contents_table:
            content = re.sub(r'\n', '', space.join(content))
        return contents_table

    def to_json(self, results):
        if self.verbose:
            print("Placing data into json file.")
        results = self.source_to_name(results)
        with open(self.json_path, 'w') as file:
            json.dump(results, file)

    def to_pickle(self, results):
        if self.verbose:
            print("Placing data into pandas dataframe pickle file.")
        results = self.source_to_name(results)
        articles = results['articles']
        data = pd.DataFrame.from_dict(articles)
        data.to_pickle(self.pickle_path)

    @staticmethod
    def replacer(data):
        data = re.sub(r'\\n+', ' ', data)
        data = re.sub(r'\s+', ' ', data)
        data = re.sub(r'</*b>', '', data)
        return data

    @staticmethod
    def rapid_list_fix(results):
        for result in results:
            result['provider'] = result['provider']['name']
            result['image'] = result['image']['url']
        return results


def settings(args):
    # Settings configuration, defaults can be changed in the config file
    config = configparser.ConfigParser()
    if args.config_file is None:
        config.read(DEFAULT_CONFIG)
    else:
        config.read(args.config_file)

    if args.kapi is None:
        api_key = config['DEFAULT']['news_api']
    else:
        api_key = args.kapi

    if args.rapid_api is None:
        rapid = config['DEFAULT']['rapid_news']
    else:
        rapid = args.rapid_api

    if args.pickle_file is None:
        pickle_path = config['DEFAULT']['picklePath']
    else:
        pickle_path = args.pickle_path

    if args.database_path is None:
        database_path = config['DEFAULT']['database']
    else:
        database_path = args.database_path

    if args.json_file is None:
        json_file = config['DEFAULT']['json']
    else:
        json_file = args.json_file

    return api_key, rapid, pickle_path, database_path, json_file


def main():
    # Argument parser for simple settings changes
    parser = argparse.ArgumentParser()
    parser.add_argument('query', help='Search query')
    parser.add_argument('-s', '--start_date', nargs=3, type=int,
                        help='Start date for gathering stock data')
    parser.add_argument('-e', '--end_date', nargs=3, type=int,
                        help='End date for gathering stock data')

    parser.add_argument('-ev', '--everywhere', action='store_true',
                        help='Query everywhere?')

    parser.add_argument('-d', '--database', action='store_true',
                        help='use a database')
    parser.add_argument('-p', '--pickle', action='store_true',
                        help='use a dataframe pickle file')
    parser.add_argument('-j', '--json', action='store_true',
                        help='use a json file')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Set verbose to true.')

    parser.add_argument('-c', '--config_file',
                        help='Path to non-default config file')

    parser.add_argument('-kapi', '--kapi',
                        help='Use a different news api key then the default in config')
    parser.add_argument('-ra', '--rapid_api',
                        help='Use a different contextual websearch api key then the default in config')

    parser.add_argument('-pf', '--pickle_file',
                        help='use a different pickle file')

    parser.add_argument('-dp', '--database_path',
                        help='use a different database_path')

    parser.add_argument('-jf', '--json_file',
                        help='use a different json file')

    args = parser.parse_args()

    api_key, rapid, pickle_path, database_path, json_file = settings(args)

    if args.start_date is None:
        start_date = None
    else:
        start_date = datetime.datetime(*map(int, args.start_date))

    if args.end_date is None:
        end_date = datetime.datetime.now()
    else:
        end_date = datetime.datetime(*map(int, args.end_date))

    news = NewsGather(api_key_news=api_key, api_key_rapid=rapid,
                      pickle_path=pickle_path, database_path=database_path, json_path=json_file,
                      verbose=args.verbose)

    if start_date is not None and end_date is not None:
        news.search_date_range(args.query, search_everywhere=True, start_date=start_date, end_date=end_date,
                               database=args.database, json_f=args.json, pickle_f=args.pickle)
    else:
        news.search_to_output(args.query, search_everywhere=True, start_date=start_date,
                              database=args.database, json_f=args.json, pickle_f=args.pickle)


"""
def test_function():
    news = NewsGather(api_key_news=,
                      api_key_rapid=,
                      pickle_path=None, database_path='news.db', json_path=None)
    data = news.search_rapid('tacos')
    print(data)
    print(type(data))
"""

if __name__ == '__main__':
    data = ['thing that we need to do',
            'second document that we need to do']
    # test_function()
    main()

