import os
import scrapy
import pandas as pd
from scrapy import signals


class FreesoundSpider(scrapy.Spider):
    name = 'freesound'

    base_url = 'https://freesound.org{}'
    search_url = 'https://freesound.org/search/?q={}'
    login_url = 'https://freesound.org/home/login/'

    _path = '../data/{}/{}.{}'
    # _username = os.environ['USERNAME']
    # _password = os.environ['PASSWORD']
    # _queries = os.environ['QUERIES']
    # _limit = int(os.environ['LIMIT'])

    _counter = 0
    _metadata = dict()
    _keywords = []

    _username = '' # freesound username
    _password = '' # freesound password
    _queries = '' # keywords to search, comma seperated
    _limit = 10 # maximum number of samples to download for each keyword search

    _columns = ['id', 'keyword', 'tags', 'type', 'duration', 'filesize', 'samplerate', 'bitdepth', 'channels']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(FreesoundSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider._setup, signal=signals.spider_opened)
        crawler.signals.connect(spider._export_csv, signal=signals.spider_closed)
        return spider

    def _export_csv(self):
        for keyword in self._keywords:
            dataframe = pd.DataFrame.from_dict(self._metadata[keyword], orient='index', columns=self._columns)
            dataframe.to_csv(path_or_buf=f'../data/{keyword}/metadata.csv')

    def _setup(self):
        self._keywords = self._queries.split(',')
        for keyword in self._keywords:
            self._metadata[keyword] = dict()
            path = f'../data/{keyword}'
            if not os.path.exists(path):
                os.makedirs(path)

    def start_requests(self):
        yield scrapy.Request(url=self.login_url, callback=self._login)

    def _login(self, response):
        token = response.xpath('//*[@name="csrfmiddlewaretoken"]/@value').extract_first()
        data = {
            'csrftoken' : token,
            'username' : self._username,
            'password' : self._password,
        }

        yield scrapy.FormRequest.from_response(response=response, formdata=data, formxpath='//*[@method="post"]', callback=self._search)

    def _search(self, response):
        for keyword in self._keywords:
            data = {'keyword': keyword, 'counter': 0}
            request_url = self.search_url.format(keyword)
            yield scrapy.Request(url=request_url, callback=self._search_cb, cb_kwargs=data)

    def _search_cb(self, response, keyword, counter):
        samples = response.css('div.sample_player_small')
        cur_counter = counter
        for sample in samples:
            if cur_counter > self._limit:
                break
            
            sample_id = sample.attrib['id']
            tags = self._extract_tags(sample)
            link = sample.css('div.sound_title div.sound_filename a.title').attrib['href']
            request_url = self.base_url.format(link)

            base = [sample_id, keyword, ','.join(tags)]
            data = {'keyword': keyword, 'counter': cur_counter, 'base': base}

            cur_counter += 1
            yield scrapy.Request(request_url, callback=self._scrape, cb_kwargs=data)

        if cur_counter < self._limit:
            next_page = response.css('div.search_paginator ul.pagination li.next-page a').attrib['href']
            if next_page is not None:
                yield response.follow(next_page, self._search_cb, cb_kwargs={'keyword': keyword, 'counter': cur_counter})

    def _scrape(self, response, keyword, counter, base):
        data = {'keyword': keyword}
        link = response.xpath('//*[@id="download"]').css('a').attrib['href']
        request_url = self.base_url.format(link)
        info = response.xpath('//*[@id="sound_information_box"]').css('dd::text').getall()
        self._metadata[keyword][counter] = base + info

        yield scrapy.Request(request_url, callback=self._download, cb_kwargs=data)

    def _download(self, response, keyword):
        url = response.url
        parts = url.split('/')

        ext = parts[-1].split('.')[-1]
        sound_id = parts[-3]
        path = self._path.format(keyword, sound_id, ext)
        with open(path, 'wb') as f:
            f.write(response.body)

    def _extract_tags(self, sample):
        tag_list = sample.css('div.sound_tags ul.tags')
        return tag_list.css('li a::text').getall()