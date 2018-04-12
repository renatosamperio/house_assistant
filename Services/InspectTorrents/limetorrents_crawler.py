"""The Pirate Bay Module."""

import logging
import sys
import pprint
import datetime
import threading
import Queue
import requests
import time
import random
import bs4
import copy
import re

from bs4 import BeautifulSoup
from collections import Counter
from torrench.utilities.Config import Config
from Utils import Utilities
from Utils import MongoAccess

class LimeTorrentsCrawler(Config):
    """
    LimeTorrents class.

    This class fetches torrents from LimeTorrents proxy,
    and diplays results in tabular form.

    All activities are logged and stored in a log file.
    In case of errors/unexpected output, refer logs.
    """

    def __init__(self, **kwargs):
        """Initialisations."""
        try:
            Config.__init__(self)
            ## Initialising class variables
            self.class_name = self.__class__.__name__.lower()
            self.logger     = Utilities.GetLogger(self.class_name)
        
            self.title      = None
            self.search_type= None
            self.with_magnet= None
            self.pages      = None
            self.collection = None
            self.database   = None
            self.db_handler = None
            self.with_db    = False 
            
            for key, value in kwargs.iteritems():
                if "title" == key:
                    self.title = value
                elif "page_limit" == key:
                    self.pages = value
                elif "search_type" == key:
                    self.search_type = value
                elif "with_magnet" == key:
                    self.with_magnet = value
                elif "collection" == key:
                    self.collection = value
                elif "database" == key:
                    self.database = value
                elif "with_db" == key:
                    self.with_db = value
                
            self.proxies    = self.get_proxies('limetorrents')
            self.proxy      = None
            self.index      = 0
            self.page       = 0
            self.total_fetch_time = 0
            self.mylist     = []
            self.masterlist = []
            self.mylist_crossite = []
            self.masterlist_crossite = []
            self.mapper     = []
            ## self.soup_dict  = {}
            self.soup_dict  = Queue.Queue()
            self.missed     = Queue.Queue()
            self.soup       = None
            self.headers    = ['NAME', 'INDEX', 'SIZE', 'SE/LE', 'UPLOADED']
            ## self.key1       = 'magnet:?xt=urn:btih:'
            ## self.key2       = '&'
            self.key1       = 'http://itorrents.org/torrent/'
            self.key2       = '.torrent?'
            
            self.supermasterlist = []
            
            self.lock       = threading.Lock()
            self.crawler_finished = False
            
            
            if self.with_db:
                self.logger.debug("  + Generating database [%s] in [%s] collections"% 
                                  (self.database, self.collection))
                self.db_handler = MongoAccess(debug=True)
                self.db_handler.connect(self.database, self.collection)
        except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)

    def http_request(self, url):
        """
        http_request method.

        This method does not calculate time.
        Only fetches URL and prepares self.soup
        """
        returned_code = None
        try:
            try:
                self.raw        = requests.get(url, timeout=30)
                returned_code   = self.raw.status_code
                self.logger.debug("  +   Returned status code: %d for url %s" % (returned_code, url))
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
                self.logger.error(e)
                self.logger.exception("Stacktrace...")
                return None, returned_code
            self.raw = self.raw.content
            self.soup = BeautifulSoup(self.raw, 'lxml')
            return self.soup
        except KeyboardInterrupt as e:
            print("Aborted!")
            self.logger.exception(e)
            ##sys.exit(2)
        except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)
    
    def http_request_timed(self, url):
        """
        http_request_time method.

        Used to fetch 'url' page and prepare soup.
        It also gives the time taken to fetch url.
        """
        returned_code = None
        try:
            try:
                headers = {"user-agent": "Mozilla/5.0 (X11; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0"}
                self.start_time     = time.time()
                self.raw            = requests.get(url, timeout=15, headers=headers)
                returned_code       = self.raw.status_code
                self.page_fetch_time= time.time() - self.start_time
                self.logger.debug("  +   Returned status code: %d for URL %s" % (returned_code, url))
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
                self.logger.error(e)
                self.logger.exception("Stacktrace...")
                return -1, self.page_fetch_time, returned_code
            except KeyboardInterrupt as e:
                self.logger.exception(e)
                print("Aborted!")
            self.raw                = self.raw.content
            self.soup               = BeautifulSoup(self.raw, 'lxml')
            return self.soup, self.page_fetch_time, returned_code
        except KeyboardInterrupt as e:
            print("Aborted!")
            ##sys.exit(2)
            self.logger.exception(e)

    def get_magnet_ext(self, link):
        """
        Module to get magnetic link of torrent.

        For 1337x, limetorrents modules.
        Magnetic link is fetched from torrent's info page.
        """
        self.logger.debug("Fetching magnetic link with [%s]"%self.class_name)
        soup = self.http_request(link)
        magnet = soup.findAll('div', class_='dltorrent')[2].a['href']
        return magnet

    def get_magnet_download(self, link):
        """
        Module to get magnetic link of torrent.

        For 1337x, limetorrents modules.
        Magnetic link is fetched from torrent's info page.
        """
        self.logger.debug("Fetching magnetic link with [%s]"%self.class_name)
        soup = self.http_request(link)
        magnet = soup.findAll('div', class_='dltorrent')[2].a['href']
        download_link = soup.findAll('div', class_='dltorrent')[0].a['href']
        return magnet, download_link
    
    def check_proxy(self):
        """
        To check proxy availability.

        Proxy is checked in two steps:
        1. To see if proxy 'website' is available.
        2. A test is carried out with a sample string 'hello'.
        If results are found, test is passed, else test failed!

        This class inherits Config class. Config class inherits
        Common class. The Config class provides proxies list fetched
        from config file. The Common class consists of commonly used
        methods.

        In case of failiur, next proxy is tested with same procedure.
        This continues until working proxy is found.
        If no proxy is found, program exits.
        """
        count = 0
        result = False
        for proxy in self.proxies:
            self.logger.debug("Trying %s" % (self.colorify("yellow", proxy)))
            self.logger.debug("Trying proxy: %s" % (proxy))
            self.soup = self.http_request(proxy)
            try:
                ## print "=== What is self.soup?",self.soup
                if self.soup is not None and self.soup == -1 or 'limetorrents' not in self.soup.find('div', id='logo').a['title'].lower():
                    self.logger.debug("Bad proxy!")
                    count += 1
                    if count == len(self.proxies):
                        self.logger.debug("No more proxies found! Terminating")
                        sys.exit(2)
                    else:
                        continue
                else:
                    self.logger.debug("Proxy available. Performing test...")
                    url = proxy+"/search/all/hello/seeds/1/"
                    self.logger.debug("Carrying out test for string 'hello'")
                    self.soup = self.http_request(url)
                    test = self.soup.find('table', class_='table2')
                    if test is not None:
                        self.proxy = proxy
                        self.logger.debug("Pass!")
                        self.logger.debug("Test passed!")
                        result = True
                        break
                    else:
                        self.logger.debug("Error: Test failed! Possibly site not reachable. See logs.")
            except (AttributeError, Exception) as e:
                self.logger.exception(e)
                pass
            finally:
                return result
    
    def get_url(self, search_type, title):
        """
        Preparing type of query:
           1) Search of movie titles
           2) Browse existing movies
        """
        try:
            self.logger.debug("  +   Defining a search type: "+search_type)
            if search_type == 'browse-movies':
                search = '/browse-torrents/Movies/seeds/'
            elif search_type == 'browse-shows':
                search = '/browse-torrents/TV-shows/seeds/'
            else:
                search = "/search/{}/{}/seeds/".format(search_type, title)
            self.logger.debug("  +   Search link: "+search)
            return search
                
        except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)
            
    def get_html(self, cond):
        """
        To get HTML page.

        Once proxy is found, the HTML page for
        corresponding search string is fetched.
        Also, the time taken to fetch that page is returned.
        Uses http_request_time() from Common.py module.
        """
        try:
            self.crawler_finished = False
            pages_queue = Queue.Queue()
            for page in range(self.pages):
                pages_queue.put(page)
            page_counter = 0
            
            while not pages_queue.empty():
                page = pages_queue.get()
                self.logger.debug("  1.1) Fetching page: %d/%s" % (page+1, self.pages))
                
                search = self.get_url(self.search_type, self.title)
                search = search + '{}/'.format(page+1)
                search_url = self.proxy + search
                self.logger.debug("  1.2) Looking into:"+search_url)
                self.soup, time_, returned_code = self.http_request_timed(search_url)
                
                if str(type(self.soup)) == 'bs4.BeautifulSoup':
                    self.logger.debug("Error: Invalid HTML search type [%s]"%str(type(self.soup)))
                    waiting_time = 30 # random.randint(1, 3)
                    self.logger.debug("       Waiting for [%d]s:"%waiting_time)
                    time.sleep(waiting_time)
                    pages_queue.put(page)
                    continue
                
                if returned_code != 200:
                    self.logger.debug("Error: Returned code [%s] captured page [%s]"% (str(returned_code), search_url))
                    self.missed.put(search_url)
                    waiting_time = 30 # random.randint(1, 3)
                    self.logger.debug("       Waiting for [%d]s:"%waiting_time)
                    time.sleep(waiting_time)
                    pages_queue.put(page)
                    continue
                    
                self.logger.debug("       Captured page %d/%d in %.2f sec" % (page+1, self.pages, time_))
                self.total_fetch_time += time_
                    
                with self.lock:
                    self.logger.debug("  1.3) Placing page [%d] in queue"%page)
                    self.soup_dict.put({page : self.soup})
                    
                with cond:
                    self.logger.debug("  1.4) Notifying html parsing with error code [%s]"%str(returned_code))
                    if returned_code == 200:
                        cond.notifyAll()
                        page_counter += 1
            
            self.logger.debug("  + Got [%d] pages and finished with [%d] and missing [%d0"%
                              (page_counter, pages_queue.qsize(), self.missed.qsize()))
            self.crawler_finished = True
        except Exception as e:
            self.logger.exception(e)
            self.logger.debug("Error message: %s" %(e))
            ## sys.exit(2)
            self.logger.debug("Something went wrong! See logs for details. Exiting!")

    def parse_html(self, cond):
        """
        Parse HTML to get required results.

        Results are fetched in masterlist list.
        Also, a mapper[] is used to map 'index'
        with torrent name, link and magnetic link
        """
        try:
            pages_parsed = 0
            with cond:
                while not self.crawler_finished:
                    self.logger.debug("  2.1) Waiting for HTML crawler notification...")
                    cond.wait()
                    
                    ## Acquiring lock and collecting soup
                    with self.lock:
                        if self.soup_dict.empty():
                            self.logger.debug("  - Got notified but queue is empty")
                            continue
                        soupDict       = self.soup_dict.get()

                    soupKeys            = soupDict.keys()
                    
                    if len(soupKeys) <0:
                        elf.logger.debug("  - No keys in queue, skiping URL parse")
                        continue

                    ## Once soup has been collected, starting to parse
                    page                = soupKeys[0]
                    self.logger.debug("  2.2) Getting page [%d]"%(page+1))
                    self.soup           = soupDict[page]
                    
                    print "===> self.soup.type: ", type(self.soup)
                    print "===> self.soup.type: ", str(type(self.soup))
                    print "===> self.soup.type: ", not isinstance(self.soup, bs4.BeautifulSoup)
                    ## sys.exit(0)
                    
                    ## Verifying soup is valid
                    if not isinstance(self.soup, bs4.BeautifulSoup):
                        self.logger.debug("Error:  Invalid HTML search item")
                        
                    ## Looking for table components
                    content             = self.soup.find('table', class_='table2')
                    self.logger.debug("  2.3) Parsing page next page")
                    
                    if content is None:
                        self.logger.debug("Error: Invalid parsed content")
                        return
                    results             = content.findAll('tr')
                    for result in results[1:]:
                        data            = result.findAll('td')
                        # try block is limetorrents-specific. Means only limetorrents requires this.
                        name            = data[0].findAll('a')[1].string
                        link            = data[0].findAll('a')[1]['href']
                        link            = self.proxy+link
                        date            = data[1].string
                        date            = date.split('-')[0]
                        size            = data[2].string
                        seeds           = data[3].string.replace(',', '')
                        leeches         = data[4].string.replace(',', '')
                        seeds_color     = self.colorify("green", seeds)
                        leeches_color   = self.colorify("red", leeches)
                        
                        self.index      += 1
                        self.mapper.insert(self.index, (name, link, self.class_name))
                        self.mylist     = [name, "--" +
                                        str(self.index) + "--", size, seeds_color+'/'+
                                        leeches_color, date]
                        self.masterlist.append(self.mylist)
                        self.mylist_crossite = [name, self.index, size, seeds+'/'+leeches, date]
                        self.masterlist_crossite.append(self.mylist_crossite)
                        
                        element         = {
                            'name': name,
                            'date': date,
                            'size': size,
                            'seeds': seeds,
                            'leeches': leeches,
                            'link': link,
                            'page': page+1
                        }
    
                        ## Getting hash
                        torrent_file    = data[0].findAll('a')[0]['href']
                        start_index     = torrent_file.find(self.key1)+len(self.key1)
                        end_index       = torrent_file.find(self.key2)
                        hash            = torrent_file[start_index:end_index]
                        element.update({'hash': hash})
                        element.update({'torrent_file': torrent_file})
                        
                        ## Getting available images
                        images          = data[0].findAll('img')
                        qualifiers      = []
                        if len(images) > 0:
                            for image in images:
                                qualifiers.append(image['title'])
                            element.update({'qualifiers': qualifiers})
    
                        ## Getting magnet link
                        if self.with_magnet:
                            self.logger.debug("  + Getting magnet link")
                            magnetic_link = self.get_magnet_ext(link)
                            element.update({'magnetic_link': magnetic_link})
                            
                        ## self.supermasterlist.append(element)
                        
                        ## Inserting in database
                        if self.with_db:
                            self.logger.debug("  2.4) Appending in database [%s]"%element['hash'])
                            result = self.Update_TimeSeries_Day(element, 
                                                        'hash',         ## This is the item key, it should be unique!
                                                        ['seeds', 'leeches'],  ## These items are defined in a time series
                                                        ) 
                            if not result:
                                self.logger.debug("Error: DB insertion failed")
                            
                        pages_parsed += 1
                        
                    if self.with_db:
                        self.logger.debug("  - Total records in DB: [%d]"%self.db_handler.Size())
                    else:
                        self.logger.debug("  - Total parsed pages: [%d]"%pages_parsed)
                        
                    self.logger.debug("  2.5) Finished parsing HTML")

            self.logger.debug('Found [%d] items'%pages_parsed)

        except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)

    def complete_db(self, cond):
        '''
        '''
        try:
            with cond:
                items           = ['leeches', 'seeds']
                datetime_now    = datetime.datetime.utcnow()
                
                month           = str(datetime_now.month)
                day             = str(datetime_now.day)
                page_counter    = 0
                total_fetch_time= 0.0
                    
                self.logger.debug('    3.1) Searching non updated items')
                leeches_key     = 'leeches.value.'+month+"."+day
                seeds_key       = 'seeds.value.'+month+"."+day
                condition       = { '$or': [{ leeches_key : {'$exists': False}}, { seeds_key : {'$exists': False}}]}
                posts           = self.db_handler.Find(condition)
                postsSize       = posts.count()
                self.logger.debug('         Found [%d] items'%postsSize)
                
                self.logger.debug('    3.2) Creating queue of posts')
                posts_queue     = Queue.Queue()
                for post in posts:
                    posts_queue.put(post)
                
                while not posts_queue.empty():
                    post = posts_queue.get()
                    ##pprint.pprint(post)
                    search_url  = post['link']
                    self.logger.debug("      3.2.1) Looking into ["+search_url+']')
                    self.soup, time_, returned_code = self.http_request_timed(search_url)
            
                    if str(type(self.soup)) == 'bs4.BeautifulSoup':
                        self.logger.debug("Error: Invalid HTML search type [%s]"%str(type(self.soup)))
                        waiting_time = 30 # random.randint(1, 3)
                        self.logger.debug("       Waiting for [%d]s:"%waiting_time)
                        time.sleep(waiting_time)
                        posts_queue.put(post)
                        continue
                    
                    if returned_code != 200:
                        self.logger.debug("Error: Returned code [%s] captured page [%s]"% (str(returned_code), search_url))
                        self.missed.put(search_url)
                        waiting_time = 30 # random.randint(1, 3)
                        self.logger.debug("       Waiting for [%d]s:"%waiting_time)
                        time.sleep(waiting_time)
                        posts_queue.put(post)
                        continue
                        
                    page_counter += 1
                    self.logger.debug("       Captured page %d/%d in %.2f sec" % (page_counter+1, postsSize, time_))
                    total_fetch_time += time_
                    
                    ## Looking for table components
                    self.logger.debug("      3.2.2) Searching for leechers, seeders and hash ID")
                    search_table    = self.soup.find('table')
                    lines           = search_table.findAll('td')
                    hash            = lines[2].string.strip()
                    
                    seeders_num     = None
                    seeders_data    = self.soup.body.findAll(text=re.compile('^Seeders'))
                    if len(seeders_data)>1:
                        seeders_num = seeders_data[0].split(':')[1].strip()
                        
                    leechers_num    = None
                    leechers_data   = self.soup.body.findAll(text=re.compile('^Leechers'))
                    if len(leechers_data)>1:
                        leechers_num= leechers_data[0].split(':')[1].strip()
                    
                    if post['hash'] == hash:
                    	self.logger.debug("      3.2.3) Updating item [%s] with [%s] leeches " % (hash, leechers_num))
                        condition   = { 'hash' : hash }
                        leeches_item= {leeches_key: leechers_num }
                        result      = self.db_handler.Update(condition, leeches_item)

                        self.logger.debug("      3.2.4) Updating item [%s] with [%s] seeds " % (hash, seeders_num))
                        condition   = { 'hash' : hash }
                        seeds_item  = {seeds_key: seeders_num }
                        result      = self.db_handler.Update(condition, seeds_item)

        except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)

    def Run(self):
        '''
        Looks for all browse movies and parses HTML in two threads. 
        Then, updates any non feature torrent in DB 
        '''
        try:
            self.logger.debug("Obtaining proxies...")
            proxy_ok = self.check_proxy()
            if proxy_ok:
                self.logger.debug("Preparing threads")
                condition = threading.Condition()
                html_crawler = threading.Thread(
                    name='html_crawler', 
                    target=self.get_html, 
                    args=(condition,))
                html_parser  = threading.Thread(
                    name='html_parser',  
                    target=self.parse_html, 
                    args=(condition,))
                
                html_crawler.start()
                html_parser.start()
                
                html_crawler.join()
                html_parser.join()
                
                crawler_db  = threading.Thread(
                    name='crawler_db',  
                    target=self.complete_db, 
                    args=(condition,))
                crawler_db.start()
                crawler_db.join()
                
                ## pprint.pprint(lmt.supermasterlist)
                
        except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)

    def UpdateBestSeriesValue(self, db_post, web_element, item_index, items_id):
        '''
        Comparator for time series update to check if for today already exists a value. 
        Could possible be if torrent hash is repeated in the website. 
        
        returns True if value has been updated. Otherwise, DB update failed
        '''
        result      = True
        try:
            postKeys = db_post.keys()
            for key in items_id:
                if key in postKeys:
                    datetime_now        = datetime.datetime.utcnow()
                    month               = str(datetime_now.month)
                    month_exists        = month in db_post[key]['value'].keys()
                    
                    ## Checking if month exists
                    if not month_exists:
                        self.logger.debug("           Adding month [%s] to [%s]"%(month, key))
                        db_post[key]['value'].update({month : {}})
                        
                    ## Checking if day exists
                    day                 = str(datetime_now.day)
                    day_exist           = day in db_post[key]['value'][month].keys()
                     
                    ## If day already exists check if it is better the one given 
                    ## right now by the webiste
                    
                    condition           = { item_index : web_element[item_index] }
                    set_key             = key+".value."+str(datetime_now.month)+"."+str(datetime_now.day)
                    subs_item_id        = {set_key: web_element[key] }
                    if day_exist:
                        ## If is value found in the website is bigger, use this one
                        ## otherwise let the one existing in the database
                        todays_db       = db_post[key]['value'][month][day]
                        todays_website  = web_element[key]
                        isTodayBetter   = todays_db < todays_website
                        
                        ## TODO: We should know the page of both items
                        #print "=== [",datetime_now.month, "/", datetime_now.day,"], todays_db >= todays_website:", todays_db, '>=', todays_website
                        isTodayWorse    = todays_db >= todays_website
                        if isTodayWorse:
                            self.logger.debug("           Existing value for [%s] similar or better", key)
                        
                        # Updating condition and substitute values
                        elif isTodayBetter:
                            ## result = True
                            result      = self.db_handler.Update(condition, subs_item_id)
                            self.logger.debug("           Updated [%s] series item with hash [%s] in collection [%s]"% 
                                      (key, web_element[item_index], self.db_handler.coll_name))
                        
                    ## if day is missing, add it!
                    else:
                        ## result = True
                        result          = self.db_handler.Update(condition, subs_item_id)
                        self.logger.debug("           Added [%s] series item for [%s/%s] with hash [%s] in collection [%s]"% 
                                  (key, str(datetime_now.month), str(datetime_now.day), web_element[item_index], self.db_handler.coll_name))
                        
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return result

    def AddMissingKeys(self, db_post, web_element):
        '''
        Adds missing keys in existing DB records.
        '''
        result                  = True
        try: 
            if isinstance(db_post,type(None)):
                self.logger.debug("Invalid input DB post for printing")

            elementKeys         = web_element.keys()
            postKeys            = db_post.keys()
            postKeysCounter     = Counter(postKeys)
            elementKeysCounter  = Counter(elementKeys)
            extra_in_db         = (postKeysCounter - elementKeysCounter).keys()
            missed_in_db        = (elementKeysCounter - postKeysCounter).keys()
           
            for key in extra_in_db:
                if key != '_id':
                    self.logger.debug('  -     TODO: Remove item [%s] from DB', key)
            
            if len(missed_in_db) > 0:
                for key in missed_in_db:
                    self.logger.debug('  -     Updated item [%s] from DB', key)
                    result     = self.Update(
                                    condition={"_id": db_post["_id"]}, 
                                    substitute={key: web_element[key]}, 
                                    upsertValue=False)
                    self.logger.debug("  -       Added key [%s] in item [%s] of collection [%s]"% 
                                  (key, item[item_index], self.db_handler.coll_name))
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return result
    
    def Update_TimeSeries_Day(self, item, item_index, items_id):
        '''
        Generate a time series model 
        https://www.mongodb.com/blog/post/schema-design-for-time-series-data-in-mongodb
        '''
        result = False
        try:
            self.logger.debug("")
            ## Check if given item already exists, otherwise
            ## insert new time series model for each item ID
            keyItem                 = item[item_index]
            condition               = {item_index: keyItem}
            posts                   = self.db_handler.Find(condition)
            datetime_now            = datetime.datetime.utcnow()
            postsSize               = posts.count()
            
            ## We can receive more than one time series item
            ## to update per call in the same item
            #TODO: Do a more efficient update/insert for bulk items
            if postsSize < 1:
                ## Prepare time series model for time series
                def get_day_timeseries_model(value, datetime_now):
                    return { 
                        "timestamp_day": datetime_now.year,
                        "value": {
                            str(datetime_now.month): {
                                str(datetime_now.day) : value
                            }
                        }
                    };
                for key_item in items_id:
                    item[key_item]   = get_day_timeseries_model(item[key_item], datetime_now)
                ## Inserting time series model
                post_id             = self.db_handler.Insert(item)
                self.logger.debug("  -     Inserted time series item with hash [%s] in collection [%s]"% 
                                  (keyItem, self.db_handler.coll_name))
                result = post_id is not None
            else:
                if postsSize>1:
                    self.logger.debug('   Warning found [%d] items for [%s]'
                                      %(postsSize, keyItem))
                for post in posts:  ## it should be only 1 post!
                    ## 1) Check if there are missing or extra keys
                    updated_missing = self.AddMissingKeys(copy.deepcopy(post), item)
                    if updated_missing:
                        self.logger.debug('    2.4.1) Added item  [%s] into DB ', keyItem)
                    else:
                        self.logger.debug('    2.4.2) DB Updated failed or no added key in item [%s]', keyItem)
                    
                    ## 2) Check if items for HASH already exists
                    ts_updated      = self.UpdateBestSeriesValue(post, 
                                                                 item, 
                                                                 item_index, 
                                                                 items_id)
                    if ts_updated:
                        self.logger.debug('    2.4.3) Time series updated for [%s]', keyItem)
                    else:
                        self.logger.debug('    2.4.4) DB Updated failed or time series not updated for [%s]', keyItem)
                    result              = updated_missing and ts_updated
                    
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return result

def main(**args):
    """Execution begins here."""
    try:
        logger      = Utilities.GetLogger("LimeTorrentsCrawler", useFile=False)
        myFormat    = '%(asctime)s|%(name)30s|%(message)s'
        logging.basicConfig(format=myFormat, level=logging.DEBUG)

        logger.debug("[LimeTorrents]")        
        lmt = LimeTorrentsCrawler(**args)
        lmt.run()
        
        logger.debug("Bye!")
    except KeyboardInterrupt:
        lmt.logger.debug("Keyboard interupt! Exiting!")
        logger.debug("Aborted!")

def cross_site(title, page_limit):
    lmt = LimeTorrentsCrawler(title, page_limit)
    return lmt

if __name__ == "__main__":
    print("It's a module!")
