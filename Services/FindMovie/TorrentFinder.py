#!/usr/bin/python

import sys, os
import logging
import time
import pprint
import string
import re
import requests
import bs4

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from blessings import Terminal

from Utils import HTTPHelper
from Utils import Utilities
from Utils.Similarity import Similarity
from optparse import OptionParser

class TorrentFinder:
  def __init__(self, search_engines=None ):
    self.component              = self.__class__.__name__
    self.logger                 = Utilities.GetLogger(self.component)

    self.top_items              = 5
    self.search_engines         = None
    self.torrench_submodules    = []
    self.term                   = Terminal()
    self.similarity             = Similarity()

    if search_engines is not None:
      self.search_engines       = search_engines
      for engine in search_engines:
        name = None
      #('kickasstorrent', 'KickassTorrents'), ## Is not usable
      #('skytorrents', 'SkyTorrents'),  ## Gives an error
      #('xbit', 'XBit'),  ## Gives an error
        if engine == 'limetorrents':
          name                  = 'LimeTorrents'
        elif engine == 'thepiratebay':
          name                  = 'ThePirateBay'
        elif engine == 'nyaa':
          name                  = 'Nyaa'
        elif engine == 'rarbg':
          name                  = 'RarBg'
        elif engine == 'x1337':
          name                  = 'x1337'
        elif engine == 'idope':
          name                  = 'Idope'
        engine_pair = (engine, name)
        self.torrench_submodules.append(engine_pair)

      #print "="*120
      #pprint.pprint(self.torrench_submodules)
      #print "="*120

  def Search(self, title, page_limit):
    ''''''
    ###########################################################
    #### This items were tested manually with URL found in
    #### torrent.ini. This file has to be set up manually
    #### and it is part of the Torrench infrastructure
    ###########################################################
    #torrench_submodules = [
      #('limetorrents', 'LimeTorrents'),
      #('thepiratebay', 'ThePirateBay'),
      #('nyaa', 'Nyaa'),
      #('rarbg', 'RarBg'),
      #('x1337', 'x1337'),
      #('idope', 'Idope')
    #]

    message_content = {
      'title': [], 
    }

    try:
      all_items             = pd.DataFrame(message_content)

      self.logger.debug("1) Starting torrent search")
      for module_info in self.torrench_submodules:
        ''' Preparing dynamic class loading'''
        package_name        = "torrench.modules."+module_info[0]
        class_name          = module_info[1]
        module              = __import__(package_name, fromlist=[class_name])
        class_instance      = getattr(module, class_name)

        ## Searching for torrents based in movie tittle
        self.logger.debug("2) Looking into [%s]\n"%(module_info[1]))
        proxy, empty_link, message_content = self.search_torrents(class_instance, title, page_limit)
        print "=== proxy:", proxy
        print "=== empty_link:", empty_link
        print "=== message_content:", message_content
        valid_items         = len(message_content['title'])
        self.logger.debug("3) Found [%d] invalid and [%d] valid torrents"%(empty_link, valid_items))

        ## Some content could had been found from torrent search...
        if valid_items > 0 :
          self.logger.debug("4) Filtering top [%d] items"%self.top_items)
          top_items         = self.find_top_items(proxy, message_content)

          print "=== top_items:", top_items
          print "=== top_items.size:", len(top_items)
          items_size        = top_items.shape
          self.logger.debug("5) Finding magnets and links for %s"%str(items_size))

          ## Looking for magnet and link for top ranked items
          for item_index, row in top_items.iterrows():
            index           = int(row['index'])
            magnet, link    = proxy.get_links(item_index)

            ## Setting new link and magnet
            top_items.set_value(item_index, 'link', link)
            top_items.set_value(item_index, 'magnet', magnet)

            ############################################################################################
            ##TODO: Create another task
            ##TODO: Task should look for imdb mark and compare names of results
            ## Checking for IMDB data in link
            source = HTTPHelper.ExtractSource(link)
            soup = HTTPHelper.ExtractData(source)
            imdb_mark = soup.body.findAll(text=re.compile('imdb'))
            if len(imdb_mark)>0:
              print self.term.red + self.term.on_green + str(row) + self.term.normal
              print self.term.red + self.term.on_green + "imdb_mark: "+str(imdb_mark) + self.term.normal
            ############################################################################################
            
          ## Merging top items from all search engines
          all_items         = pd.concat([all_items, top_items])
          self.logger.debug ("6) Concatenating %s items"%str(all_items.shape))

          #print all_items
        else:
          self.logger.debug ("4) No items found in [%s], continue with search..."%class_name)

      ## Finding top items of all merged items
      print "=== all_items.type:", type(all_items)
      print "=== all_items.size:", len(all_items)
      print "=== all_items.hasColumn:", 'seeders' in all_items.columns
      print all_items
      if 'seeders' in all_items.columns:
        self.logger.debug ("7) Finding top items of all merged items")
        all_items_by_size     = all_items.sort_values(by=['seeders'], ascending=[False])
        #all_items_by_size     = all_items_by_size.head(self.top_items)#.to_dict()
        all_items_by_size     = all_items_by_size.sort_values(by=['size'], ascending=[False])
        #print all_items_by_size.to_dict()
        #pprint.pprint(all_items_by_size)
        #print "-"*120
        self.logger.debug ("8) Sorted [%s] top items by seeeders and sizes"%str(all_items_by_size.shape))
        return all_items_by_size
      else:
        self.logger.debug ("7) No items found nor sorted")
        return all_items

    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

  def search_torrents(self, class_instance, title, page_limit):
      """Execution begins here."""
      empty_link = None
      proxy = None
      message_content = None
      try:
          class_name = class_instance.__dict__['__module__']
          self.logger.debug("  2.1) Obtaining proxies for [%s]"%(class_name))
          hasInstance = [n in class_name for n in ['xbit', 'rarbg']]

          self.logger.debug("  2.2) Creating search for [%s]"%(class_name))
          if True in hasInstance :
            proxy = class_instance(title)
          else:
            proxy = class_instance(title, page_limit)

          self.logger.debug("  2.3) Starting search with [%s]"%(class_name))
          if proxy.__dict__['class_name'] == 'xbit':
            proxy.search_torrent()
          elif proxy.__dict__['class_name'] == 'rarbg':
            proxy.get_token()
            time.sleep(0.75)
            proxy.search_torrent()
          elif proxy.__dict__['class_name'] == 'idope':
              i=0
          else:
            ## print "   ==== cond1:", not proxy.__dict__['class_name'] == 'nyaa'
            ## print "   ==== cond2:", 'x1337' in class_name
            if not proxy.__dict__['class_name'] == 'nyaa' or 'x1337' in class_name:
              print "=== checking proxy for:", class_name
              proxy.check_proxy()
            ## print "=== class_name.get_html", class_name
            ## print "=== proxy.proxy", proxy.proxy
            if proxy.proxy is not None:
              proxy.get_html()
              ## print "=== class_name.parse_html", class_name
              proxy.parse_html()
            else:
              self.logger.debug('Warning: Proxy not found for [%s]'%class_name)

          message_content = {
            'title': [], 
            'size': [], 
            'seeders': [], 
            'leechers': [], 
            'index': [], 
            'magnet': [], 
            'link': [],
            'engine': [],
            'score': []
          }
          empty_link = 0
          added_links = 0
          print "=== proxy.masterlist_crossite:", proxy.masterlist_crossite
          self.logger.debug("  2.4 Parsing results from [%s] list"%(class_name))
          for item in proxy.masterlist_crossite:
            seeders, leechers = item[3].split('/')
            leechers = int(leechers)
            seeders = int(seeders)

            if leechers > 0 or seeders > 0:
              found_title = item[0]
              index = item[1]

              ## Parsing size into bytes
              value, unit= item[2].split()
              value = value.replace(",","")
              if unit == 'GB' or unit == 'GiB':
                size = float(value)*1000000.0
              elif unit == 'MB' or unit == 'MiB':
                size = float(value)*1000.0

              ## Getting torrent links, this makes process slower
              #magnet, link = proxy.get_links(index)
              #print "\t found_title:\t\t",found_title
              #print '\t size:\t\t',size
              #print '\t seeders:\t',seeders
              #print '\t leechers:\t', leechers
              #print '\t magnetic:\t',magnet
              #print '\t torrent:\t',link

              p_tittle = Utilities.RemoveColor(found_title)
              message_content['title'].append(p_tittle)
              message_content['size'].append(size)
              message_content['seeders'].append(seeders)
              message_content['leechers'].append(leechers)
              message_content['index'].append(index)
              message_content['engine'].append(proxy.__dict__['class_name'])
              message_content['magnet'].append('-')
              message_content['link'].append('-')

              ## Adding score of how much it looks like the title
              print "===title:", title
              #score_tittle = p_tittle.replace(".", " ")
              print "===p_tittle:", p_tittle
              #print "===score_tittle:", score_tittle
              #score = HTTPHelper.WordByWord(title, score_tittle)
              score = self.similarity.score(title, p_tittle)
              print "===score:", score
              print "="*120
              message_content['score'].append(score)
              
              added_links+=1
              #self.logger.debug("Added item(s): %d"%added_links)
            else:
              empty_link +=1
          #lmt.show_output()
          #proxy.post_fetch()  # defined in Common.py
          #self.logger.debug("\nBye!")
          #return proxy, empty_link, message_content
      except Exception as inst:
        Utilities.ParseException(inst, logger=self.logger)
      finally:
          self.logger.debug("  2.5 Finished torrent search ")
          return proxy, empty_link, message_content

  def find_top_items(self, proxy, message_content):
    try:
      valid_items     = len(message_content['title'])
      #if valid_items < self.top_items:
        #self.logger.debug('Not enough items for sort, found [%d] items'%valid_items)
        #return message_content

      sLength = len(message_content['title'])
      self.logger.debug('Filtering %d items into data frames'%sLength)
      df              = pd.DataFrame(message_content)

      headers         = list(df.columns.values)
      mean            = df["size"].mean()
      max             = df["size"].max()
      quantile_upper  = df["size"].quantile(0.75)

      self.logger.debug('Mean of items is\t\t\t[%.2f]'%mean)
      self.logger.debug('Max of items is\t\t\t[%.2f]'%max)
      self.logger.debug('Upper quantile of items is\t[%.2f]'%quantile_upper)
      videos_limited  = df.loc[df['size'] > quantile_upper]
      self.logger.debug('Getting top %d items'%self.top_items)
      videos_by_size  = videos_limited.sort_values(by=['size', 'seeders'], ascending=[False, False])
      top_items       = videos_by_size.head(self.top_items)

      return top_items
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

def main(options):
  finder = TorrentFinder()
  items = finder.Search(options.title, options.page_limit)

LOG_NAME = 'TorrentFinder'
if __name__ == '__main__':
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  
  myFormat = '%(asctime)s|%(name)30s|%(message)s'
  logging.basicConfig(format=myFormat, level=logging.DEBUG)
  logger        = Utilities.GetLogger(LOG_NAME, useFile=False)
  logger.debug('Logger created.')
  
  usage = "usage: %prog --title='string' --page_limit=1"
  parser = OptionParser(usage=usage)
  parser.add_option('--title',
                      type="string",
                      action='store',
                      default=None,
                      help='Input torrent search title')

  parser.add_option("--page_limit", 
                      type="int", 
                      default=1,
                      help='Input page search limit')
    
  (options, args) = parser.parse_args()
  
  if options.title is None:
    parser.error("Missing required option: --title='string'")
    sys.exit()
    
  main(options)