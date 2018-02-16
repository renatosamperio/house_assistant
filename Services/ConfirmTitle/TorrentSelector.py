#!/usr/bin/env python
# -*- coding: latin-1 -*-
import json
import sys, os
import pprint
import logging
import re

import pandas as pd

from optparse import OptionParser
from Utils import Utilities

class TorrentSelector(object):

  def __init__(self, **kwargs):
    try:
      # Initialising class variables
      self.component            = self.__class__.__name__
      self.logger               = Utilities.GetLogger(self.component)
      self.logger.debug("  +   Creating torrent selector")
      
      ## Adding local variables
      self.title_data           = None

    except Exception as inst:
      Utilities.ParseException(inst, logger=logger)

  def sort_found_items(self, found_items):
      ''' Looks for best magnet based in title scoring'''
      best_items            = None
      try:
          ## pprint.pprint(found_items)
          ## print "="*120
          found_items_df    = pd.DataFrame.from_dict(found_items) #title_data['found_items']
          found_items_sorted= found_items_df.sort_values(by=['score'], ascending=[False])
          ## print "=== found_items_sorted:", found_items_sorted.shape
          ## pprint.pprint(found_items_sorted)
          best_items        = found_items_sorted
      except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)
      finally:
          return best_items
      
  def sort_top_items(self, top_items):
      ''' Looks for best magnet based in the averages size. Then in number of seeders.'''
      best_items            = None
      try:
          ## pprint.pprint(top_items)
          ## print "-"*120
          top_items_df      = pd.DataFrame.from_dict(top_items)
          ## pprint.pprint(top_items_df)
          mean              = top_items_df["size"].mean()
          ## quantile_upper    = top_items_df["size"].quantile(0.75)
          ## print "=== mean:", mean
          ## print "=== quantile_upper:", quantile_upper
          
          top_items_size    = top_items_df.sort_values(by=['size'], ascending=[False])
          ## pprint.pprint(top_items_size)
          ## print "="*120
          
          top_items_filter  = top_items_size.loc[top_items_size['size'] > mean]
          ## pprint.pprint(top_items_filter)
          ## print "-"*120
          top_items_seeders = top_items_filter.sort_values(by=['seeders'], ascending=[False])
          
          #TODO: If top seeders are zero, use leechers
          ## pprint.pprint(top_items_seeders)
          
          best_items        = top_items_seeders
      except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)
      finally:
          return best_items

  def existTitle(self, search_title, items_df):
      ''' Searches if title exists in column'''
      try:
          self.logger.debug("       Looking for title: [%s]"%search_title)
          items_title       = items_df['title'].str.extract('('+search_title+')', expand=True)
          items_title       = items_title.dropna(axis=0, how='any')
          indexes           = items_title.index.values
          items_title       = items_df.loc[indexes]
          
          if items_title.shape[0]<1:
              reg           = self.make_reg(search_title)
              items_title   = items_df[items_df.title.str.match(reg)]
              self.logger.debug("       Looking for with regex: [%s]"%reg)
          ## print "=== indexes:",  indexes
          
          ## reg = '\Harry[\s\.]\Potter'
          ## pattern = re.compile(reg, flags=re.IGNORECASE)
          ## items_df['title'].str.contains(reg)
          
          return items_title
      except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)

  def make_reg(self, title):
      stripped_title    = title.strip().split()
      regex_list        = ['\\'+s for s in stripped_title]
      regex_word        = '[\\s\\.]'.join(regex_list)
      return regex_word
  
  def reduce_item(self, top_item):
      '''Converts data frame dictionary with relevant items and format'''
      relevant_keys         = [ "engine", 
                               "imdb_id", 
                               "leechers", 
                               "link", 
                               "magnet", 
                               "score", 
                               "seeders", 
                               "size", 
                               "title"]
      reduced_items         = {}
      try:
          item_keys         = top_item.keys()
          print "=== relevant_keys:\t", relevant_keys
          print "=== item_keys:\t", item_keys
          for relevant_key in item_keys:
              print "=== relevant_key:\t", relevant_key
              ## Get only relevant keys
              print "=== is_in:", relevant_key in relevant_keys
              if relevant_key in relevant_keys:
                  ## Get data comes with an indexed item
                  print "=== ok_relevant_key:\t", relevant_key
                  relevant_item = top_item[relevant_key]
                  print "=== relevant_item:\t", relevant_item
                  item_keys = top_item[relevant_key].keys()
                  print "=== item_keys:\t", item_keys
                  for sub_item_key in item_keys:
                      print "=== sub_item_key:\t", sub_item_key
                      print "=== item:\t", relevant_item[sub_item_key]
                      reduced_items.update({ relevant_key : relevant_item[sub_item_key] })
                      
      except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)
      finally:
          return reduced_items
          
  def findTitle(self, title_data):
      ''' Searches for best torrent according to given title'''
      try:
          self.logger.debug("  1) Looking into best options of [found_items]")
          imdb_sorted       = self.sort_found_items(title_data['found_items'])
          ## pprint.pprint(imdb_sorted)
          ## print "-"*120
          debug = True
          
          if debug: pprint.pprint(title_data.keys())
          search_title      = title_data['search_title']
          self.logger.debug("  2.1) Check if title is included in [found_items]")
          if debug: print "=== imdb_sorted ==="
          if debug: pprint.pprint(imdb_sorted)
          if debug: print "-"*120
          imdb_filter= self.existTitle(search_title, imdb_sorted)
          if imdb_filter.shape[0]<1:
              self.logger.debug("       Title [%s] not found, using sorted items"%search_title)
              imdb_filter = imdb_sorted
              
          ## Choose the first one
          if debug: print "=== imdb_filter ==="
          if debug: pprint.pprint(imdb_filter)
          if debug: print "-"*120
          imdb_top   = imdb_filter.head(1)
          if debug: print "=== imdb_top ==="
          if debug: pprint.pprint(imdb_top)
          if debug: print "-"*120
          ##imdb_id           = imdbs_top['imdb_id']
          imdb_id           = imdb_top['imdb_id'].iloc[0]
          ## pprint.pprint(imdb_top)
          self.logger.debug("  3) Got IMDB of [%s]: %s"%(search_title, imdb_id))
          
          self.logger.debug("  4) Looking into best options of [top_items]")
          if debug: pprint.pprint(title_data.keys())
          
          ## Getting top items above average size and then in number of seeders
          torrent_sorted  = self.sort_top_items(title_data['top_items'])
          if debug: pprint.pprint(torrent_sorted)
          if debug: print "-"*120
          
          self.logger.debug("  5) Check if title is included in [top_items]")
          torrent_filtered= self.existTitle(search_title, torrent_sorted)
          torrent_filtered['imdb_id']= imdb_id
          if debug: pprint.pprint(torrent_filtered)
          if debug: print "-"*120
          ## Choose the first one
          torrent_top     = torrent_filtered.head(1)
          if debug: pprint.pprint(torrent_top)
          return torrent_top
      except Exception as inst:
          Utilities.ParseException(inst, logger=self.logger)

## Standalone main method
LOG_NAME = 'TorrentSelector'
def sort_only_top_items(top_items):
  try:
    logger      = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    for top_item in top_items:
        file_name = top_item.split('/')[-1]
        logger.debug('-> Processing [%s]'%file_name)
        logger      = Utilities.GetLogger(LOG_NAME, useFile=False)
        logger.debug('Loading data for [top_items]')
        message     = json.load(open(top_item))
        items       = message["content"]["status"]["top_items"]
    
        logger.debug('Selecting best fit for torrent')
        taskAction = TorrentSelector()
        best_items  = taskAction.sort_top_items(items)
        pprint.pprint(best_items)
        print "="*120
    
  except Exception as inst:
    Utilities.ParseException(inst, logger=logger)
      
def sort_only_found_items(found_items):
  try:

    logger      = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    for found_item in found_items:
        logger      = Utilities.GetLogger(LOG_NAME, useFile=False)
        logger.debug('Loading data for [found_items]')
        message     = json.load(open(found_item))
        pprint.pprint(message)
        items       = message["content"]["status"]["found_items"]
    
        logger.debug('Selecting best fit for torrent')
        taskAction = TorrentSelector()
        taskAction.sort_found_items(items)
    

  except Exception as inst:
    Utilities.ParseException(inst, logger=logger)
    
def call_task(options):
  ''' Command line method for running sniffer service'''
  try:
    
    logger      = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    logger.debug('Parsing sample data')
    title_data  = {}
    
    print options.top_items[0]
    message     = json.load(open(options.top_items[0] ))
    top_items   = message["content"]["status"]["top_items"]
    title_data.update({'top_items':  top_items})
    
    message     = json.load(open(options.found_items[0]))
    found_items = message["content"]["status"]["found_items"]
    title_data.update({'found_items':  found_items})
    
    search_title= message["content"]["status"]["search_title"]
    title_data.update({'search_title':  search_title})
    
    logger.debug('Selecting best fit for torrent')
    taskAction  = TorrentSelector()
    top_item    = taskAction.findTitle(title_data)
    pprint.pprint(top_item.to_dict())

  except Exception as inst:
    Utilities.ParseException(inst, logger=logger)

if __name__ == '__main__':
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  
  myFormat = '%(asctime)s|%(name)30s|%(message)s'
  logging.basicConfig(format=myFormat, level=logging.DEBUG)
  logger        = Utilities.GetLogger(LOG_NAME, useFile=False)
  logger.debug('Logger created.')
  
  usage = "usage: %prog option1=string option2=bool"
  parser = OptionParser(usage=usage)
  parser.add_option('--top_items',
                      type="string",
                      action="append",
                      default=None,
                      help='Set valid path to JSON sample message of top_items')
  parser.add_option('--found_items',
                      type="string",
                      action="append",
                      default=None,
                      help='Set valid path to JSON sample message of found_items')
  parser.add_option("--just_top_items", "-t", 
                      action="store_true",
                      default=False,
                      help='Do only top_items files')
  parser.add_option("--just_found_items", "-f", 
                      action="store_true",
                      default=False,
                      help='Do only top_items found_items')
    
  (options, args) = parser.parse_args()
  
  if options.top_items is None and not options.just_found_items:
      parser.error("Missing required option: --top_items='/valid/path/file.json'")
      sys.exit()
  elif options.found_items is None and not options.just_top_items:
      parser.error("Missing required option: --found_items='/valid/path/file.json'")
      sys.exit()

  if options.just_top_items:
      logger.debug('Sorting only [top_items]')
      sort_only_top_items(options.top_items)
  elif options.just_found_items:
      logger.debug('Sorting only [found_items]')
      sort_only_found_items(options.found_items)
  else:
      call_task(options)
