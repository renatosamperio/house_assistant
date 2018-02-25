#!/usr/bin/python

import json
import time
import sys, os
import json
import logging
import imp
import re
import csv, codecs, cStringIO
import pprint

import pandas as pd

from optparse import OptionParser
from collections import Counter

from Utils import Utilities
from Utils import MongoAccess

class FindChanges():
    def __init__(self, **kwargs):
        '''Service task constructor'''
        
        try:
            # Initialising class variables
            self.component        = self.__class__.__name__
            self.logger           = Utilities.GetLogger(self.component)

            # Thread action variables
            self.tid              = None
            self.running          = False
            
            ## Adding local variables
            self.database         = None
            self.collection       = None
            self.action_words     = None
            self.not_searching    = False
            self.database         = None
            self.torrent_terms    = None

            # Generating instance of strategy 
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "collection" == key:
                    self.collection = value
                elif "with_changes" == key:
                    self.with_changes = self.LoadTerms('list_termx.txt')
                    

            ## Setting item started for reporting to device action
            self.running    = True
            self.logger.debug("  + Generating database [%s] in [%s] collections"% 
                                (self.database, self.collection))
            self.db_handler = MongoAccess(debug=False)
            self.db_handler.connect(self.database, self.collection)
            
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def LoadTerms(self, fileName):
        try:
            with open(fileName, 'r') as file:
                return file.read().strip().split()
                
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def FindChange(self, items_id, record):
        '''
        Finds if data has changes by looking into time series models
        with requested items ID (leeches and seeds). The changes 
        considers the 1st derivative of every record in DB.
        '''
        changed_items                   = None
        try:
            for item in items_id:
                ##self.logger.debug("      Looking for changes in [%s] of [%s]", item, record['hash'])
                item_id_dict            = record[item]['value']
                item_df                 = pd.DataFrame(item_id_dict)
                for col_index in item_df:
                    item_se             = pd.Series(item_df[col_index])
                    item_df[col_index]  = pd.to_numeric(item_se)
                
                    ## Drop first element as it is does not has 1st derivative
                    first_derivative    = item_df[col_index].diff().iloc[1:]
                    
                    ## Check items that derivative is not zero
                    has_zeros           = first_derivative[first_derivative >0]
                    if has_zeros.count() > 0:
                        self.logger.debug("      Something changed in [%s] of item [%s]", 
                                          item, record['hash'])
                        
                        ## Add items if they are not included
                        changed_items = record
                        return
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return changed_items
    
    def SaveNameWords(self, all_names):
        try:
            most_common = all_names.most_common()
            with open('common_words.csv', 'wb') as csvfile:
                writer = csv.writer(csvfile)
                for word, qty in most_common:
                    word_udata=word.encode('ascii', 'ignore').decode('ascii')
                    row = [word_udata, qty]
                    writer.writerow(row)
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
            
    def GetMovies(self, items_id):
        '''
        Finds if there is a change time series by using first derivative
        '''
        try:
            db_size = self.db_handler.Size()
            self.logger.debug("  + Getting [%d] records from [%s]"%(db_size, self.collection))
            
            ## Find all available records
            records         = self.db_handler.Find()
            counter         = 1
            
            ## Defined local function to find changes in time series
            self.logger.debug("      Looking for changes in collection [%s]",self.collection)
            all_changed_items = []
#             all_names = Counter()
            start_time = time.time()
            super_count = 0
            for record in records:
                ## Collecting names
#                 processed_name = re.sub('[\(\)\{\}<>][-]', ' ', record['name'].lower())
#                 splitted_names = processed_name.split()
                ##splitted_names = [re.sub('[\(\)\{\}<>][-]', ' ', x.lower()) for x in splitted_names]
#                 print "=== n:" , record['name']
#                 print "=== p:", processed_name
#                 print "=== s:", splitted_names
                ## Counting words in each item name
#                 all_names = all_names + Counter(splitted_names)
                
                ## Finding words in list of torrent terms
#                 new_name = ' '.join(list(set(splitted_names) - set(self.torrent_terms)))
#                 print "=== N:", new_name
#                 print "="*100
                
                ## Double records

                ## Collecting leeches and seeds
                changed_item = self.FindChange(items_id, record)
                if changed_item is not None:
                    all_changed_items.append(changed_item)
                counter += 1
                
#                 if super_count > 100:
#                     break
                super_count += 1
            elapsed_time = time.time() - start_time
            self.logger.debug("  + Collected [%d] records in [%s]"%(counter, str(elapsed_time)))
            pprint.pprint(all_changed_items)
            self.logger.debug("  + Saving most popular words in names");
#             self.SaveNameWords(all_names)
                
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

LOG_NAME = 'TaskTool'

def call_task(options):
  ''' Command line method for running sniffer service'''
  try:
    
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    args = {}
    args.update({'database':    options.database})
    args.update({'collection':  options.collection})
    
    taskAction = FindChanges(**args)
    taskAction.GetMovies(['leeches', 'seeds'])

  except Exception as inst:
    Utilities.ParseException(inst, logger=logger)

if __name__ == '__main__':
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  
  myFormat = '%(asctime)s|%(name)30s|%(message)s'
  logging.basicConfig(format=myFormat, level=logging.DEBUG)
  logger     = Utilities.GetLogger(LOG_NAME, useFile=False)
  logger.debug('Logger created.')
  
  usage = "usage: %prog option1=string option2=bool"
  parser = OptionParser(usage=usage)
  parser.add_option('--database',
              type="string",
              action='store',
              default=None,
              help='Provide a valid database name')
  parser.add_option('--collection',
              type="string",
              action='store',
              default=None,
              help='Provide a valid collection name')
    
  (options, args) = parser.parse_args()
  
  if options.database is None:
    parser.error("Missing required option: --database='valid_db_name'")
    sys.exit()
  if options.collection is None:
    parser.error("Missing required option: --collection='valid_collection_name'")
    sys.exit()
  print options
  call_task(options)
  