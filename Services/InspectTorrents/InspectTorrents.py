#!/usr/bin/python

import json
import time
import sys, os
import json
import logging
import threading
import imp

from limetorrents_crawler import LimeTorrentsCrawler
from FindChanges import FindChanges

from tables.index import opt_search_types
from optparse import OptionParser, OptionGroup
from Utils import Utilities

logging.getLogger("chardet.charsetprober").setLevel(logging.WARNING)
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

class InspectTorrents(threading.Thread):
  def __init__(self, **kwargs):
    '''Service task constructor'''
    # Initialising thread parent class
    threading.Thread.__init__(self)
    
    try:
      # Initialising class variables
      self.component	= self.__class__.__name__
      self.logger	= Utilities.GetLogger(self.component)
      
      # Thread action variables
      self.tStop 	= threading.Event()
      self.threads	= []
      self.tid		= None
      self.running	= False
      
      ## Adding local variables
      self.service	= None
      self.onStart	= True
      self.not_searching= False

      # Generating instance of strategy
      for key, value in kwargs.iteritems():
	if "service" == key:
	  self.service = value
	elif "onStart" == key:
	  self.onStart = bool(value)
	  
      ## Setting item started for reporting to device action
      self.running	= True
	
      # Starting action thread
      if self.onStart:
	self.logger.debug("  + Process is starting immediately")
	self.start()
      
      ## Adding monitor thread to the list of references
      self.threads.append(self)
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

  def hasStarted(self):
    ''' Reports task thread status'''
    return self.running and not self.tStop.isSet()

  def hasFinished(self):	
    ''' Reports task thread status'''
    return not self.running and self.tStop.isSet()
  
  def run(self):
    '''Threaded action '''
    try:
      ## Setting process as started
      self.running	= True
      
      ## Setting thread for joining service list
      if self.service is not None and self.service.task is not None:
	self.logger.debug('  + Adding thread for joining context')
	self.service.task.AddContext(self, self.component)
      
      # Getting thread GetPID
      self.tid		= Utilities.GetPID()
      self.logger.debug('Starting task [%s] with PID [%d]'% (self.component, self.tid))
      
      self.logger.debug('Looping for task [%d]'%self.tid)
      while not self.tStop.isSet():
        if self.not_searching:
          self.logger.debug('   ~~~ Starting action handler action')
	# Do something here!
	# ...
          self.logger.debug('   ~~~ Posting results')
          self.service.notify("updated", result_state, items=itemsCall)

          self.logger.debug('   ~~~ Resetting search query...')
          self.not_searching = False
	self.tStop.wait(5)
	
      # Ending thread routine
      self.running = False
      self.logger.debug('Ending thread [%d]'%self.tid)
      
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
    
  def close(self):
    ''' Ending task service'''
    try:
      # Do additional stopping routines here
      # ...
      
      if not self.tStop.isSet():
	# Stop thread and give some time to do whatever it has to do
	self.logger.debug(" Stopping task thread, setting event...")
	self.tStop.set()
	time.sleep(0.75)
      else:
	self.logger.debug(" Event loop is already interrupted")
      
      # Force to stop before leaving
      if self.is_alive() and self.service is not None:  
	self.logger.debug( "  Stopping the thread and wait for it to end")
	threading.Thread.join(self, 1)  
      else:
	self.logger.debug(" Thread is not alive")
	
      self.logger.debug( "  Thread stopped")
      
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

  def execute(self, service):
    ''' Execute InspectTorrents task by calling a "run" method in the service'''

  def SetSearch(self, status):
    try:
      ## Getting correct action
      VARIABLE1      = status['VARIABLE1']
      VARIABLE2      = status['VARIABLE2']
      self.not_searching = True
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

## Standalone main method
LOG_NAME = 'InspectTorrents'
def main(options):
    
    args = {}
    args.update({
        'title':        options.title,
        'page_limit':   options.page_limit,
        'search_type':  options.search_type,
        'with_magnet':  options.with_magnet,
        'with_db':      options.with_db,
        'database':     options.database,
        'collection':   options.collection,
        })
    
    
    if options.with_search:
        logger.debug("[LimeTorrents]")        
        lmt = LimeTorrentsCrawler(**args)
        lmt.Run()
        logger.debug("Bye!")
        
    elif options.with_changes:
        taskAction = FindChanges(**args)
        taskAction.GetMovies(['leeches', 'seeds'])
    
if __name__ == '__main__':
    usage    = "usage: %prog interface=arg1 filter=arg2"
    logger   = Utilities.GetLogger(LOG_NAME, 
                                   useFile=True,
                                   fileLen=10000000, 
                                   nFiles=20)
    myFormat = '%(asctime)s|%(name)30s|%(message)s'
    logging.basicConfig(format=myFormat, level=logging.DEBUG)
    logger.debug('Logger created.')
  
    search_types = ['all', 'movies', 'music', 'games', 'applications', 
                    'browse-movies', 'browse-shows']
    parser   = OptionParser(usage=usage)
    
    torrent_search = OptionGroup(parser, "Torrent crawling configuration",
              "Used to find most populat torrents")
    torrent_search.add_option("--with_search", 
               action="store_true", 
               default=False,
               help='Find items with latest changes')
    torrent_search.add_option("--title", 
              action="append", 
              help="Input title",
              default=[])
    torrent_search.add_option('--page_limit',
              action="store",
              type="int",
              help="Input page number",
              default=1)
    torrent_search.add_option('--search_type',
              type="choice",
              action='store',
              default='all',
              choices=search_types,
              help='Search torrent types:'+str(search_types))
    torrent_search.add_option("--with_magnet", 
              action="store_true", 
              default=False,
              help='Get torrent magnet for all search items')
    
    db_operations = OptionGroup(parser, "Torrent DB configuration",
              "Used for storing collected data")
    db_operations.add_option("--with_db", 
              action="store_true", 
              default=False,
              help='Get torrent magnet for all search items')
    db_operations.add_option('--database',
              type="string",
              action='store',
              default=None,
              help='Input database name')
    db_operations.add_option('--collection',
              type="string",
              action='store',
              default=None,
              help='Input collection name')
    
    find_updates = OptionGroup(parser, "Torrent popularity search",
               "Used to identify changes in leeches and seeds")
    find_updates.add_option("--with_changes", 
               action="store_true", 
               default=False,
               help='Find items with latest changes')
     
    parser.add_option_group(find_updates)
    parser.add_option_group(torrent_search)
    parser.add_option_group(db_operations)
  
    (options, args) = parser.parse_args()
    
    if options.with_search:
        if 'browse-' not in options.search_type and len(options.title) < 1:
          parser.error("Missing required option: --search_type='%s'"%str(search_types))
    
        if options.with_db:
            if options.collection is None:
              parser.error("Missing required option: --collections='collections_name'")
            if options.database is None:
              parser.error("Missing required option: --database='database_name'")
    
    elif options.with_changes:
        if options.collection is None:
          parser.error("Missing required option: --collections='collections_name'")
        if options.database is None:
          parser.error("Missing required option: --database='database_name'")
    else:
        parser.error("Missing required option: --with_search|--with_changes")
    main(options)