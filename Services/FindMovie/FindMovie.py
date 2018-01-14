#!/usr/bin/python

import json
import time
import sys, os
import json
import logging
import threading
import imp
import re
import pprint

from optparse import OptionParser

from Utils import Utilities

from TorrentFinder import TorrentFinder

logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("log1").setLevel(logging.WARNING)

class FindMovie(threading.Thread):
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
      self.title        = None
      self.page_limit   = None
      self.search_engines= None
      self.action_words = None
      self.not_searching= False
      self.finder       = None

      # Generating instance of strategy 
      for key, value in kwargs.iteritems():
	if "service" == key:
	  self.service = value
	elif "onStart" == key:
	  self.onStart = bool(value)
        elif "page_limit" == key:
          self.page_limit = int(value)
        elif "search_engines" == key:
          self.search_engines = value
          pprint.pprint(self.search_engines)
        elif "action_words" == key:
          self.action_words = value

      ## Setting item started for reporting to device action
      self.running	= True

      ## Creating torrent finder instance
      self.finder = TorrentFinder(self.search_engines)

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
  
  def SetSearch(self, status):
    try:
      ## Getting correct action
      action_sentence   = status['action']
      action_caller     = status['caller']
      action_words_re   = "|".join(self.action_words)+ '\b'
      isMatch           = re.search(action_words_re, action_sentence, re.I)
      #print "=== status.action:", status['action']
      #print "=== status.caller:", status['caller']
      #print "=== action_sentence:", action_sentence
      #print "=== action_words_re:", action_words_re
      #print "=== isMatch:", isMatch
      if isMatch:
        keyword = isMatch.group(0)
        action_phrase   = action_sentence[len(keyword):].lstrip().strip()
        self.title      = action_phrase
        self.logger.debug("   ~~~ Found action [%s] with phrase [%s]"%
                          (keyword, action_phrase))

        self.not_searching = True
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
    
  def locate_torrent(self, title, page_limit):
    """Execution begins here."""
    resultItems = {}
    try:
      ## Searching items
      top_items         = self.finder.Search(title, page_limit)
      if len(top_items)<1:
        self.logger.debug('   ~~~ No items found, data frame of size [%d]'%len(top_items))
        return None

      top_items         = top_items.to_dict()
      itemKeys          = top_items.keys()
      resultItems       = {"top_items": []}

      ## Creating transposed list of top items
      for key in itemKeys:
        sub_items       = top_items[key]
        frameIds        = sub_items.keys()
        for frame_id in frameIds:
          result_items  = resultItems["top_items"]
          result_items.append( {"frame_id":frame_id} )
        break

      ## Looking into each dataframe-based item
      for key in itemKeys:
        sub_items       = top_items[key]
        frameIds        = sub_items.keys()
        for frame_id in frameIds:
          result_items  = resultItems["top_items"]

          ## Generating transposed items
          sub_item      = { key : sub_items[frame_id]}
          for result_item in result_items:
            if result_item["frame_id"] == frame_id:
              result_item.update(sub_item)
              break

    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
    finally:
      #return resultItems["top_items"]
      pprint.pprint(resultItems)
      return resultItems

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
          if self.title is None:
            raise Exception('Torrent search started with invalid title')

          self.logger.debug('   ~~~ Searching for torrent')
          top_items          = self.locate_torrent(self.title, self.page_limit)
          itemsCall        = {'search_title' :self.title}
          if top_items is None or 'top_items' not in top_items.keys():
            result_state     = 'failed'
            itemsCall.update({ 'top_items' : {}})
          else:
            self.logger.debug('   ~~~ Found [%d] items'%len(top_items["top_items"]))
            result_state     = 'success'
            #pprint.pprint(top_items)
            itemsCall.update({ 'top_items' : top_items})

          self.logger.debug('   ~~~ Posting results')
          self.service.notify("updated", result_state, items=itemsCall)

          self.logger.debug('   ~~~ Resetting search query...')
          self.not_searching = False
          self.title         = None
	self.tStop.wait(1)
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
    ''' Execute FindMovie task by calling a "run" method in the service'''


## Standalone main method
LOG_NAME = 'TaskTool'
def call_task(options):
  ''' Command line method for running sniffer service'''
  try:
    
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    args = {}
    args.update({'option1': options.opt1})
    args.update({'option2': options.opt2})
    
    taskAction = FindMovie(**args)

  except Exception as inst:
    Utilities.ParseException(inst, logger=logger)

if __name__ == '__main__':
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  
  myFormat = '%(asctime)s|%(name)30s|%(message)s'
  logging.basicConfig(format=myFormat, level=logging.DEBUG)
  logger 	= Utilities.GetLogger(LOG_NAME, useFile=False)
  logger.debug('Logger created.')
  
  usage = "usage: %prog option1=string option2=bool"
  parser = OptionParser(usage=usage)
  parser.add_option('--opt1',
		      type="string",
		      action='store',
		      default=None,
		      help='Write here something helpful')
  parser.add_option("--opt2", 
		      action="store_true", 
		      default=False,
		      help='Write here something helpful')
    
  (options, args) = parser.parse_args()
  
  if options.opt1 is None:
    parser.error("Missing required option: --opt1='string'")
    sys.exit()
    
  call_task(options)