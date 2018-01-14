#!/usr/bin/python

import json
import time
import sys, os
import json
import logging
import threading
import imp
import pprint

from optparse import OptionParser
from Utils import Utilities
from Utils.Similarity import Similarity

from imdbpie import Imdb

class SearchIMDB(threading.Thread):
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
      self.search_title = None

      self.logger.debug("  + Creating IMDB client")
      self.imdb         = Imdb()

      self.logger.debug("  + Creating similarity calculator")
      self.similarity   = Similarity()

      # Generating instance of strategy
      for key, value in kwargs.iteritems():
	if "service" == key:
	  self.service = value
	elif "onStart" == key:
	  self.onStart = bool(value)
        elif "search_title" == key:
          self.search_title = value

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
          found_items   = self.imdb.search_for_title(self.search_title)
          result_state  = 'failed'
          itemsCall     = {}
          if len(found_items)>0:
            for item in found_items:
              if 'title' in item.keys():
                score   = self.similarity.score(self.search_title, item['title'])
                item.update({'score':score})
                result_state  = 'success'

          #pprint.pprint(found_items)
          self.logger.debug('   ~~~ Posting results')
          itemsCall.update({'search_title': self.search_title, 
                            'found_items':found_items})
          self.service.notify("updated", result_state, items=itemsCall)

          self.logger.debug('   ~~~ Resetting search query...')
          self.not_searching = False
          self.search_title = None
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
    ''' Execute SearchIMDB task by calling a "run" method in the service'''

  def SetSearch(self, status):
    try:
      ## Getting correct action
      self.search_title   = status['search_title']
      self.not_searching = True
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

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
    
    taskAction = SearchIMDB(**args)

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