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
from TorrentSelector import TorrentSelector


class ConfirmTitle(threading.Thread):
  def __init__(self, **kwargs):
      '''Service task constructor'''
      # Initialising thread parent class
      threading.Thread.__init__(self)

      try:
          # Initialising class variables
          self.component	= self.__class__.__name__
          self.logger	    = Utilities.GetLogger(self.component)
          
          # Thread action variables
          self.tStop 	    = threading.Event()
          self.threads	    = []
          self.tid		    = None
          self.running	    = False

          ## Adding local variables
          self.service	    = None
          self.onStart      = True
          self.not_searching= False
          self.title_data   = {}
          self.selector     = TorrentSelector()
          
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
          self.running    = True
          ## Setting thread for joining service list
          if self.service is not None and self.service.task is not None:
              self.logger.debug('  + Adding thread for joining context')
              self.service.task.AddContext(self, self.component)
          
          # Getting thread GetPID
          self.tid        = Utilities.GetPID()
          self.logger.debug('Starting task [%s] with PID [%d]'% (self.component, self.tid))
          
          self.logger.debug('Looping for task [%d]'%self.tid)
          while not self.tStop.isSet():
              if self.not_searching:
                  if len(self.title_data.keys())<1:
                      raise Exception('Search data not defined but process started')
                  self.logger.debug('   +++ Confirming title data')
                  top_item_df       = self.selector.findTitle(self.title_data)
                  
                  result_state      = 'failed'
                  itemsCall         = { }
                  top_item          = { }
                  if top_item_df.shape[0] > 0:
                      result_state  = 'success'
                  
                      print "-_"*60
                      top_items_dict= top_item_df.to_dict()
                      print "-_"*60
                      pprint.pprint(top_items_dict)
                      top_item      = self.selector.reduce_item(top_items_dict)
                      print "-_"*60
                      pprint.pprint(top_item)
                  
                  
                  itemsCall.update({'confirmed_title': top_item })
                  itemsCall.update({'search_title': self.title_data['search_title'] })
                  pprint.pprint(itemsCall)
                  print"#"*100
                  print"#"*100
                  print"#"*100
                  self.service.notify("updated", result_state, items=itemsCall)

                  self.logger.debug('   +++ Resetting search query...')
                  self.not_searching = False
              
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
    ''' Execute ConfirmTitle task by calling a "run" method in the service'''

  def SetSearch(self, status):
    try:
      ## Getting correct action
      self.logger.debug('   +++ Setting up [search_title]')
      search_title      = status['search_title']
      self.title_data.update({'search_title':  search_title})
      
      ## Getting type of status message
      if status["device_action"] == "find_magnet":
          self.logger.debug('   +++ Setting up [top_items]')
          ## pprint.pprint(status)
          ## print "="*120
          self.title_data.update({'top_items': status['top_items']})
      elif status["device_action"] == "imdb_data":
          self.logger.debug('   +++ Setting up [found_items]')
          self.title_data.update({'found_items':  status['found_items']})
          self.not_searching = True
          self.logger.debug('   +++ Starting search process')
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
    
    taskAction = ConfirmTitle(**args)

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