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

from VoiceOrder import Recorder


class GetVoiceOrder(threading.Thread):
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
      
      ## Service variables
      self.butler_words  = None

      # Generating instance of strategy
      for key, value in kwargs.iteritems():
	if "service" == key:
	  self.service = value
        elif "butler_words" == key:
          self.butler_words = value
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

      ## Starting voice recognition
      self.logger.debug('Setting up voice recognition...')
      args = {}
      args.update({'butler_words': self.butler_words})
      args.update({'service': self.service})
      voice_recognition = Recorder(**args)

      self.logger.debug('Looping for task [%d]'%self.tid)
      voice_recognition.Run()
      #while not self.tStop.isSet():
	## Do something here!
	## ...
	#self.tStop.wait(5)
	
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
    ''' Execute GetVoiceOrder task by calling a "run" method in the service'''


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
    
    taskAction = GetVoiceOrder(**args)

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