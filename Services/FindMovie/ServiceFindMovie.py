#!/usr/bin/env python
# -*- coding: latin-1 -*-

import logging
import zmq
import threading
import sys
import time
import random
import signal
import os
import json
import pprint

from Utils import Utilities
from Provider.IServiceHandler import ServiceHandler
from FindMovie import FindMovie

class ServiceFindMovie(ServiceHandler):
  ''' Service for Looks for a movie torrents'''
  def __init__(self, **kwargs):
    ''' Service constructor'''
    ServiceHandler.__init__(self, **kwargs)
    
  def DeserializeAction(self, msg):
    ''' Validates incoming message when called in service section'''
    try:
      isForDevice = msg['header']['service_name'] == 'movie_finder' or msg['header']['service_name'] == 'all'
	
      isRightTransaction = False
      if 'transaction' in msg['header'].keys():
	isRightTransaction = msg['header']['transaction'] == self.transaction
      elif 'service_transaction' in msg['header'].keys():
	isRightTransaction = msg['header']['service_transaction'] == self.transaction
      else:
	self.logger.debug("Message without transaction ID")
	return isRightTransaction
      
      if not isRightTransaction:
	self.logger.debug("Service with different transaction")
	return False
      
      result = (isForDevice and isRightTransaction)
      if result:
	self.logger.debug("   Validation [PASSED]")
      return result
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
  
  def ParseItems(self, items, resp_format):
    ''' Obtains data from input parameters'''
    try:
      self.logger.debug("  + Parsing items in action...")
      status = resp_format["content"]["status"]
      
      ## Adding more parameters
      if items is not None:
      	itemsKeys = items.keys()
      	for item in itemsKeys:
      	  status.update({item:items[item]})

      resp_format["content"]["status"] = status 
      return resp_format  
	      
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

  def GetActionHandler(self, msg):
    ''' '''    
    self.logger.debug("Creating a Looks for a movie torrents")
    result	="failure"
    deviceAction= None
    
    try:
      message = msg['Task']['message']
      conf 	= message['content']['configuration']
      state 	= msg['Task']['state']
      confKeys 	= conf.keys()
      args 	= {'onStart': True, 'service': self}

      ## Parsing parameters
      self.logger.debug("   Parsing message parameters")
      for key in confKeys:
	if key in confKeys:
	  value = message['content']['configuration'][key]
	  args.update({key: value})

      # Creating service object and notify
      start_state = 'started'
      taskType = state['type']
      if not(taskType == 'on_start' or taskType == 'start_now'):
	self.logger.debug("  - Process is set and start is on demand")
	args['onStart'] = False
	start_state = 'created'	
	
      # Creating service object and notify
      deviceAction = FindMovie(**args)
      if deviceAction.hasStarted():
	result="success"
      else:
	self.logger.debug("  Failed to instance action handler")


    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
    finally:
      # Notifying if task was created
      tid = Utilities.GetPID()
      self.notify("started", result, items={'pid':tid})
      return deviceAction
    
  def close(self):
    ''' Ends process inside the service'''
    self.logger.debug("Stopping Looks for a movie torrents service")
    
  def ControlAction(self, msg):
    ''' Actions taken if another process reports with control topic'''
    try:
      ## Validating transaction
      isRightTransaction = self.ValidateTransaction(msg)
      if not isRightTransaction:
	self.logger.debug("Service with different transaction")
	return isRightTransaction
	  
      header = msg['header']
      content = msg['content']

      if 'service_name' not in header.keys():
        self.logger.debug("[service_name] Not found in [header]")
        return

      if 'status' not in content.keys():
        self.logger.debug("[status] Not found in [content]")
        return
      status =  content['status']

      if 'result' not in status.keys():
        self.logger.debug("[result] Not found in [status]")
        return

      ## Filtering reporting process
      if header['service_name'] == 'voice_order' and status["result"] == "success":
        statusKeys = status.keys()
        if 'action' not in statusKeys:
          self.logger.debug("[action] Not found in [status]")
          return
        if 'caller' not in statusKeys:
          self.logger.debug("[caller] Not found in [status]")
          return
        self.logger.debug("Received message for action handler")
        self.actionHandler.SetSearch(status)

      ## elif header['service_name'] == 'PROCESS2'  and status["result"] == 'success':
      ##   self.logger.debug("Received message from PROCESS2'")
      ##   Do something useful

    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
