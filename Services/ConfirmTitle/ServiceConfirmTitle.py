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

from Utils import Utilities
from Provider.IServiceHandler import ServiceHandler
from ConfirmTitle import ConfirmTitle

class ServiceConfirmTitle(ServiceHandler):
  ''' Service for Confirms found tittle'''
  def __init__(self, **kwargs):
    ''' Service constructor'''
    ServiceHandler.__init__(self, **kwargs)

  def DeserializeAction(self, msg):
    ''' Validates incoming message when called in service section'''
    try:
      isForDevice = msg['header']['service_name'] == 'confirm_title' or msg['header']['service_name'] == 'all'
	
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
    self.logger.debug("Creating a Confirms found tittle")
    result	="failure"
    deviceAction= None
    start_state = 'failed'
    
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
      deviceAction = ConfirmTitle(**args)
      if deviceAction.hasStarted():
	result="success"
      else:
	self.logger.debug("  Failed to instance action handler")


    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
    finally:
      # Notifying if task was created
      tid = Utilities.GetPID()
      self.notify(start_state, result, items={'pid':tid})
      return deviceAction

  def close(self):
    ''' Ends process inside the service'''
    self.logger.debug("Stopping Confirms found tittle service")

  def ControlAction(self, msg):
    ''' Actions taken if another process reports with control topic'''
    try:
      ## Validating transaction
      isRightTransaction = self.ValidateTransaction(msg)
      if not isRightTransaction:
	self.logger.debug("Service with different transaction")
	return isRightTransaction
	  
      header = msg['header']
      status =  msg['content']['status']

      if 'service_name' not in header.keys():
        self.logger.debug("[service_name] Not found in [header]")
        return

      if 'content' not in msg.keys():
        self.logger.debug("[content] Not found in [content]")
        return
      content =  msg['content']
      
      if 'status' not in content.keys():
        self.logger.debug("[status] Not found in [content]")
        return
      status =  content['status']

      if 'result' not in status.keys():
        self.logger.debug("[result] Not found in [status]")
        return

      # Filtering reporting process
      if header['service_name'] == 'movie_finder' and status["result"] == "success":
        self.logger.debug("Received message from [movie_finder]")
        statusKeys = status.keys()
        if 'search_title' not in statusKeys:
          self.logger.debug("[search_title] Not found in [status]")
          return
        if 'top_items' not in statusKeys:
          self.logger.debug("[top_items] Not found in [status]")
          return
        self.logger.debug("Received message for action handler")
        self.actionHandler.SetSearch(status)

      elif header['service_name'] == 'search_imdb' and status["result"] == "success":
        self.logger.debug("Received message from [search_imdb]")
        statusKeys = status.keys()
        if 'search_title' not in statusKeys:
          self.logger.debug("[search_title] Not found in [status]")
          return
        if 'found_items' not in statusKeys:
          self.logger.debug("[found_items] Not found in [status]")
          return
        self.logger.debug("Received message for action handler")
        self.actionHandler.SetSearch(status)

    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)
