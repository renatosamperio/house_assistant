#!/usr/bin/env python

import zmq
import random
import sys
import time
import logging
import json
import pprint

from Utils import Utilities
from optparse import OptionParser, OptionGroup
from Utils.XMLParser import ParseXml2Dict

LOG_NAME = 'MOCK_PUB'

def publish_message(msg, endpoint, topic='control'):
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  try:
    logger.debug( "+ Connecting endpoint [%s] in topic [%s]"%
                      (endpoint, topic))

    ## Creating ZMQ connection
    zmq_context = zmq.Context()
    socket  = zmq_context.socket(zmq.PUB)
    socket.connect(endpoint)
    time.sleep(0.1)

    ## Sending JSON message
    json_msg = json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))
    logger.debug( "+ Sending message of [%d] bytes"%(len(json_msg)))
    message = "%s @@@ %s" % (topic, json_msg)
    socket.send(message)

  except Exception as inst:
    Utilities.ParseException(inst)

def message_control(identifiers):
  msg = {
    "content": {
        "status": {
            "device_action": identifiers['DeviceAction'],
            "result": identifiers['Result']
        }
    },
    "header": {
        "action": identifiers['DeviceAction'],
        "service_id": identifiers['ServiceId'],
        "service_name": identifiers['ServiceName'],
        "service_transaction": identifiers['ServiceTransaction']
    }
  }
  return msg

def PubMessage(services, topic=None):
  ''''''
  try:
    '''' Starts service task in test environent'''
    ## Setting semaphore for checking next task service
    print('Setting semaphore for next task service')
    #print "*"*150
    
    ## Starting services one-by-one
    fileName    = 'Context-%s.xml'%services['context_name']
    contextFile = services['service_path']+'/Conf/'+fileName
    endpoint    = "tcp://"+services['server_ip']+":"+services['pub_port']
    serviceName = 'context'
    transaction = services['transaction']
    
    # Creating fake parser
    parser = OptionParser()
    parser.add_option('--context_file',
                    type="string",
                    action='store',
                    default=None)
    parser.add_option('--transaction',
                    type="string",
                    action='store',
                    default=None)
    parser.add_option('--action',
                    type='choice',
                    action='store',
                    dest='action',
                    choices=['start', 'stop', 'restart', 'none'],
                    default='none')
    parser.add_option('--endpoint', 
                    metavar="URL", 
                    default=None)
    parser.add_option('--service_name',
                    type='choice',
                    action='store',
                    dest='service_name',
                    choices=['context'],
                    default=None)
    parser.add_option('--topic', 
                    metavar="TOPIC", 
                    default='process')  
    parser.add_option('--task_id', 
                    metavar="TASK_ID", 
                    default='all')
    parser.add_option('--use_file', 
                    dest='use_file', 
                    action='store_true',
                    default=True)
    
    (options, args)     = parser.parse_args()
    options.transaction = transaction
    options.action      = services['action']
    options.service_name= serviceName
    options.context_file= contextFile
    
    if services['topic'] is not None:
      options.topic     = services['topic']
      
    if services['taskId'] is not None:
      options.task_id   = services['taskId']
      
    ## Preparing JSON Message
    msg = conf_command.message(options)
    if msg is not None:
      print( "+ Connecting endpoint [%s] in topic [%s]"%
                        (endpoint, options.topic))

      ## Creating ZMQ connection
      context = zmq.Context()
      socket  = context.socket(zmq.PUB)
      socket.connect(endpoint)
      time.sleep(0.1)
      
      ## Sending JSON message
      json_msg = json.dumps(msg, sort_keys=True, indent=4, separators=(',', ': '))
      print( "+ Sending message of [%d] bytes"%(len(json_msg)))
      message = "%s @@@ %s" % (options.topic, json_msg)
      socket.send(message)
      print "*"*150
      print message
      print "*"*150
      
      ## Closing ZMQ connection
      socket.close()
      context.destroy()
      time.sleep(0.1)

  except Exception as inst:
    Utilities.ParseException(inst)

def send_message(options):
  try:
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    control_msg = {}

    ## Parsing message context file
    logger.debug( "+ Parsing message context file")
    context_info = ParseXml2Dict(options.xml_file, 'Context')
    for task in context_info['TaskService']:
      if task['id'] == options.task_id:
        device_action = task['Task']['message']['content']['configuration']['device_action']

        ## Preparing basic message content
        generic_identifiers = {
          'DeviceAction': device_action,
          'Result': 'success',
          'ServiceId': task['id'],
          'ServiceName': task['Task']['message']['header']['service_name'],
          'ServiceTransaction': options.transaction,
        }
        control_msg = message_control(generic_identifiers)

        ## Adding extra variables according to each command
        if device_action == 'voice_action':
          extra_items = {
            'caller': options.voice_caller,
            'action': options.voice_action,
          }
          control_msg["content"]["status"].update(extra_items)
    ## Publishing message
    publish_message(control_msg, context_info['BackendEndpoint'], topic='control')
  except Exception as inst:
    Utilities.ParseException(inst)


if __name__ == '__main__':
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  
  myFormat = '%(asctime)s|%(name)30s|%(message)s'
  logging.basicConfig(format=myFormat, level=logging.DEBUG)
  logger        = Utilities.GetLogger(LOG_NAME, useFile=False)
  logger.debug('Logger created.')
  
  usage = "usage: %prog --xml_file=file.xml --task_id=ts_000"
  parser = OptionParser(usage=usage)
  parser.add_option('--xml_file', 
                  metavar="FILE", 
                  default=None,
                  help='Context XML file')  
  parser.add_option('--task_id', 
                  metavar="ID", 
                  default=None,
                  help='Unique process identifier')  
  parser.add_option('--transaction',
                  type="string",
                  action='store',
                  metavar="TRANSACTION", 
                  default=None,
                  help='Unique transaction identifier')  
  parser.add_option('--result',
                  type="string",
                  metavar="SUCCESS|FAIL", 
                  action='store',
                  default='success',
                  help='Status result')  
  parser.add_option('--topic', 
                  type="string",
                  metavar="TOPIC", 
                  default='control',
                  help='Message topic')

  voiceAction = OptionGroup(parser, 'Options for voice action task')
  voiceAction.add_option('--voice_caller',
                  type="string",
                  action='store',
                  metavar="CALLER",
                  default='Washington',
                  help='Receiver of speech command')
  voiceAction.add_option('--voice_action',
                  type="string",
                  action='store',
                  metavar="ACTION", 
                  default='search Star Wars The Last Jedi',
                  help='Speech command')

  parser.add_option_group(voiceAction)
  (options, args) = parser.parse_args()

  if options.xml_file is None:
    parser.error("Missing required option: --xml_file='/path/file.xml'")
    sys.exit()

  if options.task_id is None:
    parser.error("Missing required option: --task_id='ts_000'")
    sys.exit()

  if options.transaction is None:
    parser.error("Missing required option: --transaction='aaabbbcccddd'")
    sys.exit()

  send_message(options)

