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

def send_message(options):
  try:
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    control_msg = {}

    ## Parsing message context file
    logger.debug( "+ Parsing message context file")
    context_info = ParseXml2Dict(options.xml_file, 'Context')
    
    ## Openning JSON message file
    json_file = open(options.json_file, 'r')

    json_message = json_file.read().replace('\n', '')
    json_file.close()
    json_message = json.loads(json_message)

    ## Publishing message
    publish_message(json_message, context_info['BackendEndpoint'], topic='control')
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
  parser.add_option('--json_file', 
                  type="string",
                  metavar="FILE", 
                  default=None,
                  help='Default JSON message')

  (options, args) = parser.parse_args()

  if options.xml_file is None:
    parser.error("Missing required option: --xml_file='/path/valid/file.xml'")
    sys.exit()

  if options.task_id is None:
    parser.error("Missing required option: --task_id='ts_000'")
    sys.exit()

  if options.transaction is None:
    parser.error("Missing required option: --transaction='aaabbbcccddd'")
    sys.exit()

  if options.json_file is None:
    parser.error("Missing required option: --json_file='/path/valid/file.json'")
    sys.exit()
    
  send_message(options)

