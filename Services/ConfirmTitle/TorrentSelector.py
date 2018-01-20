#!/usr/bin/env python
# -*- coding: latin-1 -*-

from optparse import OptionParser
from Utils import Utilities

class TorrentSelector(object):

  def __init__(self, **kwargs):
    try:
      # Initialising class variables
      self.component    = self.__class__.__name__
      self.logger       = Utilities.GetLogger(self.component)

    except Exception as inst:
      Utilities.ParseException(inst, logger=logger)


## Standalone main method
LOG_NAME = 'TorrentSelector'
def call_task(options):
  ''' Command line method for running sniffer service'''
  try:
    
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    args = {}
    args.update({'option1': options.opt1})
    args.update({'option2': options.opt2})
    
    taskAction = TorrentSelector(**args)

  except Exception as inst:
    Utilities.ParseException(inst, logger=logger)

if __name__ == '__main__':
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  
  myFormat = '%(asctime)s|%(name)30s|%(message)s'
  logging.basicConfig(format=myFormat, level=logging.DEBUG)
  logger        = Utilities.GetLogger(LOG_NAME, useFile=False)
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