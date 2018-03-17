#!/usr/bin/python

import logging
import sys, os
import json
import pprint

from Utils import Utilities

from optparse import OptionParser, OptionGroup
from transitions import Machine

import pandas as pd
import numpy as np

class PipeModel(object):

    def __init__(self):
        try:
            # Initialising class variables
            self.component          = self.__class__.__name__
            self.logger             = Utilities.GetLogger(self.component)
            
            self.output    = None

        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def execute(self, output, **input):
        result = {}
        try:
            if self.state == 'step1':
                self.Step1(result, **input)
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            output = {'result': result['result']}
            return True

    def step3_current_estimate(self, output, **input):
        '''
        Adds columns for current estimate and its 1st derivative
        '''
        result                  = True
        try:
            ## Collecting input data
            for key, value in input.iteritems():
                if "items_id" == key:
                    items_id    = value
                elif "data_source" == key:
                    data_source = value

            ## Iterating item IDs
            for item_id in items_id:
                current_estimate_values = []
                source                  = input[item_id]
                measurements            = pd.Series(source['Measurement'].values)
                series_size             = sum(1 for x in source.iterrows())
                index                   = 0
                current_estimate_init   = measurements.mode()
                current_estmate_last    = None

                self.logger.debug("  3.1) Calculating current estimate")
                for row in source.iterrows():
                    measurement         = row[1][0]
                    #measurement_1st_der = row[1][1]
                    ##error_estimate      = row[1][2]
                    kalman_gain         = row[1][3]
                    
                    ## Preparing error in estimate
                    if index == 0:
                        current_estimate= current_estimate_init
                    else:
                        current_estimate= current_estmate_last
                    
                    ## Calculating current estimate
                    current_estimate_old= current_estimate
                    current_estimate    = current_estimate + (kalman_gain*(measurement-current_estimate))
                    ##print("CE:\t %.6f, %.6f, %.6f, %.6f" % (kalman_gain, measurement, current_estimate_old, current_estimate))
                    current_estmate_last= current_estimate
                    current_estimate_values.append(current_estimate)
                    
                    index +=1
                ## Adding new columns with error in estimate and Kalman gain
                self.logger.debug("  3.2) Adding current estimate as new columns")
                source['CurrentEstimate'] = current_estimate_values
                
                self.logger.debug("  3.3) Adding current estimate 1st derivative  as new columns")
                current_estimate_series = pd.Series(current_estimate_values)
                derivative              = np.diff(current_estimate_series)
                derivative              = np.insert(derivative.astype(float), 0, float('nan'))
                key                     = '1stDerivCurr'
                source[key]             = derivative

                output.update({item_id : source})
                self.logger.debug("-"*65)
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            output.update({'result': result})

    def step2_kalman_gain(self, output, **input):
        '''
        Adds columns for Kalman gain and error in estimate
        '''
        result                  = True
        try:
            ## Collecting input data
            for key, value in input.iteritems():
                if "items_id" == key:
                    items_id    = value
                elif "data_source" == key:
                    data_source = value

            ## Defining Kalman gain function
            def kalman_gain(error_estimate, error_measurement):
                return error_estimate/(error_estimate+error_measurement)
            
            ##source              = output['result_data'][0]['months_seeds']
            ## Iterating item IDs
            for item_id in items_id:
                source          = input[item_id]
                
                ## Defining initial variables
                kalman_gain_values  = []
                err_estimate_values = []
                error_estimate_last = None
                series_size         = sum(1 for x in source.iterrows())
                  
                ## Calculating initial error in measurement
                self.logger.debug("  2.1) Calculating initial error in measurement")
                measurements        = pd.Series(source['Measurement'].values)
                stddev              = measurements.std()
                error_measurement   = 0.00001 if stddev==0 else stddev
                
                ## Calculating step values for error of estimate and kalman gain
                self.logger.debug("  2.2) Calculating values for error of estimate and Kalman gain")
                for index in range(series_size):
                      
                    ## Preparing error in estimate
                    if index == 0:
                        error_estimate  = 0.001
                    else:
                        error_estimate  = error_estimate_last
                      
                    ## Calculating Kalman gain
                    kalman_gain_val = kalman_gain(error_estimate, error_measurement)
                    kalman_gain_values.append(kalman_gain_val)
    #                print("KG:\t %.6f, %.6f, %.6f" % (error_estimate, error_measurement, kalman_gain_val))
                      
                    ## Calculating error in estimate
                    error_estimate      = (1-kalman_gain_val)*error_estimate
                    error_estimate_last = error_estimate
    #                print("EE:\t %.8f, %.8f, %.8f" % (kalman_gain_val, error_estimate_last, error_estimate))
                    err_estimate_values.append(error_estimate)
                
                ## Adding new columns with error in estimate and Kalman gain
                self.logger.debug("  2.3) Adding error in estimate and Kalman gain as new columns")
                source['ErrorEstimate'] = err_estimate_values
                source['KalmanGain']    = kalman_gain_values
                self.logger.debug("-"*65)
                
                output.update({item_id : source})
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            output.update({'result': result})
        
    def step1_format_data(self, output, **input):
        '''
        Transforms queried data into a collapsed version with all
        values in one column. Days and months are indexed.
        
        Adds column for measurement and its 1st derivative.
        '''
        
        dict_months_us          = {}
        data_unit               = None
        items_id                = None
        result                  = True
        try:
            ## Collecting input data
            for key, value in input.iteritems():
                if "items_id" == key:
                    items_id    = value
                elif "data_unit" == key:
                    data_unit   = value

            ## Iterating item IDs
            for item_id in items_id:
                key_item_dict   = data_unit[item_id]
        
                self.logger.debug("  1.1) Setting sequentially time series for [%s]"%item_id)
                months              = key_item_dict['value']
                
                ## Convert query results to a dataframe
                self.logger.debug("  1.2) Converting query results to a dataframe")
                months_df           = pd.DataFrame(months)
                
                ## Convert indexes (days), columns names (months) from unicode to int 
                ##   and collected values to int
                self.logger.debug("  1.3) Converting indexes, columns and values")
                months_df.index     = months_df.index.map(int)
                months_df.columns   = months_df.columns.astype(int)
                months_df           = months_df.astype(int)
                
                ## Sort dataframe items by index
                self.logger.debug("  1.4) Sorting dataframe items by index")
                months_df.sort_index(inplace=True)
                
                ## Collapse all items in one column
                self.logger.debug("  1.5) Collapsing all items in one column")
                months_us           = months_df.unstack()
                months_series       = pd.Series(months_us)
                derivative          = np.diff(months_series)
                months_us           = pd.DataFrame(months_us, columns = ['Measurement'])
                derivative          = np.insert(derivative.astype(float), 0, float('nan'))
                key                 = '1stDerivMeas'
                months_us[key]      = derivative
                #derivative_size     = derivative.size
                #months_us_size      = sum(1 for x in months_us.iterrows())
                
                ## Collecting indexes as 'pandas.core.indexes.numeric.Int64Index'
                #indexed_months      = months_us.index.get_level_values(0)
                #indexed_days        = months_us.index.get_level_values(1)
                
                item_name = 'months_'+item_id
                self.logger.debug("  1.6) Collecting results for [%s]"%item_name)
                self.logger.debug("-"*65)
                ##lst_months_us.append({item_name : months_us})
                
                output.update({item_id : months_us})
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            output.update({'result': result})
            ##output = dict_months_us

class ForecastModel():
    def __init__(self, **kwargs):
        '''Service task constructor'''
        
        try:
            # Initialising class variables
            self.component          = self.__class__.__name__
            self.logger             = Utilities.GetLogger(self.component)

            #self.fsm                = None
            self.fsm                = PipeModel()
            self.machine            = None
            self.forecast_item      = None
            self.error_estimate     = None
            self.error_measurement  = None
            for key, value in kwargs.iteritems():
                if "forecast_item" == key:
                    self.forecast_item = value
            
            self.states=['step1', 'step2', 'step3', 'step4', 'step5']
            self.transitions = [
                { 'trigger': 'advance', 'source': 'step1', 'dest': 'step2', 'before': 'step1_format_data' },
                { 'trigger': 'advance', 'source': 'step2', 'dest': 'step3', 'before': 'step2_kalman_gain' },
                { 'trigger': 'advance', 'source': 'step3', 'dest': 'step4', 'before': 'step3_current_estimate' },
                { 'trigger': 'advance', 'source': 'step4', 'dest': 'step5', 'before': 'execute' }
            ]
            
            # Initialize
            self.logger.debug("+ Creating state machine")
            self.machine = Machine(self.fsm, states=self.states, transitions=self.transitions, initial='step1')
            ## machine.add_ordered_transitions()
            ## machine.next_state()
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def Run(self, data_unit, items_id):
        '''
        '''
        try:
            ## Initialising input model
            output = {}
            output.update({'data_unit': data_unit})
            
            ## Running model
            for step in range(len(self.transitions)-1):
                self.logger.debug( "Executing model in state: "+self.fsm.state)
                input  = {}
                input.update({'items_id': items_id})
                input.update(output)
                output = {}
                self.fsm.advance(output, **input)
                #pprint.pprint(output)
                 
                if not output['result']:
                    self.logger.debug( "Error: Failure in state: "+self.fsm.state)
                    return
            pprint.pprint(output)
         
#                 self.logger.debug("  4) Calculating current estimate of 1) with mean/mode as first initial value")
#                 self.logger.debug("  5) Calculating error in estimate")
#                 self.logger.debug("  6) Calculating 1st derivative of 1)")
#                 self.logger.debug("  7) Calculating 1st derivative of 4)")
#                 self.logger.debug("  8) Calculating mean, standard deviation and standard score of 6)")
#                 self.logger.debug("  9) Calculating mean, standard deviation and standard score of 7)")
#                 self.logger.debug("  10) Calculating squared error of 8) and 9)")
#                 self.logger.debug("  11) Optimising 10) to obtain minimum error as Initial measurement")

        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
           
def call_task(options):
  ''' Command line method for running sniffer service'''
  try:
    
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    args = {}
    args.update({'database':        options.database})
    args.update({'collection':      options.collection})
    
    logger.debug('Openning sample model file [%s]'%options.forecast_file)
    with open(options.forecast_file, 'r') as file:
        forecast_item = json.load(file)
        args.update({'forecast_item': forecast_item})
    taskAction = ForecastModel(**args)
    for data_unit in forecast_item:
        #taskAction.Run(data_unit, ['leeches', 'seeds'])
        taskAction.Run(data_unit, ['seeds'])
        
  except Exception as inst:
    Utilities.ParseException(inst, logger=logger)

LOG_NAME = 'ForecastModel' 
if __name__ == '__main__':
  logger = Utilities.GetLogger(LOG_NAME, useFile=False)
  
  myFormat = '%(asctime)s|%(name)30s|%(message)s'
  logging.basicConfig(format=myFormat, level=logging.DEBUG)
  logger     = Utilities.GetLogger(LOG_NAME, useFile=False)
  logger.debug('Logger created.')
  
  usage = "usage: %prog option1=string option2=bool"
  parser = OptionParser(usage=usage)
  
  parser.add_option('--database',
              type="string",
              action='store',
              default=None,
              help='Provide a valid database name')
  parser.add_option('--collection',
              type="string",
              action='store',
              default=None,
              help='Provide a valid collection name')
  parser.add_option('--forecast_file',
              type="string",
              action='store',
              default=None,
              help='Input file wiht sample data')
  
  (options, args) = parser.parse_args()
  
  if options.forecast_file is None:
      if options.database is None:
        parser.error("Missing required option: --database='valid_db_name'")
        sys.exit()
      if options.collection is None:
        parser.error("Missing required option: --collection='valid_collection_name'")
        sys.exit()
  ##print options
  call_task(options)
  