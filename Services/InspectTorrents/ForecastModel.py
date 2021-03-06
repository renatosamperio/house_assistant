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

logging.getLogger("transitions.core").setLevel(logging.WARNING)

class PipeModel(object):

    def __init__(self):
        try:
            # Initialising class variables
            self.component          = self.__class__.__name__
            self.logger             = Utilities.GetLogger(self.component)
            
            self.output    = None

        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def step6_choose_optimal(self, output, **input):
        '''
        Adds columns for current estimate and its 1st derivative
        '''
        result                  = True
        try:
            ## Collecting input data
            for key, value in input.iteritems():
                if "data_model" == key:
                    data_model = value

            self.logger.debug("  6.1) Getting most optimal")
            source                  = data_model['model']
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            data_model['step_result']   = result
            output.update({'data_model' : data_model})

    def step5_optimise(self, output, **input):
        '''
        Adds columns for current estimate and its 1st derivative
        '''
        result                  = True
        try:
            data_model          = None
            
            ## Collecting input data
            for key, value in input.iteritems():
                if "data_model" == key:
                    data_model = value
                    
            ## Collecting step data
            initial_error_estim     = data_model['initial_error_estim']
            error_optimisation      = data_model['error_optimisation']
            optimisation_steps      = data_model['optimisation_steps']
            
            ## Iterating item IDs
            ### for item_id in items_id:
            self.logger.debug("  5.1) Getting derivative statistical values")
            deriv_curr_estim_values = []
            #source                  = input[item_id]
            source                  = data_model['model']
            deriv_sq_error          = pd.Series(source['SqErrorDeriv'].values)
            stddev_deriv_sq_error   = deriv_sq_error.std()

            ## Updating error optimisation table
            if error_optimisation is None:
                ## There is no table, creating one
                error_optimisation  = pd.DataFrame([], 
                                                   index=optimisation_steps, 
                                                   columns = ['SumDerivStdev', '1stDerivSum'])
            
            self.logger.debug("  5.2) Calculating 1st derivative of std of sum of squared error")
            list_indexes = list(error_optimisation.index.get_values())
            index_value = list_indexes.index(initial_error_estim)
            
            error_optimisation.iloc[index_value]['SumDerivStdev']   = stddev_deriv_sq_error
            
            ## Calculating 1st derivative for sum of squared errors
            if index_value == 0:
                ## First derivative is a NaN
                der_sum_der_std         = float('nan')
            else:
                previous_value          = error_optimisation.iloc[index_value-1]['SumDerivStdev']
                der_sum_der_std         = stddev_deriv_sq_error - previous_value
            error_optimisation.iloc[index_value]['1stDerivSum'] = der_sum_der_std
            data_model['error_optimisation'] = error_optimisation
            self.logger.debug("-"*65)
                
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            data_model['step_result'] = result
            output.update({'data_model' : data_model})
            
    def step4_normalise(self, output, **input):
        '''
        Adds columns for current estimate and its 1st derivative
        '''
        result                  = True
        try:
            data_model          = None

            ## Collecting input data
            for key, value in input.iteritems():
                if "data_model" == key:
                    data_model = value

            self.logger.debug("  4.1) Getting derivative statistical values")
            deriv_curr_estim_values = []
            deriv_meas_values       = []
            deriv_sq_err_der        = []
            source                  = data_model['model']
            deriv_measurements      = pd.Series(source['1stDerivMeas'].values)
            deriv_current_estimate  = pd.Series(source['1stDerivCurr'].values)
            stddev_deriv_meas       = deriv_measurements.std()
            stddev_deriv_curr_estim = deriv_current_estimate.std()
            mean_deriv_meas         = deriv_measurements.mean()
            mean_deriv_curr_estim   = deriv_current_estimate.mean()
            index                   = 0
            
            self.logger.debug("  4.2) Normalising derivatives with standard score")
            for row in source.iterrows():
                measurement_1st_der = row[1][1]
                curr_estim_1st_der  = row[1][5]
                
                ## Normalising 1st derivative of current estimate with
                ##   standard score
                norm_deriv_curr_est = 0.0
                if stddev_deriv_curr_estim != 0.0:
                    norm_deriv_curr_est = (curr_estim_1st_der-mean_deriv_curr_estim)/stddev_deriv_curr_estim
                deriv_curr_estim_values.append(norm_deriv_curr_est)
                
                ## Normalising 1st derivative of current measurement
                ##    with standard score
                norm_deriv_meas     = 0.0
                if stddev_deriv_meas != 0.0:
                    norm_deriv_meas = (measurement_1st_der-mean_deriv_meas)/stddev_deriv_meas
                deriv_meas_values.append(norm_deriv_meas)
                
                ## Getting squared error of difference of both
                ##   derivatives (measurement and current estimate)
                sq_error_deriv      = (norm_deriv_curr_est**2)-(norm_deriv_meas**2)
                deriv_sq_err_der.append(sq_error_deriv)
                ## print "[%d]\t %.6f, %.6f, %.6f" % (index, norm_deriv_curr_est, norm_deriv_meas, sq_error_deriv)
                
                index +=1

            self.logger.debug("  4.3) Adding derivative data as new columns")
            source["NormDerMeas"]   = deriv_meas_values
            source["NormDerCurrEst"]= deriv_curr_estim_values
            source["SqErrorDeriv"]  = deriv_sq_err_der
            self.logger.debug("-"*65)
            
            data_model['model'] = source
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            data_model['step_result'] = result
            output.update({'data_model' : data_model})

    def step3_current_estimate(self, output, **input):
        '''
        Adds columns for current estimate and its 1st derivative
        '''
        result                  = True
        try:
            data_model          = None
            
            ## Collecting input data
            for key, value in input.iteritems():
                if "data_model" == key:
                    data_model = value

            current_estimate_values = []
            source                  = data_model['model']
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
                ##print("[%d]\t %.6f, %.6f, %.6f, %.6f" % 
                ##      (index, kalman_gain, measurement, current_estimate_old, current_estimate))
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
            source['1stDerivCurr']  = derivative

            ### output.update({item_id : source})
            data_model['model'] = source
            self.logger.debug("-"*65)
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            data_model['step_result'] = result
            output.update({'data_model' : data_model})

    def step2_kalman_gain(self, output, **input):
        '''
        Adds columns for Kalman gain and error in estimate
        '''
        result                  = True
        try:
            ### initial_error_estim = None
            data_model          = None
            
            ## Collecting input data
            for key, value in input.iteritems():
                if "data_model" == key:
                    data_model = value

            ## Collecting step data
            initial_error_estim = data_model['initial_error_estim']
            
            ## Defining Kalman gain function
            def kalman_gain(error_estimate, error_measurement):
                return error_estimate/(error_estimate+error_measurement)
            
            source          = data_model['model']
            
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
                    error_estimate  = initial_error_estim
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
            
            data_model['model'] = source
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            data_model['step_result'] = result
            output.update({'data_model' : data_model})

    def step1_format_data(self, output, **input):
        '''
        Transforms queried data into a collapsed version with all
        values in one column. Days and months are indexed.
        
        Adds column for measurement and its 1st derivative.
        '''
        data_model              = None
        result                  = True
        try:
            ## Collecting input data
            for key, value in input.iteritems():
                if "input_data" == key:
                    input_data   = value
                elif "data_model" == key:
                    data_model = value

            ## Collecting step data
            train_property  = data_model['train_property']
            key_item_dict   = input_data[train_property]
    
            self.logger.debug("  1.1) Formatting [%s] data in single column"%train_property)
            data_values             = key_item_dict['value']
            months_as_keys          = data_values.keys()
            months_days             = []
            indexes                 = []
            values                  = []
            
            ## Merging all measurements in one time line
            for month in months_as_keys:
                days                = data_values[month].keys()
                for day in days:
                    months_day_key  = '%02d'%int(month)+'%02d'%int(day)
                    day_value       = data_values[month][day]
                    months_days.append({months_day_key:day_value})
            months_days = sorted(months_days)
            
            ## Convert all items into a single time series
            ##   Getting indexes and columns
            for sorted_month_day in months_days:
                indexes.append(sorted_month_day.keys()[0])
                values.append(int(sorted_month_day.values()[0]))
                
            ## Convert query results to a dataframe
            months_df               = pd.DataFrame(values, 
                                                   index=indexes, 
                                                   columns = ['Measurement'])
            
            ## Collapse all items in one column
            self.logger.debug("  1.2) Calculating 1st derivative")
            derivative          = np.diff(months_df['Measurement'])
            derivative          = np.insert(derivative.astype(float), 0, float('nan'))
            key                 = '1stDerivMeas'
            months_df[key]      = derivative
            
            ### item_name = 'months_'+item_id
            item_name = 'months_'+train_property
            self.logger.debug("  1.3) Collecting results for [%s]"%item_name)
            
            ## Passed data model to next state
            data_model['model'] = months_df
            self.logger.debug("-"*65)
                
        except Exception as inst:
            result = False
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            data_model['step_result'] = result
            output.update({'data_model' : data_model})

class ForecastModel():
    def __init__(self, **kwargs):
        '''Service task constructor'''
        
        try:
            # Initialising class variables
            self.component          = self.__class__.__name__
            self.logger             = Utilities.GetLogger(self.component)

            self.fsm                = PipeModel()
            self.machine            = None
            self.forecast_item      = None
            self.error_estimate     = None
            self.error_measurement  = None

            for key, value in kwargs.iteritems():
                if "forecast_item" == key:
                    self.forecast_item = value
 
            self.optimisation_steps=[0.001, 1, 5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, 10240, 20480, 40960, 81920, 163840, 327680]
            self.states=['step1', 'step2', 'step3', 'step4', 'step5', 'step6', 'step7']
            self.transitions = [
                { 'trigger': 'advance', 'source': 'step1', 'dest': 'step2', 'before': 'step1_format_data' },
                { 'trigger': 'advance', 'source': 'step2', 'dest': 'step3', 'before': 'step2_kalman_gain' },
                { 'trigger': 'advance', 'source': 'step3', 'dest': 'step4', 'before': 'step3_current_estimate' },
                { 'trigger': 'advance', 'source': 'step4', 'dest': 'step5', 'before': 'step4_normalise' },
                { 'trigger': 'advance', 'source': 'step5', 'dest': 'step6', 'before': 'step5_optimise' },
                { 'trigger': 'restart', 'source': 'step6', 'dest': 'step1' },
                { 'trigger': 'choose', 'source': 'step1', 'dest': 'step7', 'before': 'step6_choose_optimal' }
            ]
            
            # Initialize
            self.logger.debug("+ Creating state machine")
            self.machine = Machine(self.fsm, states=self.states, transitions=self.transitions, initial='step1')
            ## machine.add_ordered_transitions()
            ## machine.next_state()
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def ExecuteModel(self, input_data, items_id, initial_error_estim, error_optimisation):
        result = None
        try:
            ## Initialising input model
            output = {}
            output.update({'input_data': input_data})
            
            ### result_models = []
            data_model = {
                    'train_property':       'seeds',
                    'initial_error_estim':  initial_error_estim,
                    'optimisation_steps':   self.optimisation_steps,
                    'error_optimisation':   error_optimisation,
                    'step_result':          False,
                    'model':                None
            }
            
            
            ## Running model wihtout going to the last step: step6_choose_optimal
            for step in range(len(self.states)-2):
                
                if self.fsm.state == 'step1':
                    self.logger.debug( "  Initialising data model")
                    output.update({'data_model' : data_model})
                
                self.logger.debug( "  Executing state: [%s] in [%s] with initial error [%s]"%
                                   (self.fsm.state, input_data['hash'], str(initial_error_estim)))
                input  = {}
                input.update({'items_id':   items_id})
                input.update({'data_model': output['data_model']})
                input.update(output)
                output = {}
                previous_state = self.fsm.state
                
                ## Moving FSM state
                self.fsm.advance(output, **input)
                
                if not output['data_model']['step_result']:
                    self.logger.debug( "Error: Failure in state: "+self.fsm.state)
                    return

                if self.fsm.state == 'step6':
                    self.logger.debug( "  Interrupting FSM sequence")
                    self.logger.debug( "  Storing result data model")
                    result  = output['data_model']
                    self.fsm.restart()
                    self.logger.debug( "Restarting FSM to [%s]"%self.fsm.state)

        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return result

    def Run(self, forecast_item):
        '''
        '''
        result              = {}
        optimal_model       = {}
        error_optimisation  = None
        result_models       = []
        try:
            for input_data in forecast_item:
                #['leeches', 'seeds']
                self.logger.debug("Processing input data for [%s]"%input_data['name'])
                ## Optimising errors of initial error estimates
                for initial_error_estim in self.optimisation_steps:
                    ## Executing Kalman filter model
                    result = self.ExecuteModel(input_data, 
                                               ['seeds'], 
                                               initial_error_estim, 
                                               error_optimisation)
                    
                    if result is None:
                        print "Error: Invalid result set"
                        break
                    result_models.append(result)
                    break
                break
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return result

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
    taskAction.Run(forecast_item)

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

  call_task(options)
  