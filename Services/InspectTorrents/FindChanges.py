#!/usr/bin/python

import json
import time
import sys, os
import json
import logging
import imp
import re
import csv, codecs, cStringIO
import pprint
import random
import datetime

import pandas as pd
import numpy as np
from scipy import stats #stats.mode

from optparse import OptionParser, OptionGroup
from collections import Counter
from transitions import Machine
from slackclient import SlackClient

from Utils import Utilities
from Utils import Similarity
from Utils import MongoAccess
from ForecastModel import ForecastModel

logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
logging.getLogger("boto").setLevel(logging.WARNING)
#logging.getLogger("imdbpie.imdbpie").setLevel(logging.WARNING)

reload(sys)
sys.setdefaultencoding('utf8')

class FindChanges():
    def __init__(self, **kwargs):
        '''Service task constructor'''
        
        try:
            # Initialising class variables
            self.component        = self.__class__.__name__
            self.logger           = Utilities.GetLogger(self.component)

            ## Adding local variables
            self.database         = None
            self.collection       = None
            self.action_words     = None
            self.not_searching    = False
            self.database         = None
            self.torrent_terms    = None
            self.forecast_item    = None
            self.db_handler       = None
            self.slack_channel    = None
            self.last_days        = 5

            # Generating instance of strategy  
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "last_days" == key:
                    self.last_days = value
                elif "collection" == key:
                    self.collection = value
                elif "slack_channel" == key:
                    self.slack_channel = value
                    self.logger.debug("  +   Setting up channel [%s]"%self.slack_channel)
                elif "forecast_item" == key:
                    self.forecast_item = value
                elif "with_changes" == key:
                    if value is not None:
                        self.with_changes = self.LoadTerms('list_termx.txt')
                elif "list_term" == key:
                    if value is not None:
                        self.with_changes = self.LoadTerms(value)

            if self.forecast_item is None:
                ## Setting item started for reporting to device action
                self.logger.debug("  + Generating database [%s] in [%s] collections"% 
                                    (self.database, self.collection))
                self.db_handler = MongoAccess(debug=False)
                self.db_handler.connect(self.database, self.collection)
            
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def LoadTerms(self, fileName):
        try:
            with open(fileName, 'r') as file:
                return file.read().strip().split()
                
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)

    def FindChange(self, items_id, record):
        '''
        Finds if data has changes by looking into time series models
        with requested items ID (leeches and seeds). The changes 
        considers the 1st derivative of every record in DB.
        '''
        changed_items                   = None
        newest_item                     = None
        try:
            for item in items_id:
                ##self.logger.debug("      Looking for changes in [%s] of [%s]", item, record['hash'])
                months                  = record[item]['value']
                months_as_keys          = record[item]['value'].keys()
                
                ## Find if latest values are zeros
                ##   Organise months and days from different months
                ##   in a list of sorted month_days
                months_days             = []
                for month in months_as_keys:
                    days                = record[item]['value'][month].keys()
                    for day in days:
                        months_day_key  = '%02d'%int(month)+'%02d'%int(day)
                        day_value       = record[item]['value'][month][day]
                        months_days.append({months_day_key:day_value})
                        
                ##   Reverser search for find latest days
                months_days = sorted(months_days, reverse=True)
                
                ##   Find if last items are in zero
                zero_items = 0
                for measure in months_days:
                    if int(measure.values()[0]) != 0:
                        break
                    zero_items += 1

                ## Ignoring items with more than 3 days in zeros
                if zero_items > 3:
                    #self.logger.debug("        IGNORE [%s] with [%d] latest days in zeros"%
                    #              (record["hash"], zero_items))
                    continue

                ## Look for values without missing values
                months_df               = pd.DataFrame(months)
                for col_index in months_df:
                    months_se            = pd.Series(months_df[col_index])
                    months_df[col_index] = pd.to_numeric(months_se)
                    
                    ## Find if the indexes are numerically sequential
                    months_np_array     = months_se.index.astype(np.int16)
                    months_arr_seq      = np.sort(months_np_array)
                    sequencesArray      = Utilities.IsNpArrayConsecutive(months_arr_seq)
                    isIncremental       = len(sequencesArray)==1
                    
                    ## Do not continue if it is not sequential
                    if not isIncremental:
                        ## self.logger.debug("      Index array of [%s] is not sequential for [%s]", item, record['hash'])
                        
                        #self.logger.debug("        IGNORE [%s] because is not sequential"%(record['hash']))
                        continue
                    
                    ## Checking latest hits
                    if len(months_days)<=5:
                        self.logger.debug("   -> NEWEST: item [%s] with [%s] in less than 5 days"%(item, record["hash"]))
                        newest_item = record
                    
                    if len(months_days)<=10:
                        self.logger.debug("   -> RECENT: item [%s] with [%s] in less than 10 days"%(item, record["hash"]))
                    
                    ## Drop first element as it is does not has 1st derivative
                    first_derivative    = months_df[col_index].diff().iloc[1:]
                    
                    ## Check items that derivative is not zero
                    has_zeros           = first_derivative[first_derivative >0]
                    if has_zeros.count() > 0:
                        self.logger.debug("  Something changed in [%s] of item [%s]", 
                                          item, record['hash'])
                        ## Add items if they are not included
                        changed_items = record
                        return
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return changed_items, newest_item
    
    def SaveNameWords(self, all_names):
        try:
            most_common = all_names.most_common()
            with open('common_words.csv', 'wb') as csvfile:
                writer = csv.writer(csvfile)
                for word, qty in most_common:
                    word_udata=word.encode('ascii', 'ignore').decode('ascii')
                    row = [word_udata, qty]
                    writer.writerow(row)
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
            
    def GetMovies(self, items_id):
        '''
        Finds if there is a change time series by using first derivative
        '''
        all_changed_items   = []
        all_newest_items    = []
        try:
            if self.forecast_item is not None:
                return
            db_size = self.db_handler.Size()
            self.logger.debug("  + Getting [%d] records from [%s]"%(db_size, self.collection))
            
            ## Find all available records
            records         = self.db_handler.Find()
            counter         = 1
            
            ## Defined local function to find changes in time series
            self.logger.debug("      Looking for changes in collection [%s]",self.collection)
#             all_names = Counter()
            start_time = time.time()
            super_count = 0
            for record in records:
                ## Collecting names
#                 processed_name = re.sub('[\(\)\{\}<>][-]', ' ', record['name'].lower())
#                 splitted_names = processed_name.split()
                ##splitted_names = [re.sub('[\(\)\{\}<>][-]', ' ', x.lower()) for x in splitted_names]
#                 print "=== n:" , record['name']
#                 print "=== p:", processed_name
#                 print "=== s:", splitted_names
                ## Counting words in each item name
#                 all_names = all_names + Counter(splitted_names)
                
                ## Finding words in list of torrent terms
#                 new_name = ' '.join(list(set(splitted_names) - set(self.torrent_terms)))
#                 print "=== N:", new_name
#                 print "="*100
                
                ## Double records

                ## Collecting leeches and seeds
                changed_item, newest_item = self.FindChange(items_id, record)
                if changed_item is not None:
                    all_changed_items.append(changed_item)
                if newest_item is not None:
                    all_newest_items.append(newest_item)
                counter += 1
                
#                 if super_count > 100:
#                     break
                super_count += 1
            elapsed_time = time.time() - start_time
            self.logger.debug("  + Collected [%d] records in [%s]"%(counter, str(elapsed_time)))
#             self.logger.debug("  + Saving most popular words in names");
#             self.SaveNameWords(all_names)
                
        except Exception as inst:
            Utilities.ParseException(inst, logger=self.logger)
        finally:
            return all_changed_items, all_newest_items

    def PostNew(self, newest_items):
        ''' '''
        def list_values(key):
            try:
                key_item    = new_item[key]['value']
                item_month  = key_item.keys()[0]
                item_days   = key_item[item_month].keys()
                lst_values  = [key_item[item_month][day] for day in item_days]
                lst_values  = list(map(int, lst_values))
                array_values=np.array(lst_values)
                return array_values
            except Exception as inst:
              Utilities.ParseException(inst, logger=logger)
        
        def clean_sentence(sentence):
            try:
                ## Removing special works from torrent
                splitted        = sentence.strip().split()
                new_sentence    = []
                for token in splitted:
                    if token.lower() not in self.with_changes:
                        new_sentence.append(token)
                new_sentence    = ' '.join(new_sentence)
                
                return new_sentence
            except Exception as inst:
              Utilities.ParseException(inst, logger=logger)
        
        def get_imdb_best_title(torrent_info, comparator):
            try:
                from operator import itemgetter
                from imdbpie import Imdb
                
                imdb            = Imdb()
                updated_imdb    = []
                imdb_selected   = []
                item_selected   = {}
                year_found      = None
                torrent_title   = torrent_info['torrent_title']
                ###print "\t~~~~> before:\t\t", torrent_title
                
                ## Using torrent title until year,
                ##   only if torrent title has a year
                title_has_year  = re.match(r'.*([1-3][0-9]{3})', torrent_title)
                if title_has_year is not None:
                    year_found  = title_has_year.group(1)
                    
                    ## Adding torrent title year
                    torrent_info.update({'year':year_found})
                    ###print "\t~~~~> year_found:\t", year_found
                    #after = torrent_title.replace(title_has_year.group(1),'')
                    splitted = torrent_title.split(year_found)[0]
                else:
                    splitted = torrent_title
                torrent_info['torrent_title'] = splitted
                ###print "\t~~~~> splitted:\t\t", splitted
                ###counter = 0
                ##print "= "*20
                
                ## Getting IMDB information
                imdb_data       = imdb.search_for_title(splitted)
                self.logger.debug("+ Getting IMDB [%s] items"%(str(len(imdb_data))))
                
                ## Pre-selecting only IMDB titles that 
                ##   look very similar to torrent title
                for imdb_item in imdb_data:
                    score = comparator.score(splitted, imdb_item['title'])
                    year_matches    = year_found == imdb_item['year']
                    item_type       = 'feature' == imdb_item['type']
                    
                    ## Adding only very similar titles
                    if score > 0.98:
                        imdb_item.update({'score':score})
                        imdb_item.update({'year_matches':year_matches})
                        updated_imdb.append(imdb_item)
                    
                    ###print "\t\t[",counter,"]   \t\t", score,"\t[" ,imdb_item['title'],"]\t", year_matches, "\t", item_type
                    ###counter += 1
                    ##pprint.pprint(imdb_item)
                    ##print "= "*20
                    
                ## Sorting IMDB retrieved items by similarity score
                sorted_imdb = sorted(updated_imdb, key=itemgetter('score'), reverse=True) 
                ##print "~"*40
                ##pprint.pprint(sorted_imdb)
                ##print "~"*40
                
                
                ## Checking if torrent year matches, otherwise 
                ##   provide only feature type IMDB items
                better_item_not_found           = False
                for imdb_item in sorted_imdb:
                    item_added                  = False
                    new_item                    = {}
                    if imdb_item['year_matches']:
                        #print imdb_item
                        #print "= "*20
                        better_item_not_found   = True
                        item_added              = True
                    elif not better_item_not_found and 'feature' == imdb_item['type']:
                        #print imdb_item
                        #print "@ "*20
                        item_added              = True
                        
                    ## Retrieving additional IMDB information
                    ##   and adding item
                    if item_added:
                        imdb_id                 = imdb_item['imdb_id']
                        title_info              = imdb.get_title(imdb_id)
                        
                        ###print "+ "*40, 'title_info'
                        ###pprint.pprint(title_info)
                        ###print "+ "*40
                        
                        imdb_image_url          = ''
                        if 'image' in title_info['base'].keys():
                            imdb_image_url          = title_info['base']['image']['url']
                        else:
                            self.logger.debug("-   Image URL not found")
                            self.logger.debug("-   Looking in similarities for images...")
                            for similarity_item in title_info['similarities']:
                                itemKeys        = similarity_item.keys()
                                if 'image' in itemKeys:
                                    imageKey    = similarity_item['image'].keys()
                                    if 'url' in imageKey:
                                        imdb_image_url = similarity_item['image']['url']
                                        self.logger.debug("      Using image from similarities")
                                        break
                        imdb_raiting            = ''
                        if 'rating' in title_info['ratings'].keys():
                            imdb_raiting        = str(title_info['ratings']['rating'])
                        else:
                            self.logger.debug("-   Raiting not found")

                        imdb_plot               = ''
                        if 'outline' in title_info['plot'].keys():
                            imdb_plot           = title_info['plot']['outline']['text']
                        else:
                            self.logger.debug("-   Plot not found")

                        
                        imdb_title_url          = title_info['base']['id']

                        imdb_item.update({'raiting':    imdb_raiting})
                        imdb_item.update({'plot':       imdb_plot})
                        imdb_item.update({'image_url':  imdb_image_url})
                        imdb_item.update({'title_url':  'http://www.imdb.com/'+imdb_title_url})
                        
                        
                        #new_item.update({'imdb_info':imdb_item})
                        
                        imdb_selected.append(imdb_item)
                
                item_selected.update({'imdb_info':imdb_selected})
                item_selected.update({'torrent_info':torrent_info})
            except Exception as inst:
              Utilities.ParseException(inst, logger=logger)
            finally:
                return item_selected
            
        try:
            ## Getting Slack and IMDB clients
            slack_token         = os.environ["SLACK_API_TOKEN"]
            slack_client        = SlackClient(slack_token)
            comparator          = Similarity.Similarity()
            for new_item in newest_items:
                
                ## Getting torrent data
                name            = new_item['name']
                clean_name      = clean_sentence(name)
                link            = new_item['link']
                size            = new_item['size']
                seeds           = list_values('seeds')
                leeches         = list_values('leeches')
                #pprint.pprint(new_item)
                
                ## Torrent information
                torrent_info    = {
                    'torrent_title':    clean_name,
                    'torrent_link':     link,
                    'seeds': {
                        'mean':         seeds.mean(),
                        'stdev':        seeds.std()
                    },
                    'leeches': {
                        'mean':         leeches.mean(),
                        'stdev':        leeches.std()
                    }
                }
                
                ## Searching for IMDB info
                imdb_selected   = get_imdb_best_title(torrent_info, comparator)


# {'imdb_info': [{'image_url': u'https://ia.media-imdb.com/images/M/MV5BMTY0NjU4NjE4Nl5BMl5BanBnXkFtZTgwNjk0ODY5MjI@._V1_.jpg',
#                 u'imdb_id': u'tt7131870',
#                 'plot': u"China's deadliest special forces operative settles into a quiet life on the sea. When sadistic mercenaries begin targeting nearby civilians, he must leave his newfound peace behind and return to his duties as a soldier and protector.",
#                 'raiting': 6.3,
#                 'score': 1.0,
#                 u'title': u'Wolf Warrior 2',
#                 u'type': u'feature',
#                 u'year': u'2017',
#                 'year_matches': True}],
#  'torrent_info': {'leeches': {'mean': 1016.0, 'stdev': 0.0},
#                   'seeds': {'mean': 6425.0, 'stdev': 0.0},
#                   'torrent_link': u'http://limetorrents.cc/Wolf-Warrior-2-2017-BDRip(AVC)-by-KinoHitHD-torrent-10555891.html',
#                   'torrent_title': u'Wolf Warrior 2 2017 by',
#                   'year': u'2017'}}


                print "==> imdb_selected:"
                pprint.pprint(imdb_selected)
                
                label_time_now  = datetime.datetime.now().strftime("Found on the %d of %B, %Y")
                label_name      = str(imdb_selected['torrent_info']['torrent_title'])
                label_seeds     = str(int(imdb_selected['torrent_info']['seeds']['mean']))
                label_leeches   = str(int(imdb_selected['torrent_info']['leeches']['mean']))
                label_torr_url  = imdb_selected['torrent_info']['torrent_link']
                
                attachments             = []
                for imdb_item in imdb_selected['imdb_info']:
                    fields              = []
                    seeds           = {
                                        "title": "Seeds",
                                        "value": label_seeds,
                                        "short": True
                                    }
                    fields.append(seeds)
                    leeches         = {
                                        "title": "Leeches",
                                        "value": label_leeches,
                                        "short": True
                                    }
                    fields.append(leeches)
                    if len(imdb_item['raiting'])>0:
                        raiting_field= {
                                        "title": "Raiting",
                                        "value": str(imdb_item['raiting']),
                                        "short": True
                                    }
                        fields.append(raiting_field)
                    
                    imdb_title      = str(imdb_item['title'])
                    imdb_year       = str(imdb_item['year'])
                    imdb_title_url  = str(imdb_item['title_url'])
                    imdb_image_url  = str(imdb_item['image_url'])
                    imdb_plot       = str(imdb_item['plot']).encode('utf-8').strip()
                    
                    if imdb_item['year'] is None:
                        torrent_year        = str(imdb_selected['torrent_info']['year'])
                        if torrent_year is not None or torrent_year != '':
                            attachment_title= imdb_title + " - " +torrent_year
                    else:
                        attachment_title = imdb_title + " - " + imdb_year
                    attachement_item    = { "title":        attachment_title,
                                            "title_link":   imdb_title_url,
                                            "image_url":    imdb_image_url,
                                            
                                            "author_name": "Lime Torrents Crawler",
                                            "author_icon": "https://cdn.appmus.com/images/4bfa32737acbaaa618ef471b37099ad7.jpg",
                                            "author_link":  label_torr_url,
                                            
                                            "text":         imdb_plot,
                                            "pretext":      label_time_now,
                                            
                                            "footer":       "IMDB",
                                            "footer_icon":  'https://cdn4.iconfinder.com/data/icons/socialmediaicons_v120/48/imdb.png',
                                            
                                            "fields":       fields,
                                          }
                    attachments.append(attachement_item)
                
                slack_client.api_call(
                  "chat.postMessage",
                  channel="test",
                  text="",
                  as_user=True,
                  attachments=attachments
                )
                print "-"*40
        except Exception as inst:
          Utilities.ParseException(inst, logger=logger)
LOG_NAME = 'TaskTool'

def call_task(options):
  ''' Command line method for running sniffer service'''
  try:
    
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Calling task from command line')
    
    args = {}
    args.update({'database':        options.database})
    args.update({'collection':      options.collection})
    args.update({'list_term':       options.list_term})
    
    if options.slack_file is not None:
        newest_items    = options.slack_file
        args.update({'list_term':   options.list_term})
        taskAction      = FindChanges(**args)
        taskAction.PostNew(newest_items)
    elif options.forecast_file is None:
        
        taskAction              = FindChanges(**args)
        changes, newest_items   = taskAction.GetMovies(['seeds'])
        #pprint.pprint(changes)
        #print "="*80
        taskAction.PostNew(newest_items)
        #pprint.pprint(newest_items)
        #print "="*80
    else:
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

  slack_parser = OptionGroup(parser, "Slack options",
              "Used to publish data in Slack")
  slack_parser.add_option('--slack_channel',
              type="string",
              action='store',
              default=None,
              help='Input a valid slack channel')
  
  analyse_parser = OptionGroup(parser, "Torrent analyse tools",
              "Used to analyse data")
  analyse_parser.add_option("--plot",
               action="store_true",
               default=False,
               help='Plot items')

  forecast_parser = OptionGroup(parser, "Torrent forecast modelling",
              "Used to alarm torrent experiences")
  forecast_parser.add_option('--forecast_file',
              type="string",
              action='store',
              default=None,
              help='Input file with sample data')
  forecast_parser.add_option('--slack_file',
              type="string",
              action='store',
              default=None,
              help='Post results in slack')
  forecast_parser.add_option('--list_term',
              type="string",
              action='store',
              default=None,
              help='List of terms to ignore')

  parser.add_option_group(forecast_parser)
  parser.add_option_group(analyse_parser)
  
  (options, args) = parser.parse_args()
  
  if options.slack_file is not None:     
    if options.list_term is None:
         parser.error("Missing required option: --list_term='valid_file_path'")
    with open(options.slack_file, 'r') as file:
        slack_file = json.load(file)
        options.slack_file = slack_file
        options.database = 'limetorrents'
        options.collection = 'movies'
      
  elif options.forecast_file is None:
      if options.database is None:
        parser.error("Missing required option: --database='valid_db_name'")
        sys.exit()
      if options.collection is None:
        parser.error("Missing required option: --collection='valid_collection_name'")
        sys.exit()
  ##print options
  call_task(options)
