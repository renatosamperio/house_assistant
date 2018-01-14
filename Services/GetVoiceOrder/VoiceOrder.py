#!/usr/bin/env python

from __future__ import division

import re
import sys
import pyaudio
import logging
import grpc
import pprint

from google.cloud import speech
from google.cloud.speech import enums
from google.cloud.speech import types
from six.moves import queue

from Utils import Utilities
from optparse import OptionParser

LOG_NAME = 'VoiceOrder'
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)

class MicrophoneStream(object):
  """Opens a recording stream as a generator yielding the audio chunks."""
  RATE = 16000
  CHUNK = int(RATE / 10)  # 100ms

  def __init__(self, butler_words):
    self.component    = self.__class__.__name__
    self.logger       = Utilities.GetLogger(self.component)
    
    self._rate = self.RATE
    self._chunk = self.CHUNK

    # Create a thread-safe buffer of audio data
    self._buff = queue.Queue()
    self.closed = True
    self.butler_words = butler_words

  def __enter__(self):
    self._audio_interface = pyaudio.PyAudio()
    self._audio_stream = self._audio_interface.open(
      format=pyaudio.paInt16,
      channels=1, rate=self._rate,
      input=True, frames_per_buffer=self._chunk,
      stream_callback=self._fill_buffer,
    )

    self.closed = False

    return self

  def __exit__(self, type, value, traceback):
    self._audio_stream.stop_stream()
    self._audio_stream.close()
    self.closed = True
    # Signal the generator to terminate so that the client's
    # streaming_recognize method will not block the process termination.
    self._buff.put(None)
    self._audio_interface.terminate()

  def _fill_buffer(self, in_data, frame_count, time_info, status_flags):
    """Continuously collect data from the audio stream, into the buffer."""
    self._buff.put(in_data)
    return None, pyaudio.paContinue

  def generator(self):
    while not self.closed:
      chunk = self._buff.get()
      if chunk is None:
        return
      data = [chunk]

      while True:
        try:
          chunk = self._buff.get(block=False)
          if chunk is None:
              return
          data.append(chunk)
        except queue.Empty:
          break

      yield b''.join(data)

  def listen_print_loop(self, responses, service):
    try:
      num_chars_printed = 0
      for response in responses:
        if not response.results:
          continue
        result = response.results[0]
        if not result.alternatives:
          continue

        transcript = result.alternatives[0].transcript
        overwrite_chars = ' ' * (num_chars_printed - len(transcript))

        if not result.is_final:
          sys.stdout.write(transcript + overwrite_chars + '\r'+'\n')
          sys.stdout.flush()

          num_chars_printed = len(transcript)

        else:
          if self.butler_words is None:
            raise Exception('No butler words were found!')
          
          final_phrase = (transcript + overwrite_chars).lstrip()
          butler_words_re = "|".join(self.butler_words)+'|exit|quit'+ '\b'

          isMatch = re.search(butler_words_re, transcript, re.I)
          if isMatch:
            keyword = isMatch.group(0)
            #print("=== keyword: %s"%keyword)
            action = final_phrase[len(keyword):].lstrip().strip()
            #print("=== action: %s"%action)
            self.logger.debug('Got order => [%s: %s]'%(keyword, action))
            if service is not None:
              itemsCall = {'caller': keyword, 'action':action}
              service.notify("updated", 'success', items=itemsCall)
            break

          num_chars_printed = 0
    except grpc._channel._Rendezvous:
      self.logger.debug(' Time expired, keeping microphone [ON]')
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

class Recorder(object):
  def __init__(self, **kwargs):
    self.service      = None
    self.butler_words = None
    
    self.component    = self.__class__.__name__
    self.logger  = Utilities.GetLogger(self.component)
    
    for key, value in kwargs.iteritems():
      if "butler_words" == key:
        self.butler_words = value
      elif "service" == key:
        self.service = value

  def WaitOrder(self):
    language_code = 'en-US'  # a BCP-47 language tag

    client              = speech.SpeechClient()
    config              = types.RecognitionConfig(
      encoding=enums.RecognitionConfig.AudioEncoding.LINEAR16,
      sample_rate_hertz=MicrophoneStream.RATE,
      language_code=language_code)
    streaming_config    = types.StreamingRecognitionConfig(
      config=config, interim_results=True)

    with MicrophoneStream(self.butler_words) as stream:
      audio_generator   = stream.generator()
      requests          = (types.StreamingRecognizeRequest(audio_content=content)
                           for content in audio_generator)

      responses         = client.streaming_recognize(streaming_config, requests)
      stream.listen_print_loop(responses, self.service)
      
  def Run(self):
    try:
      isTimerOff = True
      while isTimerOff:
        try:
          self.WaitOrder()
        except grpc._channel._Rendezvous:
          continue
    except Exception as inst:
      Utilities.ParseException(inst, logger=self.logger)

def call_method(options):
  try:
    args = {}
    args.update({'butler_words': options.butler_words})
    recorder = Recorder(**args)
    recorder.Run()
  except Exception as inst:
    Utilities.ParseException(inst)

if __name__ == '__main__':
    logger = Utilities.GetLogger(LOG_NAME, useFile=False)
    
    myFormat = '%(asctime)s|%(name)30s|%(message)s'
    logging.basicConfig(format=myFormat, level=logging.DEBUG)
    logger        = Utilities.GetLogger(LOG_NAME, useFile=False)
    logger.debug('Logger created.')
    
    usage = "usage: %prog butler_words=Word1 butler_words=Word2"
    parser = OptionParser(usage=usage)
    parser.add_option('--butler_words',
                        type="string",
                        action='append',
                        default=None,
                        help='Input torrent search title')
    
    (options, args) = parser.parse_args()
    
    if options.butler_words is None:
      parser.error("Missing required option: --butler_words='Alfred'")
      sys.exit()
    call_method(options)

