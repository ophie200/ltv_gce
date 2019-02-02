import time
from collections import namedtuple
import humanize
import pandas as pd
import numpy as np
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter

from google.cloud import bigquery

import google.cloud.logging

# Instantiates a logging client
client = google.cloud.logging.Client()
# Connects the logger to the root logging handler
client.setup_logging()

import logging

logging.info('start logging')
logger = logging.getLogger(__name__)

CONCURRENCY=8
CHUNK_SIZE=250000
write_lock = multiprocessing.Lock()

def estimate_clv_model(summary, model_penalizer=None):
  #set default values if they are not stated
  if model_penalizer is None:
    model_penalizer = 0

  # Building the Model using BG/NBD
  bgf = BetaGeoFitter(penalizer_coef=model_penalizer)
  bgf.fit(summary['frequency'], summary['recency'], summary['T'])

  # There cannot be non-positive values in the monetary_value or frequency vector
  summary_with_value_and_returns = summary[(summary['monetary_value']>0) & (summary['frequency']>0)]
  # Setting up Gamma Gamma model
  ggf = GammaGammaFitter(penalizer_coef = 0)
  ggf.fit(summary_with_value_and_returns['frequency'], summary_with_value_and_returns['monetary_value']) 

  # Refitting the BG/NBD model with the same data if frequency, recency or T are not zero length vectors
  if not (len(x) == 0 for x in [summary_with_value_and_returns['recency'],summary_with_value_and_returns['frequency'],summary_with_value_and_returns['T']]):
    bgf.fit(summary_with_value_and_returns['frequency'],summary_with_value_and_returns['recency'],summary_with_value_and_returns['T'])

  return [bgf, ggf]


# Will need later for calculating the alive probability of a customer
def calc_alive_prob(row, model):
    f = row['frequency']
    r = row['recency']
    t = row['T']
    return model.conditional_probability_alive(f,r,t)
