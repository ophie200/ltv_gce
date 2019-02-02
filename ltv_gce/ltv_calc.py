import argparse
import logging

import pandas as pd
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter

from google.cloud import bigquery

# initialize storage
from google.cloud import storage
storage_client = storage.Client()

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


def estimate_model():
  margin_of_error = .01
  confidence_level = 2.576
  sigma = .5
  min_sample_size = int(math.ceil(math.pow(confidence_level,2) * sigma*(1-sigma) / math.pow(margin_of_error,2)))
  print min_sample_size

  qry = ("SELECT client_id, frequency, recency, T, monetary_value FROM ltv.summary ORDER BY RAND() LIMIT {}").format(min_sample_size)
  print(qry)

  # initialize BigQuery
  client = bigquery.Client()
  query_job = client.query(qry)
  df = query_job.to_dataframe() # no need to go through query_job.result()
  df.head(3)

  # get random subsample of min_sample_size from bq
  #clv_random_sample = clvSummary.orderBy(F.rand()).limit(min_sample_size)

  [bgf, ggf] = estimate_clv_model(clv_random_sample_pd)
  # save model to gs
  #clv_model_fn = "s3://net-mozaws-prod-us-west-2-pipeline-analysis/nawong/ltv_model/bgf_small_size_" + date.today().strftime("%Y%m%d") + '.pkl'
  clv_model_fn = "bgf_small_size_" + date.today().strftime("%Y%m%d") + '.pkl'
  bgf.save_model(clv_model_fn, save_data=False, save_generate_data_method=False)
  ggf.save_model("ggf_small_size_" + date.today().strftime("%Y%m%d") + '.pkl', save_data=False, save_generate_data_method=False)
  # NOTE: should really cross-validate predictive power


def run():
  estimate_model()



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
