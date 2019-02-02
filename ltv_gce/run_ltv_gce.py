from . import estimate_clv_model


def main():
  
  # model error params
  margin_of_error = .01
  confidence_level = 2.576
  sigma = .5
  min_sample_size = int(math.ceil(math.pow(confidence_level,2) * sigma*(1-sigma) / math.pow(margin_of_error,2)))
  print min_sample_size

  qry = ("SELECT client_id, frequency, recency, T, monetary_value FROM ltv.summary ORDER BY RAND() LIMIT {}").format(min_sample_size)
  print(qry)

  # initialize BigQuery to get random subsample of min_sample_size
  client = bigquery.Client()
  query_job = client.query(qry)
  df_random_sample = query_job.to_dataframe() # no need to go through query_job.result()
  df_random_sample.head(3)
  
  # estimate ltv model
  [bgf, ggf] = estimate_clv_model(df_random_sample)

  # 

  # initialize storage
  storage_client = storage.Client()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
