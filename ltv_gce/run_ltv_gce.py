from . import estimate_clv_model


def main():
  
  start = time.clock()

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


  # pull distinct (hash) client_id from ut_clients_daily with non-zero search history
  #client_ids = util.query_vertica(DISTINCT_CLIENTS_QUERY)
  
  # Could be a derived number from internal set knoledge
  # (i.e. ( Total RAM - Safety constant ) / chunk required RAM
  pool = multiprocessing.Pool(processes=CONCURRENCY)

  logger.info("Starting real work with {} workers in chunks of {} clients".format(CONCURRENCY, CHUNK_SIZE))

  workers = []
  
  #### change this to pull data from bg that fits into memory
  #### dask?
  
  for group in grouper(client_ids, CHUNK_SIZE):
        # No point in multiprocessing if we aren't concurrent
        if CONCURRENCY == 1:
            process_chunk(fo,group)
        # Add to our async work queue
        else:
            workers.append(pool.apply_async(process_chunk, (fo, group,)))

  # All work has been submitted, plan for shutdown
  logger.debug("Closing work queue")
  pool.close()

  # Wait for all workers to complete their work cleanly
  logger.debug("Waiting for all workers to complete cleanly")
  logger.debug("Total jobs : {}".format(len(workers)))
  pool.join()

  #field_order = ['client_id','frequency','recency','customer_age','avg_session_value','predicted_searches_14_days','alive_probability','predicted_clv_12_months','historical_searches','historical_clv','total_clv','days_since_last_active','user_status','calc_date']
  #util.load_into_vertica(fo,vertica_output_table_name,delimiter='|',field_order = field_order)

  elapsed = (time.clock() - start)
  print('runtime: %f', elapsed)
  logger.debug('Query vertica complete')

  
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
