#!pip install apache_beam

#I used the above command to install apache_beam 

import apache_beam as beam

with beam.Pipeline() as pipe:         #Naming the pipeline as pipe
  ip = (pipe
      |"Reading the file from gs" >> beam.io.ReadFromText("gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv", skip_header_lines=True)
      |"Splitting the columns" >> beam.Map(lambda x:x.split(","))  
      |"filtering the transaction_amount greater than 20" >> beam.Filter(lambda x:x[1]>"20.00")
      |"filtering out date before 2010" >> beam.Filter(lambda x:x[0]>"2009-12-31")
      |"changling the datatype of transaction_amount to float" >> beam.Map(lambda x:(x[0], float(x[1])))
      |"summing the transaction_amount by date" >> beam.CombinePerKey(sum)
      |"writing output to csv file with the columns date and total_amount" >> beam.io.WriteToText("/output/results.csv", header=["date","total_amount"])
)
