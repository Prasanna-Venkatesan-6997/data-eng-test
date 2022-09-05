!pip install apache_beam

import apache_beam as beam

with beam.Pipeline() as pipe:
  ip = (pipe
      |beam.io.ReadFromText("/content/transactions.csv", skip_header_lines=True)
      |beam.Map(lambda x:x.split(","))  
      |beam.Filter(lambda x:x[1]>"20.00")
      |beam.Filter(lambda x:x[0]>"2009-12-31")
      |beam.Map(lambda x:(x[0], float(x[1])))
      |beam.CombinePerKey(sum)
      |beam.io.WriteToText("/content/transactions.csv", header=["date","total_amount"])
)