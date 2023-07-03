# Load the parquet file
on_time_dataframe = spark.read.parquet('data/on_time_performance.parquet')
on_time_dataframe.registerTempTable("on_time_performance")

# Filter down to the fields we need to identify and link to a flight
flights = on_time_dataframe.rdd.map(
    lambda x: 
  {
      'Carrier': x.Carrier, 
      'FlightDate': x.FlightDate, 
      'FlightNum': x.FlightNum, 
      'Origin': x.Origin, 
      'Dest': x.Dest, 
      'TailNum': x.TailNum
  }
)
flights.first()

# Group flights by tail number, sorted by flight number, date, then origin/dest
flights_per_airplane = flights\
  .map(lambda record: (record['TailNum'], [record]))\
  .reduceByKey(lambda a, b: a + b)\
  .map(lambda tuple:
      {
        'TailNum': tuple[0], 
        'Flights': sorted(tuple[1], key=lambda x: (x['FlightNum'], x['FlightDate'], x['Origin'], x['Dest']))
      }
    )
flights_per_airplane.first()

# Save to Mongo
import pymongo_spark
pymongo_spark.activate()
flights_per_airplane.saveToMongoDB('mongodb://localhost:27017/agile_data_science.flights_per_airplane')
