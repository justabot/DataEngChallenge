import pyspark.sql.functions as func
from pyspark.sql import SparkSession

print('\n\n\n')
print(' __i')
print('|---|    ')
print('|[_]|    ')
print('|:::|    ')
print('|:::|    ')
print('`\   \   ')
print('  \_=_\ ')
print('\n\n\n')
print('Location events analysis for Freckle Iot')
print('\n\n\n\n\n')
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
df = spark.read.json("./location-data-sample/*")
print('Gathering IDFA data...')
grouped_idfa = df.groupby(df.idfa) \
	.count() 
print('\n\n\n')
print('3a. Max Location events per IDFA...')
print(grouped_idfa \
	.sort("count", ascending=False) \
	.first())
print('\n\n\n')
print('3b. Min Location events per IDFA...')
print(grouped_idfa \
	.sort("count", ascending=True) \
	.first()) 
print('\n\n\n')
print('3c. Avg and Standard Deviation of Location events per IDFA...')
avg_idfa = grouped_idfa.agg(
	func.avg('count'),
	func.stddev('count'))
avg_idfa.show()
print('\n\n\n')
print('4. Creating geohashes...')
df_geo = df \
	.withColumn("thr_geohash", df.geohash.substr(1,9)) \
	.withColumn("one_geohash", df.geohash.substr(1,7)) \
	.withColumn("six_geohash", df.geohash.substr(1,6)) \
	.withColumn("irt_geohash", df.geohash.substr(1,5)) \
	.withColumn("tee_geohash", df.geohash.substr(1,4)) 
print('\n\n\n')
print('5a. Clusters of users who have been within 3.71 Meters of each other...')
df_geo.select(
	df_geo.idfa, df_geo.thr_geohash) \
	.where("thr_geohash NOT LIKE '%00000%'") \
	.groupby(df_geo.thr_geohash) \
	.agg(func.countDistinct('idfa')) \
	.sort("count(DISTINCT idfa)", ascending=False) \
	.show()
print('\n\n\n')
print('5b. Clusters of users who have been within 119 Meters of each other...')
df_geo.select(
	df_geo.idfa, df_geo.one_geohash) \
	.where("one_geohash NOT LIKE '%00000%'") \
	.groupby(df_geo.one_geohash) \
	.agg(func.countDistinct('idfa')) \
	.sort("count(DISTINCT idfa)", ascending=False) \
	.show()
print('\n\n\n')
print('5c. Clusters of users who have been within 610 Meters of each other...')
df_geo.select(
	df_geo.idfa, df_geo.six_geohash) \
	.where("six_geohash NOT LIKE '%00000%'") \
	.groupby(df_geo.six_geohash) \
	.agg(func.countDistinct('idfa')) \
	.sort("count(DISTINCT idfa)", ascending=False) \
	.show()
print('\n\n\n')
print('5d. Clusters of users who have been within 3.9 Kilometers of each other...')
df_geo.select(
	df_geo.idfa, df_geo.irt_geohash) \
	.where("irt_geohash NOT LIKE '%0000%'") \
	.groupby(df_geo.irt_geohash) \
	.agg(func.countDistinct('idfa')) \
	.sort("count(DISTINCT idfa)", ascending=False) \
	.show()
print('\n\n\n')
print('5e. Clusters of users who have been within 19.5 Kilometers of each other...')
df_geo.select(
	df_geo.idfa, df_geo.tee_geohash) \
	.where("tee_geohash NOT LIKE '%000%'") \
	.groupby(df_geo.tee_geohash) \
	.agg(func.countDistinct('idfa')) \
	.sort("count(DISTINCT idfa)", ascending=False) \
	.show()
print('\n\n\n')
print('6. Outputting cluster to parquet...')
df_geo.write.parquet("df_geo")
print("You've been great.")