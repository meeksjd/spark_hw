
file_path = "/data/weather/2010"

inTextData = spark.read.format("csv").option("header", "true").option("delimiter","\t").load(file_path)
rdd1 = inTextData.rdd
rdd2 = rdd1.map(lambda x: str(x).split('=')[1])
rdd3 = rdd2.map(lambda x: ' '.join(x.split()))
rdd4 = rdd3.map(lambda x: x[1:-2])
rdd4.saveAsTextFile(file_path+'temp')
newInData = spark.read.csv(file_path+'temp',header=False,sep=' ')
cleanData = newInData.drop('_c1','_c4','_c6','_c8','_c10','_c12','_c14')
cleanData = cleanData.withColumnRenamed('_c0','STN').withColumnRenamed('_c2','YEARMODA')\
                    .withColumnRenamed('_c3','TEMP').withColumnRenamed('_c5','DEWP')\
                    .withColumnRenamed('_c7','SLP').withColumnRenamed('_c9','STP')\
                    .withColumnRenamed('_c11','VISIB').withColumnRenamed('_c13','WDSP')\
                    .withColumnRenamed('_c15','MXSPD').withColumnRenamed('_c16','GUST')\
                    .withColumnRenamed('_c17','MAX').withColumnRenamed('_c18','MIN')\
                    .withColumnRenamed('_c19','PRCP').withColumnRenamed('_c20','SNDP')\
                    .withColumnRenamed('_c21','FRSHTT')