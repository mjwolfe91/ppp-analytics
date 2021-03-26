from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

conf = SparkConf().setAppName("PppCleaningData").setMaster("local")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def load_csv(file):
    df = sqlContext.read.csv(SparkFiles.get(file), header=True)
    return df

sc.addFile("Data/under150_all.csv")
sc.addFile("https://datahub.io/JohnSnowLabs/demographic-statistics-by-zip-code/r/demographic-statistics-by-zip-code-csv.csv")

denton_zips = ['76227', '75007', '75010', '76201', '76205', '76207', '76208', '76209', '76210', '75022', '75028', '76247', '75034', '76249',
               '75065', '76226', '75067', '75057', '75077', '75068', '76258', '76259', '76266', '75056', '76262']

if __name__ == '__main__':
    ppp_df = load_csv("under150_all.csv")
    ppp_df = ppp_df.select(col("Zip"), col("BusinessName"), col("LoanAmount")).filter(~ppp_df.Zip.isin(denton_zips))
    zip_df = load_csv("demographic-statistics-by-zip-code-csv.csv")
    zip_df = zip_df.select(col("Jurisdiction_Number"), col("Percent_Public_Assistance_Total")).filter(~zip_df.Jurisdiction_Number.isin(denton_zips))
    final_df = ppp_df.join(broadcast(zip_df), ppp_df.Zip == zip_df.Jurisdiction_Number, "left")
    final_df.show(truncate=False)