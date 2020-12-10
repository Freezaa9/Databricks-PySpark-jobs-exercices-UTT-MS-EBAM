# File location and type
file_location = "/FileStore/tables/coronavirus_commercants_parisiens_livraison_a_domicile.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ";"

data=sc.textFile(file_location)
#data=spark.read.format("CSV").option("header","true").load(file_location)

print('Taille du fichier : ',data.count())

non_empty_lines = data.filter(lambda x: len(x.split(";"))>1 )
resultat1=non_empty_lines.map(lambda x: (x.split(';')[2],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: -x[-1]).collect()

#display(resultat1)

#resultat2=non_empty_lines.map(lambda x: (x.split(';')[3],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: -x[-1]).collect()


#resultat3=non_empty_lines.map(lambda x: (x.split(';')[4],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x: -x[-1]).collect()
display(resultat3)