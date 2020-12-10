# File location and type
file_location = "/FileStore/tables/diamonds-1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","


data=sc.textFile(file_location)
header = data.first() #extract header
data = data.filter(lambda line: line != header)


# Check your result
#for i in data.take(5) : print (i)

print('Taille du fichier : ',data.count())

non_empty_lines = data.filter(lambda x: len(x.split(delimiter))>1 )
#non_empty_lines = non_empty_lines.filter(lambda line: type(line.split(delimiter)[7])!= 'str')

resultat1=non_empty_lines.map(lambda x: (x.split(delimiter)[2],x.split(delimiter)[7])).reduceByKey(lambda x,y:max(int(x),int(y))).sortBy(lambda x: x[-1]).collect()

display(resultat1)
