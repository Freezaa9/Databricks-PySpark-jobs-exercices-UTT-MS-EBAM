# File location and type
file_location = "/FileStore/tables/survey.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

data=sc.textFile(file_location)
header = data.first() #extract header
data = data.filter(lambda line: line != header)

def mafonction(x):
  val=x.split(delimiter)[2].upper().replace('"','').strip()
  if val=='M':
    val='MALE'
  elif val=='F':
    val='FEMALE'
  elif val=='WOMAN':
    val='FEMALE'
  elif val=='FEMALE (TRANS)':
    val='FEMALE'
  return val,1
  

print('Taille du fichier : ',data.count())

non_empty_lines = data.filter(lambda x: len(x.split(delimiter))>1 )
resultat1=non_empty_lines.map(mafonction).reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[-1]).collect()

display(resultat1)