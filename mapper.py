
query_list = []
sql_list = []
name_lines = open('name.txt','r').readlines()
new_name_list = []
data_type_list = []
for line in name_lines:
    line = line.replace('(',' ').replace(')',' ').replace(',', ' ').split()
    new_name_list.append(line[0])  
    data_type_list.append(line[1])

column_lines = open('column.txt','r')
colum_list = []

for line in column_lines:
    line = line.replace('[','').replace(']',' ').split()
    colum_list.append(line[0])


# .withColumn("target_practice_id", get_column(src_df, "TargetPracticeID", IntegerType())) \

if len(new_name_list) and len(colum_list) and len(data_type_list):
    print("LOOOKS GOOOD")
    # IntegerType, DateType, BinaryType, StringType,DecimalType,FloatType, ShortType
    for i in range(len(new_name_list)):
        if data_type_list[i] == "VARCHAR":
            data_type_list[i] = "StringType"
        elif data_type_list[i] == "DATE":
            data_type_list[i] = "DateType" 
        elif data_type_list[i] == "INTEGER":
            data_type_list[i] = "IntegerType"
        elif data_type_list[i] == "VARBINARY":
            data_type_list[i] = "BinaryType"
        elif data_type_list[i] == "SMALLINT":
            data_type_list[i] = "ShortType"
        elif data_type_list[i] == "DECIMAL":
            data_type_list[i] = "DecimalType"
        elif data_type_list[i] == "BIGINT":
            data_type_list[i] = "LongType"
        elif data_type_list[i] == "REAL":
            data_type_list[i] = "FloatType"
        elif data_type_list[i] == "TIMESTAMP":
            data_type_list[i] = "TimestampType"       
        else:
            raise ("somthing missing")

        line = '.withColumn("' + new_name_list[i] + '",' + ' get_column(src_df, "' +  colum_list[i] + '", ' + data_type_list[i] + '())) \\'
        query_list.append(line)

query_list.append('.select([') 
for i in new_name_list:
    line = '\t"' + i + '",'
    query_list.append(line)

query_list.append('])')

query_list.append('spark.sql("CREATE DATABASE IF NOT EXISTS udw")')

query_list.append('spark.sql("""')
query_list.append('\tCREATE TABLE IF NOT EXISTS udw.practice (')

for i in range(len(data_type_list)):
    if data_type_list[i] == "StringType":
        data_type_list[i] = "STRING"
    elif data_type_list[i] == "DateType":
        data_type_list[i] = "DATE" 
    elif data_type_list[i] == "IntegerType":
        data_type_list[i] = "INTEGER"
    elif data_type_list[i] == "BinaryType":
        data_type_list[i] = "BINARY"
    elif data_type_list[i] == "ShortType":
        data_type_list[i] = "SHORT"
    elif data_type_list[i] == "DecimalType":
        data_type_list[i] = "DECIMAL"
    elif data_type_list[i] == "LongType":
        data_type_list[i] = "LONG"
    elif data_type_list[i] == "FloatType":
        data_type_list[i] = "FLOAT"
    elif data_type_list[i] == "TimestampType":
        data_type_list[i] = "TIMESTAMP"       
    else:
        raise ("somthing missing")
    line = '\t\t' + new_name_list[i] + ' ' + data_type_list[i] + ','
    query_list.append(line)

with open(r'output.txt', 'w') as fp:
    for item in query_list:
        item = item
        # write each item on a new line
        fp.write("%s\n" % item)
    print('Done')