# minimal postgres db connection code - from https://pynative.com/python-postgresql-tutorial/

import psycopg2
import pandas as pd
import getpass 

def get_pw():
    try: 
        pw = getpass.getpass(prompt='Postgres Password: ') 
    except Exception as error: 
        print('ERROR', error) 
    else: 
        return(pw) 

def get_catalog_df(user,pw,host,port,db):
    try:
        connection = psycopg2.connect(user = user,
                                      password = pw,
                                      host = host,
                                      port = port,
                                      database = db)

        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print("updated HERE")
        print ( connection.get_dsn_parameters(),"\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        # fetchone(), fetchmany(), fetcthall()
        print("You are connected to - ", record,"\n")
        cursor.execute("SELECT table_name FROM information_schema.tables where table_schema='public';")
        record_list = cursor.fetchall()
        i = 0
        table_list = []
        for item in record_list:
            print("record ",str(i)," is:", item,"\n")
            table_list = table_list + list(item)
            i = i+1
        print("table_list is ",table_list)
        cursor.execute("SELECT column_name FROM information_schema.columns where table_name = 'tables' LIMIT 10")
        record_col_spec = cursor.fetchall()
        i = 0
        table_table_cols_list = []
        for item_col in record_col_spec:
            print("record cols from tables table ",str(i)," is:", item_col,"\n")
            table_table_cols_list = table_table_cols_list + list(item_col)
            i = i+1
        print("table_table_cols_list is ",table_table_cols_list)
        # create a dataframe with details about the columns
        cursor.execute("SELECT column_name, data_type, table_name FROM information_schema.columns where table_schema='public' order by table_name")
        record_col_details = cursor.fetchall()
        col_details_list = []
        for item_col in record_col_details:
            print("record cols from tables table ",str(i)," is:", item_col,"\n")
            # table_table_cols_list = table_table_cols_list + list(item_col)
            col_details_list.append(item_col)
            i = i+1
        df = pd.DataFrame(col_details_list, columns =['column_name', 'data_type', 'table_name'])
        #print(df.head(40))

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    finally:
        #closing database connection.
            if(connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")
            return(df)
            
            
def main():
  print("Hello World!")
  pw = get_pw()
  print("Got pw")
  # get dataframe with db catalog details
  catalog_df = get_catalog_df("postgres",pw,"127.0.0.1","5432","dvdrental")
  print(catalog_df.head(40))
  
  
   
if __name__== "__main__":
  main()

print("next thing")            