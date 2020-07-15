# minimal postgres db connection code - from https://pynative.com/python-postgresql-tutorial/

import psycopg2
import pandas as pd
import getpass 
import yaml
import pickle
import logging
import os

logging.getLogger().setLevel(logging.WARNING)
logging.warning("logging check")

def get_config(config_file):
    current_path = os.getcwd()
    print("current directory is: "+current_path)

    path_to_yaml = os.path.join(current_path, config_file)
    print("path_to_yaml "+path_to_yaml)
    try:
        with open (path_to_yaml, 'r') as c_file:
            config = yaml.safe_load(c_file)
        return(config)
    except Exception as e:
        print('Error reading the config file')
    
        


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
            logging.debug("record "+str(i)+" is:"+str(item)+"\n")
            table_list = table_list + list(item)
            i = i+1
        print("table_list is ",table_list)
        cursor.execute("SELECT column_name FROM information_schema.columns where table_name = 'tables' LIMIT 10")
        record_col_spec = cursor.fetchall()
        i = 0
        table_table_cols_list = []
        for item_col in record_col_spec:
            logging.debug("record cols from tables table "+str(i)+" is:"+str(item_col)+"\n")
            table_table_cols_list = table_table_cols_list + list(item_col)
            i = i+1
        print("table_table_cols_list is ",table_table_cols_list)
        # create a dataframe with details about the columns
        cursor.execute("SELECT column_name, data_type, table_name FROM information_schema.columns where table_schema='public' order by table_name")
        record_col_details = cursor.fetchall()
        col_details_list = []
        for item_col in record_col_details:
            logging.debug("record cols from tables table "+str(i)+" is:"+str(item_col)+"\n")
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
            
def get_path():
    ''' get the path for data files '''
    rawpath = os.getcwd()
    # data is in a directory called "data" that is a sibling to the directory containing the notebook
    path = os.path.abspath(os.path.join(rawpath, '..', 'data'))
    return(path)

def save_catalog_df(df,pickle_name,modifier):
    ''' persist a dataframe as a pickle file with the specified filename and path '''
    file_name = pickle_name+'_'+modifier+'.pkl'
    pickle_path = os.path.join(get_path(),file_name)
    logging.debug("output file_name is "+str(pickle_path))
    df.to_pickle(pickle_path)
            


def main():
  print("Hello World!")
  config = get_config('scrape_db_catalog_config.yml')
  pw = get_pw()
  print("Got pw")
  # get dataframe with db catalog details, using parameters from config file
  catalog_df = get_catalog_df(config['general']['user'],pw,config['general']['host'],config['general']['port'],config['general']['database'])
  # save the df as a pickle file
  save_catalog_df(catalog_df,config['files']['output_pickle_name'],config['files']['modifier'])
  print(catalog_df.head(40))
  
  
   
if __name__== "__main__":
  main()

print("next thing")            