import pandas as pd 
import requests 
from bs4 import BeautifulSoup 
import sqlite3
from datetime import datetime
log_file = "log_file.txt" 
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
tabel_attribute = 'tabel'
def extract(url):
    #url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    count = 0
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('table')

    if len(tables) >= 3:
        table = tables[2]  # Table 3 is at index 2 (zero-based index)
        tbody = table.find('tbody')
        rows = tbody.find_all('tr')

        df = pd.DataFrame(columns=["Country/Territory", "UN region", "IMF", "IMF YEAR" , "World Bank", "WORLD_BANK YEAR", "United Nations" ,"UNITED_NATION YEAR " ])

        for row in rows:
         if count < 50 :
            cols = row.find_all(['th', 'td'])
            if len(cols) >= 8:  # Assuming at least 5 columns are present in each row
                data_dict = {
                    "Country/Territory": cols[0].text.strip(),
                    "UN region": cols[1].text.strip(),
                    "IMF": cols[2].text.strip(),
                    "IMF YEAR": cols[3].text.strip(),
                    "World Bank": cols[4].text.strip(),
                    "WORLD_BANK YEAR": cols[5].text.strip(),
                     "United Nations": cols[6].text.strip(),
                    "UNITED_NATION YEAR ": cols[7].text.strip()
                }
                df2 = pd.DataFrame(data_dict , index=[0])
                df = pd.concat([df,df2], ignore_index=True)
                tab = pd.DataFrame(df)
                count += 1
        return tab
    else:
        print("Table 3 not found.")

# df = extract(url)
#print(df)
def transform(df):
 
    # here lamda x applay changes to IMF , WORLD BANK , UNITED
    # Remove Commas and Dollar Signs, Convert to Float, and Divide by 1 Billion
 df[['IMF', 'World Bank', 'United Nations']] = \
    (df[['IMF', 'World Bank', 'United Nations']].apply(lambda x: x.str.replace(',', '').str.replace('$', '').astype(float))) / 1000

# Round to 2 Decimal Places
 df[['IMF', 'World Bank', 'United Nations']] = \
    df[['IMF', 'World Bank', 'United Nations']].round(2)
 
 return df



# transform(df)

# print(df)

csv_path='world_economy.csv'
def load_csv(df , csv_path):
   df.to_csv(csv_path , index=[0])


# load_csv(df , csv_path)

connection = sqlite3.connect('world_ecomony.db')
tabel_name = 'World_econmoy'
file_path = 'world_economy.csv' 
def load_to_db(df , sql_connection , tabel_name):
   Attributes = ["Country/Territory", "UN region", "IMF", "IMF YEAR" , "World Bank", "WORLD_BANK YEAR", "United Nations" ,"UNITED_NATION YEAR "]
   df3 = pd.read_csv(df , names=Attributes)
   df3.to_sql(tabel_name ,sql_connection , if_exists='replace' , index=False)


# load_to_db(file_path ,connection ,tabel_name )


def run_query(query_statement , connection):
   query_exection = pd.read_sql(query_statement,connection)
   print(query_exection)

# run_query(f"select IMF from {tabel_name}" , connection)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    time_stamp = now.strftime(timestamp_format)
    with open (log_file , 'a') as log :
       log.write(time_stamp + "," + message + "\n")

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
  
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data =  extract(url)
  
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
  
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data)
print("Transformed Data") 
print(transformed_data) 
  
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
  
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_csv(transformed_data , csv_path)
  
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
  
log_progress("Load To DataBase phase Started") 
load_to_db(file_path ,connection ,tabel_name )
  
# Log the completion of the Loading process 
log_progress("Load TO DataBase phase Ended") 
# Log the completion of the ETL process 

log_progress("Extract from  DataBase phase Started") 

run_query(f"select IMF from {tabel_name}" , connection)
  
# Log the completion of the Loading process 
log_progress("Extract from  DataBase phase Ended") 
log_progress("ETL Job Ended") 

   
   
   





