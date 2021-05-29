import os
import eurostat
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

#delete all csv files
def del_files():
    dir_name = os.getcwd()
    test = os.listdir(dir_name)

    for item in test:
        if item.endswith(".csv"):
            os.remove(os.path.join(dir_name, item))
    print("All csv files were deleted.")

#retrieve tourism data from eurostat
def retrieve_data(files, names, startPeriod, endPeriod, country):
    el_filter_pars = {'GEO': ['EL']}
    ot_filter_pars = {'GEO': [country]}

    for x in names:
        files.append(eurostat.get_sdmx_data_df(f'tour_occ_{x}', startPeriod, endPeriod, el_filter_pars, flags = True, verbose=True))
        files.append(eurostat.get_sdmx_data_df(f'tour_occ_{x}', startPeriod, endPeriod, ot_filter_pars, flags = True, verbose=True))

#data storage to mysql database
def store_data(files, names, country, user, passwd):
    #reset database and initiate connection
    engine = create_engine(f'mysql://{user}:{passwd}@localhost/eu_tour')
    engine.execute("DROP DATABASE eu_tour")
    engine.execute("CREATE DATABASE eu_tour")
    engine.execute("USE eu_tour")
    con = engine.connect()
    
    el_files=files[0::2]
    ot_files=files[1::2]
    for i,x in enumerate(names):
        el_files[i].to_sql(name=f'el_{x}', con=con, if_exists='replace')
        ot_files[i].to_sql(name=f'{country.lower()}_{x}', con=con, if_exists='replace')
    
    #connection closed
    con.close()

#extracting data to csv files
def extract_csv(files, names, country):
    el_files=files[0::2]
    ot_files=files[1::2]
    
    for i,x in enumerate(names):
        el_files[i].to_csv(f'el_{x}.csv', index=False)
        ot_files[i].to_csv(f'{country.lower()}_{x}.csv', index=False)

#plot data in graphs
def plot_data(files, names, country):
    for x in files:
        x=pd.DataFrame({"GEO": ['EL'], "2011": [2321765] })
        x['GEO']=pd.to_datetime(x['GEO'])
        x.plot(kind='line', x='GEO', y='2011')
        # x.plot(kind='scatter', x='GEO', y='2011')
        # plt.show()
        # x=x.astype(float)
        # x.plot()

#main function
def main():
    files=[]
    names = ['ninat', 'arnat', 'ninrmw', 'arnrmw']

    #asking the user to enter the variable values
    startPeriod = int(input("Enter start period: ") or 2008)
    endPeriod = int(input("Enter end period: ") or 2011)
    country = input("Enter country code: ") or 'FI'
    user = input("Enter MYSQL user: ") or 'root'
    passwd = input("Enter MYSQL passwd: ") or 'gtxm'

    del_files()
    retrieve_data(files, names, startPeriod, endPeriod, country)
    store_data(files, names, country, user, passwd)
    extract_csv(files, names, country)
    plot_data(files, names, country)
    

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, "\nSomething's wrong, exiting...")
