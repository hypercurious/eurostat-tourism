import os
import eurostat
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

#delete all csv and png files
def del_files():
    dir_name = os.getcwd()
    test = os.listdir(dir_name)

    for item in test:
        if item.endswith(".csv") or item.endswith(".png"):
            os.remove(os.path.join(dir_name, item))
    print("All csv and png files were deleted.")

#retrieve tourism data from eurostat
def retrieve_data(files, names, startPeriod, endPeriod, country):
    el_filter_pars = {'GEO': ['EL'], 'UNIT': ['NR'], 'NACE_R2': ['I551-I553']}
    ot_filter_pars = {'GEO': [country], 'UNIT': ['NR'], 'NACE_R2': ['I551-I553']}

    for x in names:
        files.append(eurostat.get_sdmx_data_df(f'tour_occ_{x}', startPeriod, endPeriod, el_filter_pars, flags = True, verbose=True))
        files.append(eurostat.get_sdmx_data_df(f'tour_occ_{x}', startPeriod, endPeriod, ot_filter_pars, flags = True, verbose=True))
    
    for i,x in enumerate(files):
        files[i]=x[x.columns.drop(list(x.filter(regex='OBS')))]

#store data to mysql database
def store_data(files, names, country, user, passwd):
    #reset database and initiate connection
    engine = create_engine(f'mysql://{user}:{passwd}@localhost')
    engine.execute("DROP DATABASE IF EXISTS eu_tour")
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

#export data in csv files
def export_csv(files, names, country):
    el_files=files[0::2]
    ot_files=files[1::2]
    
    for i,x in enumerate(names):
        el_files[i].to_csv(f'el_{x}.csv', index=False)
        ot_files[i].to_csv(f'{country.lower()}_{x}.csv', index=False)

#plot data in graphs and save the graphs
def plot_data(files, names, country, startPeriod, endPeriod):
    el_files=files[0::2]
    ot_files=files[1::2]
    for i,x in enumerate(el_files):
        x=x.set_index('C_RESID')
        x=x[x.columns[5:9]].astype(float)
        x.plot.bar(rot=0, title=f'el_{names[i]}')
        plt.savefig(f'el_{names[i]}.png',bbox_inches='tight')
        plt.show()
    for i,x in enumerate(ot_files):
        x=x.set_index('C_RESID')
        x=x[x.columns[5:9]].astype(float)
        x.plot.bar(rot=0, title=f'{country.lower()}_{names[i]}.png')
        plt.savefig(f'{country.lower()}_{names[i]}.png',bbox_inches='tight')
        plt.show()

#main function
def main():
    files=[]
    names = ['ninat', 'arnat']

    #asking the user to enter the variable values
    startPeriod = int(input("Enter start period: ") or 2008)
    endPeriod = int(input("Enter end period: ") or 2011)
    country = input("Enter country code: ") or 'FI'
    user = input("Enter MYSQL user: ") or 'root'
    passwd = input("Enter MYSQL passwd: ") or 'gtxm'

    #calling the functions
    del_files()
    retrieve_data(files, names, startPeriod, endPeriod, country)
    store_data(files, names, country, user, passwd)
    export_csv(files, names, country)
    plot_data(files, names, country, startPeriod, endPeriod)
    

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e, "\nSomething's wrong, exiting...")
