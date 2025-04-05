import requests
from bs4 import BeautifulSoup
import datetime
import time
import sqlite3
import os
import logging

def daterange(start_date, end_date):
    """
    generate all dates in a selected month
    """
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

def access_the_site(test, date, new_month):
    """
    access the site if possible
    """
    device = []; daily_hits = []
    date = date.strftime("%Y%m%d")
    link = 'http://web.archive.org/web/'+ date + '/https://www.gsmarena.com/'
    with requests.Session() as s:
        try:
            response = requests.request("GET", link)
            url = response.url
            print("Accessing the {0} website.".format(url))
            if date == str(url)[27:35]:
                logging.info("Current date: %s" %(date))
                device, daily_hits = parse_data(url)
                time.sleep(1)
            else:
                date = str(url)[27:35] # proceed to the next day
                if date < new_month:
                    logging.info("Current date: %s" %(date))
                    device, daily_hits = parse_data(url)
                    time.sleep(5)
                    test = False                  
        except:
            print("Cannot load the {0} website.".format(link))
        return test, date, device, daily_hits

def parse_data(url):
    """
    scrape data for a whole month
    """
    temp_download = []
    device = []; daily_hits = []
    
    page = requests.get(url)
    if page.status_code == 200:
        soup = BeautifulSoup(page.content, 'html.parser')
        line = 0
        for i in soup.findAll('tr'): # <tr> tags
            temp_download.append(i)
            if line >= 2 and line < 12:
                device.append(str(temp_download[line].find('nobr'))[6:-7])
                daily_hits.append(str(temp_download[line].find('td', attrs={'headers': 'th3c'}))[19:-5])
            line += 1
    return device, daily_hits

def connect_to_db():
    """
    connect to the database
    """
    path = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(path, "topTenByDailyInterest.db")
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute('DROP table IF exists topTen')
        cur.execute("""CREATE table IF not exists topTen(
                    'id' integer not null primary key autoincrement,
                    'date' date not null,
                    'device' text not null,
                    'daily_hits' numeric not null)""")
        return conn, cur
    except:
        logging.warning("I am not able to connect to the database.")

def disconnect(conn, cur):
    """
    close a connection to the database
    """
    cur.close()
    conn.close()

def store_to_db(conn, cur, date, device, daily_hits):
    """
    store scraped data to the database
    """
    for j in range(len(device)):
        insert_data = 'INSERT into topTen (date, device, daily_hits) values (?, ?, ?);'
        cur.execute(insert_data, [date, device[j], daily_hits[j]])
        conn.commit()
        logging.info("Successfully committed changes to the database.")

if __name__ == '__main__':
    logging.basicConfig(filename='log_file.log', filemode='w' ,format='%(message)s', level=logging.INFO)
    # logging.basicConfig(format='%(message)s', level=logging.INFO, handlers=[logging.FileHandler('log_file.log', mode='w'), logging.StreamHandler()])

    start_date = datetime.date(2020, 1, 1) # set a date
    end_date = datetime.date(2020, 2, 1)
    
    new_month = end_date.strftime("%Y%m%d")
    test = True
    conn, cur = connect_to_db()
    
    for date in daterange(start_date, end_date):
        if not test:
            test = True
            continue
        test, date, device, daily_hits = access_the_site(test, date, new_month)
        if date >= new_month:
            break
        store_to_db(conn, cur, date, device, daily_hits)
    disconnect(conn, cur)
