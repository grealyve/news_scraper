import requests
from bs4 import BeautifulSoup
import psycopg2 as psyc
import time

databasename = "*****"   #Your database name
user_name = "*****"      #Your user name
password = "*****"           #Your password
host_ip = "127.0.0.1"
host_port = "5432"

connection = psyc.connect(database = databasename,
                        user = user_name,
                        password = password,
                        host = host_ip,
                        port = host_port)

try:
    connection.autocommit = True
    cursor = connection.cursor()
    query_create_table = """CREATE TABLE IF NOT EXISTS data(
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    info TEXT NOT NULL,
    author TEXT NOT NULL,
    date TEXT NOT NULL,
    source TEXT NOT NULL
    )
    """
    cursor.execute(query_create_table)

except(Exception, psyc.Error) as error:
    print("PostgreSql veri tabanına bağlanırken veya tabloyu oluştururken bir hata oluştu : " + error)
    connection = None


class Scrape:

    websites = {
        "bleepingcomputer":"https://www.bleepingcomputer.com/",
        "thehackernews": "https://thehackernews.com/",
        "threatpost": "https://threatpost.com/"}

    def isExists(self, title):                                      # Checks if data is already exist in DB.
        cursor.execute("SELECT title FROM data WHERE title = %s", (title,))
        return cursor.fetchone() is not None

    def insertToDB(self, title, info, date, author, source):
            try:
                query_insert = (f"INSERT INTO data (title, info, author, date, source) VALUES ('{title}', '{info}', '{author}', '{date}', '{source}')")
                cursor.execute(query_insert)
            except(Exception, psyc.Error) as error:
                print("Datayı eklerken bir hata oluştu : ", error)
    
    def insertData(self):
        for website in self.websites:
            if(website == "bleepingcomputer"):
                url = self.websites[website]
                html = requests.get(url, allow_redirects=True, headers= {'User-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}).content
                soup = BeautifulSoup(html, "html.parser")
                result = soup.find("div", {"class":"bc_latest_news"}).find("ul").find_all("li")
                source = "bleepingcomputer"

                for li in result:
                    if (li.find_all('div', {"class":"bc_latest_news_text"}) != []):
                        title = li.find_all("div")[1].h4.text
                        info = li.find_all("div")[1].p.text
                        author = li.find_all("div")[1].ul.find_all("li")[0].text
                        date = li.find_all("div")[1].ul.find_all("li")[1].text
                        title = title.replace("'", "\"")
                        info = info.replace("'", "\"")

                        if(self.isExists(title)==True):
                            continue
                        else:
                            self.insertToDB(title, info, date, author, source)

            elif(website == "thehackernews"):
                url = self.websites[website]
                html = requests.get(url, allow_redirects=True, headers= {'User-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}).content
                soup = BeautifulSoup(html, "html.parser")
                result = soup.find_all("div", {"class":"clear home-right"})
                source = "thehackernews"

                for div in result:
                    h2 = div.find("h2")
                    if (h2.find("img")==None):                  #This command to check if there is any commercial
                        title = h2.text
                        info = div.find_all("div")[1].text
                        temp = div.find("div", {"class":"item-label"}).text
                        date = temp[1:-1].split("\ue804")[0]
                        author = temp[1:-1].split("\ue804")[1]
                        title = title.replace("'", "\"")
                        info = info.replace("'", "\"")

                        if(self.isExists(title)==True):
                            continue
                        else:
                            self.insertToDB(title, info, date, author, source)
                    else:
                        continue
                    
            elif(website == "threatpost"):
                url = self.websites[website]
                html = requests.get(url, allow_redirects=True, headers= {'User-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}).content
                soup = BeautifulSoup(html, "html.parser")
                result = soup.find_all("article", {"class":"c-card c-card--horizontal--half@md c-card--horizontal@lg c-card--horizontal--flat@md"})
                source = "threatpost"

                for article in result:
                    title = article.find("div").find_all("div")[1].h2.text
                    info = article.find("p").text
                    date = article.find("time").text
                    author = article.find("a", {"class":"c-card__author-name"}).text
                    title = title.replace("'", "\"")
                    info = info.replace("'", "\"")

                    if(self.isExists(title)==True):
                        continue
                    else:
                        self.insertToDB(title, info, date, author, source)

                    
if __name__ == "__main__":
    sc = Scrape()
    while True:
        sc.insertData()
        curr_time = time.strftime("%H:%M:%S", time.localtime())
        print("Data has been inserted at :", curr_time)
        time.sleep(3600)

connection.close()