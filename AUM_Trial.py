import snowflake.connector
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
from datetime import datetime
import time
import pandas as pd
#import datetime

# Snowflake connection
conn = snowflake.connector.connect(
    user='SNOWFLAKE_USER',
    password='SNOWFLAKE_PASSWORD',
    account='SNOWFLAKE_ACCOUNT'
)

cursor = conn.cursor() # cursor object

cursor.execute("USE DATABASE New_HKEX_DATA")
cursor.execute("USE SCHEMA My_Schema")

#  create the table if it doesn't exist
create_table_sql = """
CREATE TABLE IF NOT EXISTS new_AUM_Data (
    date DATE,
    aum_9008 FLOAT,
    aum_9042 FLOAT,
    aum_9439 FLOAT
);
"""

cursor.execute(create_table_sql)

# Set up Selenium options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode for automation
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

def scrape_aum(url):

  # Initialize the WebDriver
  driver = webdriver.Chrome(options=chrome_options)
  driver.get(url)
  time.sleep(5)  # Wait for the page to load

  # Get the page source and parse it with BeautifulSoup
  page_source = driver.page_source
  soup = BeautifulSoup(page_source, 'html.parser')

  # Locate the specific div containing the AUM data
  div = soup.find('div', class_='left_list_item list_item_as')

  if div:
      # Extract the data
      aum = div.find('dt', class_='col_aum').text.strip() if div.find('dt', class_='col_aum') else "N/A"

      aum_value = aum.text.strip() if aum else "N/A"
      if aum_value.startswith("US$"):
        aum_value = aum_value[3:]  # Remove "US$"
      if aum_value.endswith("M"):
        aum_value = float(aum_value[:-1])  # Remove "M" and convert to float
      
      #current_date = datetime.datetime.now().strftime("%Y-%m-%d")
      aum_date = div.find('dt', class_='col_aum_date').text.strip() if div.find('dt', class_='col_aum_date') else "N/A"

      date = aum_date.text.strip() if aum_date else "N/A"
    
      
      return aum_value, date
  else:
      print("Div not found. Please check the HTML structure and update the div selector.")

  # Close the Selenium driver
  driver.quit()


def web_scrape():
  BOS_HSK, Date = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9008&sc_lang=en")
  HGI, Date = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9439&sc_lang=en")
  CAM, Date = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9042&sc_lang=en")

  csv_file = r"C:\Users\ASUS\Desktop\SQRR\AUM\all_aum_data.csv"
  try:
    df = pd.read_csv(csv_file)
  except FileNotFoundError:
    df = pd.DataFrame(columns=['Date', 'Bosera HashKey Bitcoin ETF (9008)', 'ChinaAMC Bitcoin ETF (9042)', 'Harvest Bitcoin ETF (9439)'])

  # Check if the current date is already in the DataFrame
  if Date not in df['Date'].values:
    data = pd.DataFrame({
      'Date': [Date],
      'Bosera HashKey Bitcoin ETF (9008)': [BOS_HSK],
      'ChinaAMC Bitcoin ETF (9042)': [CAM],
      'Harvest Bitcoin ETF (9439)': [HGI]
    })

    # Append the data
    df = pd.concat([df, data], ignore_index=True)

    # Save the updated DataFrame to the CSV file
    df.to_csv(csv_file, index=False)
    print("Update successfully.")
  else:
    print("Data already recorded.")
  print(df)

web_scrape()

conn.commit()
cursor.close()
conn.close()

