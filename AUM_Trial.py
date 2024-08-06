import snowflake.connector
import os
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
        user='STARLIGHT9026',
        password='Starlight@9026',
        account='lj38853.europe-west4.gcp',
        database='AUM_Database',
        schema='AUM_schema'
    )

cursor = conn.cursor() # cursor object

cursor.execute("USE DATABASE AUM_Database")
cursor.execute("USE SCHEMA AUM_schema")

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

    aum_value = aum.strip() if aum else "N/A"
    if aum_value.startswith("US$"):
      aum_value = aum_value[3:]  # Remove "US$"
    if aum_value.endswith("M"):
        aum_value = float(aum_value[:-1])  # Remove "M" and convert to float


    aum_date = div.find('dt', class_='col_aum_date').text.strip() if div.find('dt', class_='col_aum_date') else "N/A"
    aum_date = aum_date.strip() if aum_date else "N/A"
    aum_date = aum_date.replace('as at ', '').strip()
    aum_date = aum_date.replace('(', '').replace(')', '').strip()
    aum_date = datetime.strptime(aum_date, '%d %b %Y').strftime('%Y-%m-%d')
      
    return aum_value, aum_date
  else:
    print("Div not found. Please check the HTML structure and update the div selector.")

  # Close the Selenium driver
  driver.quit()

def web_scrape():
  BOS_HSK_9008, Date_9008 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9008&sc_lang=en")
  HGI_9042, Date_9042 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9439&sc_lang=en")
  CAM_9439, Date_9439 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9042&sc_lang=en")

  # Find the latest date
  latest_date = max(Date_9008, Date_9042, Date_9439)

  # Check if a record for the current date already exists
  cursor.execute(f"SELECT COUNT(*) FROM new_AUM_Data WHERE date = '{latest_date}'")
  record_exists = cursor.fetchone()[0] > 0

  if record_exists:
    # Update the existing record
    cursor.execute(f"""
        UPDATE new_AUM_Data
        SET aum_9008 = {BOS_HSK_9008}, aum_9042 = {HGI_9042}, aum_9439 = {CAM_9439}
        WHERE date = '{latest_date}'
    """)
  else:
    # Insert new data into Snowflake
    cursor.execute(f"""
        INSERT INTO new_AUM_Data (date, aum_9008, aum_9042, aum_9439) 
        VALUES ('{latest_date}', {BOS_HSK_9008}, {HGI_9042}, {CAM_9439})
    """)


web_scrape()

conn.commit()
cursor.close()
conn.close()

