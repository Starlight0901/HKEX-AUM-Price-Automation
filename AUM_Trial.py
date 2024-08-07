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

create_volume_table_sql = """
CREATE TABLE IF NOT EXISTS Volume_Turnover_AUM_Data (
    date DATE,
    volume_9008 FLOAT,
    turnover_9008 FLOAT,
    volume_9042 FLOAT,
    turnover_9042 FLOAT,
    volume_9439 FLOAT,
    turnover_9439 FLOAT
);
"""

cursor.execute(create_volume_table_sql)

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

def scrape_aum_volume(url):
  # Initialize the WebDriver
  driver = webdriver.Chrome(options=chrome_options)
  driver.get(url)
  time.sleep(5)  # Wait for the page to load

# Get the page source and parse it with BeautifulSoup
  page_source = driver.page_source
  soup = BeautifulSoup(page_source, 'html.parser')

  div_right = soup.find('div', class_='left_list_item list_item_op')

  if div_right:
    aum_volume = div_right.find('dt', class_='col_volume').text.strip() if div_right.find('dt', class_='col_volume') else "N/A"
    aum_volume_value = aum_volume.strip() if aum_volume else "N/A"
    if aum_volume_value.endswith("K"):
      aum_volume_value = float(aum_volume_value[:-1])*1000  # Remove "K" and convert to thousands
# -----------------------------------------------------------------------------------------------------------------------------------------------------------

    aum_turnover = div_right.find('dt', class_='col_turnover').text.strip() if div_right.find('dt', class_='col_turnover') else "N/A"
    aum_turnover_value = aum_turnover.strip() if aum_volume else "N/A"
    if aum_turnover_value.startswith("US$"):
      aum_turnover_value = aum_turnover_value[3:]  # Remove "US$"
    if aum_turnover_value.endswith("K"):
      aum_turnover_value = float(aum_turnover_value[:-1])*1000  # Remove "K" and convert to thousands

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

      
    return aum_volume_value, aum_turnover_value
  else:
    print("Div not found. Please check the HTML structure and update the div selector.")
          
  # Close the Selenium driver
  driver.quit()

def save_to_aum_db(latest_date, BOS_HSK_9008, HGI_9042, CAM_9439):
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

def save_to_vol_turn_db(latest_date, volume_9008, turnover_9008, volume_9042, turnover_9042, volume_9439, turnover_9439):
  # Check if a record for the current date already exists
  cursor.execute(f"SELECT COUNT(*) FROM Volume_Turnover_AUM_Data WHERE date = '{latest_date}'")
  record_exists = cursor.fetchone()[0] > 0

  if record_exists:
    # Update the existing record
    cursor.execute(f"""
        UPDATE Volume_Turnover_AUM_Data
        SET volume_9008 = {volume_9008}, turnover_9008 = {turnover_9008}, volume_9042 = {volume_9042}, turnover_9042 = {turnover_9042},  volume_9439 = {volume_9439}, turnover_9439 = {turnover_9439}
        WHERE date = '{latest_date}'
    """)
  else:
    # Insert new data into Snowflake
    cursor.execute(f"""
        INSERT INTO Volume_Turnover_AUM_Data (date, volume_9008, turnover_9008, volume_9042, turnover_9042, volume_9439, turnover_9439) 
        VALUES ('{latest_date}', {volume_9008}, {turnover_9008}, {volume_9042}, {turnover_9042}, {volume_9439}, {turnover_9439})
    """)


def web_scrape():
  # scrape AUM values with date
  BOS_HSK_9008, Date_9008 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9008&sc_lang=en")
  HGI_9042, Date_9042 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9439&sc_lang=en")
  CAM_9439, Date_9439 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9042&sc_lang=en")

  # scrape volume and turnover values
  volume_9008, turnover_9008 = scrape_aum_volume(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9008&sc_lang=en")
  volume_9042, turnover_9042 = scrape_aum_volume(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9439&sc_lang=en")
  volume_9439, turnover_9439 = scrape_aum_volume(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9042&sc_lang=en")
      
  # Find the latest date
  latest_date = max(Date_9008, Date_9042, Date_9439)

  save_to_aum_db(latest_date, BOS_HSK_9008, HGI_9042, CAM_9439)
  save_to_vol_turn_db(latest_date, volume_9008, turnover_9008, volume_9042, turnover_9042, volume_9439, turnover_9439)

web_scrape()

conn.commit()
cursor.close()
conn.close()

