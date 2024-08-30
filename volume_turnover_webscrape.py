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
        user='SQRR',
        password='Panama9-Latticed8-Chamomile5-Scary3-Krypton9',
        account='zj20388.us-east4.gcp',
        database='HKEX',
        schema='HKEX'
    )

cursor = conn.cursor() # cursor object

# Specify the database and schema to use
cursor.execute("USE DATABASE HKEX")
cursor.execute("USE SCHEMA HKEX")


#  create the table if it doesn't exist
create_table_sql = """
CREATE TABLE IF NOT EXISTS VOLUME_TURNOVER_AUM_DATA (
    date DATE,
    volume_9008 FLOAT,
    volume_9042 FLOAT,
    volume_9439 FLOAT,
    turnover_9008 FLOAT,
    turnover_9042 FLOAT,
    turnover_9439 FLOAT
);
"""

cursor.execute(create_table_sql)

#conn.commit()
#cursor.close()
#conn.close()

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
  div_left = soup.find('div', class_='left_list_item list_item_as')

  if div_left:

    aum_date = div_left.find('dt', class_='col_aum_date').text.strip() if div_left.find('dt', class_='col_aum_date') else "N/A"
    aum_date = aum_date.strip() if aum_date else "N/A"
    aum_date = aum_date.replace('as at ', '').strip()
    aum_date = aum_date.replace('(', '').replace(')', '').strip()
    aum_date = datetime.strptime(aum_date, '%d %b %Y').strftime('%Y-%m-%d')

# -----------------------------------------------------------------------------------------------------------------------------------------------------------
    # Extract the data
    aum = div_left.find('dt', class_='col_aum').text.strip() if div_left.find('dt', class_='col_aum') else "N/A"

    aum_value = aum.strip() if aum else "N/A"
    if aum_value.startswith("US$"):
      aum_value = aum_value[3:]  # Remove "US$"
    if aum_value.endswith("M"):
        aum_value = float(aum_value[:-1])  # Remove "M" and convert to float

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

    div_right = soup.find('div', class_='left_list_item list_item_op')

    if div_right:

      aum_volume = div_right.find('dt', class_='col_volume').text.strip() if div_right.find('dt', class_='col_volume') else "N/A"

    aum_volume_value = aum_volume.strip() if aum else "N/A"
    if aum_volume_value.endswith("K"):
        aum_volume_value = float(aum_volume_value[:-1])*1000  # Remove "K" and convert to thousands

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

    aum_turnover = div_right.find('dt', class_='col_turnover').text.strip() if div_right.find('dt', class_='col_turnover') else "N/A"

    aum_turnover_value = aum_turnover.strip() if aum else "N/A"
    if aum_turnover_value.startswith("US$"):
      aum_turnover_value = aum_turnover_value[3:]  # Remove "US$"
    if aum_turnover_value.endswith("K"):
        aum_turnover_value = float(aum_turnover_value[:-1])*1000  # Remove "K" and convert to thousands

# -----------------------------------------------------------------------------------------------------------------------------------------------------------


    return aum_value, aum_date, aum_volume_value, aum_turnover_value
  else:
    print("Div not found. Please check the HTML structure and update the div selector.")

  # Close the Selenium driver
  driver.quit()

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

def web_scrape():
  BOS_HSK_9008, Date_9008, volume_9008, turnover_9008 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9008&sc_lang=en")
  CAM_9042, Date_9042, volume_9042, turnover_9042 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9042&sc_lang=en")
  HGI_9439, Date_9439, volume_9439, turnover_9439 = scrape_aum(url = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9439&sc_lang=en")

  print(BOS_HSK_9008, volume_9008, turnover_9008)
  print(CAM_9042, volume_9042, turnover_9042)
  print(HGI_9439, volume_9439, turnover_9439)
  # Find the latest date
  latest_date = max(Date_9008, Date_9042, Date_9439)
  print(latest_date)
  return volume_9008, turnover_9008, volume_9042, turnover_9042, volume_9439, turnover_9439, latest_date


volume_9008, turnover_9008, volume_9042, turnover_9042, volume_9439, turnover_9439, date = web_scrape()

query = """
SELECT * FROM VOLUME_TURNOVER_AUM_DATA
ORDER BY DATE DESC
LIMIT 1;
"""

cursor.execute(query)
last_row = cursor.fetchone()
print(last_row)

if volume_9008 == '-':
  volume_9008 = last_row[1]

print(volume_9008)

if turnover_9008 == '-':
  turnover_9008 = last_row[2]

print(turnover_9008)

if volume_9042 == '-':
  volume_9042 = last_row[3]

print(volume_9042)

if turnover_9042 == '-':
  turnover_9042 = last_row[4]

print(turnover_9042)

if volume_9439 == '-':
  volume_9439 = last_row[5]

print(volume_9439)

if turnover_9439 == '-':
  turnover_9439 = last_row[6]

print(turnover_9439)

# Check if a record for the current date already exists
cursor.execute(f"SELECT COUNT(*) FROM VOLUME_TURNOVER_AUM_DATA WHERE date = '{date}'")
record_exists = cursor.fetchone()[0] > 0

print("record exists: ", record_exists)
if record_exists:
  # Update the existing record
  cursor.execute(f"""
      UPDATE VOLUME_TURNOVER_AUM_DATA
      SET volume_9008 = {volume_9008}, volume_9042 = {volume_9042}, volume_9439 = {volume_9439}, turnover_9008 = {turnover_9008}, turnover_9042 = {turnover_9042}, turnover_9439 = {turnover_9439}
      WHERE date = '{date}'
  """)
  print("Table Updated")
else:
  # Insert new data into Snowflake
  cursor.execute(f"""
      INSERT INTO VOLUME_TURNOVER_AUM_DATA (date, volume_9008, turnover_9008, volume_9042, turnover_9042, volume_9439, turnover_9439)
      VALUES ('{date}', {volume_9008}, {turnover_9008}, {volume_9042}, {turnover_9042}, {volume_9439},  {turnover_9439})
  """)
  print("Insertion Done")

