# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime
import os
import time

# Path to the CSV file
csv_file_path = "volume_turnover_aum_data.csv"

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
    elif aum_volume_value.endswith("M"):
        aum_volume_value = float(aum_volume_value[:-1])*1000000  # Remove "M" and convert to millions

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

    aum_turnover = div_right.find('dt', class_='col_turnover').text.strip() if div_right.find('dt', class_='col_turnover') else "N/A"

    aum_turnover_value = aum_turnover.strip() if aum else "N/A"
    if aum_turnover_value.startswith("US$"):
      aum_turnover_value = aum_turnover_value[3:]  # Remove "US$"
    if aum_turnover_value.endswith("K"):
        aum_turnover_value = float(aum_turnover_value[:-1])*1000  # Remove "K" and convert to thousands
    elif aum_turnover_value.endswith("M"):
        aum_turnover_value = float(aum_turnover_value[:-1])*1000000  # Remove "M" and convert to millions

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

    return aum_value, aum_date, aum_volume_value, aum_turnover_value
  else:
    print("Div not found. Please check the HTML structure and update the div selector.")

  # Close the Selenium driver
  driver.quit()

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
  return [
        {
            "date": latest_date,
            "volume_9008": volume_9008,
            "turnover_9008": turnover_9008,
            "volume_9042": volume_9042,
            "turnover_9042": turnover_9042,
            "volume_9439": volume_9439,
            "turnover_9439": turnover_9439,
        }
    ]

#volume_9008, turnover_9008, volume_9042, turnover_9042, volume_9439, turnover_9439, date = web_scrape()


def save_to_csv(data):
    # Load existing data if the CSV exists
    if os.path.exists(csv_file_path):
        df_existing = pd.read_csv(csv_file_path)
    else:
        df_existing = pd.DataFrame(columns=["date", "volume_9008", "turnover_9008", "volume_9042", "turnover_9042", "volume_9439", "turnover_9439"])

    # Convert new data to a DataFrame
    df_new = pd.DataFrame(data)

    # Remove duplicates based on 'symbol' and 'date'
    df_combined = pd.concat([df_existing, df_new])
    df_combined.replace("-", np.nan, inplace=True)
    df_combined.fillna(method='ffill', inplace=True)

    df_combined = df_combined.drop_duplicates(subset=["date"], keep="last")

    # Save back to the CSV
    df_combined.to_csv(csv_file_path, index=False)
    print(f"Data saved to {csv_file_path}")


if __name__ == "__main__":
    scraped_data = web_scrape()
    save_to_csv(scraped_data)

