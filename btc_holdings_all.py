import requests
from bs4 import BeautifulSoup
from datetime import datetime
from snowflake.connector import connect
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

"""## **Scrape Bosera data (9008)**"""

def fetch_and_store_9008():

    # Selenium setup
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    # Navigate to the URL
    url_9008 = 'https://www.bosera.com.hk/en-US/products/fund/detail/BTCL'
    driver.get(url_9008)
    time.sleep(5)

    # Get page source after JavaScript has executed
    html = driver.page_source
    soup_9008 = BeautifulSoup(html, 'lxml')

    # Close the Selenium driver
    driver.quit()

    # Extract BTC Holdings
    all_tds_9008 = soup_9008.find_all('td', {'class': 'ant-table-cell'})
    btc_holdings_9008 = all_tds_9008[146].get_text(strip=True)
    btc_holdings_9008 = float(btc_holdings_9008.replace(',', ''))

    # Extract the date
    date_9008 = all_tds_9008[1].get_text(strip=True)
    date_9008 = datetime.strptime(date_9008, '%d/%m/%Y')
    date_9008 = date_9008.strftime('%Y-%m-%d')

    # Store data in Snowflake
    store_data_in_snowflake(date_9008, btc_holdings_9008, '9008')

"""## **Scrape ChinaAMC data (9042)**"""

def fetch_and_store_9042():

    # Selenium setup
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    # Navigate to the URL
    url_9042 = 'https://www.chinaamc.com.hk/product/chinaamc-bitcoin-etf/#Holdings'
    driver.get(url_9042)
    time.sleep(5)

    # Get page source after JavaScript has executed
    html = driver.page_source
    soup_9042 = BeautifulSoup(html, 'lxml')

    # Close the Selenium driver
    driver.quit()

    # Net Asset Value (mil)
    table_nav = soup_9042.find('table', {'class': 'amc-table fund-overview'})
    rows_nav = table_nav.find_all('tr')
    nav = None
    for row in rows_nav:
        cells = row.find_all('td')
        if 'Net Asset Value (mil)' in cells[0].get_text(strip=True):
            nav = cells[1].get_text(strip=True)
            break
    nav = float(nav.replace(',', '')) * 1000000

    # Closing Level
    table_cl = soup_9042.find('table', {'class': 'amc-table index-information'})
    rows_cl = table_cl.find_all('tr')
    closing_level = None
    for row in rows_cl:
        cells = row.find_all('td')
        if 'Closing Level' in cells[0].get_text(strip=True):
            closing_level = cells[1].get_text(strip=True)
            break
    closing_level = float(closing_level.replace(',', ''))

    # Weighting %
    tables = soup_9042.find_all('table', {'class': 'amc-table'})
    table_w = tables[7]
    rows_w = table_w.find_all('tr')
    weighting = None
    for row_w in rows_w:
        cells_w = row_w.find_all('td')
        if cells_w and 'VA BITCOIN CURRENCY' in cells_w[0].get_text(strip=True):
            weighting = cells_w[1].get_text(strip=True)
            break
    weighting = float(weighting)

    # Calculate BTC Holdings
    btc_holdings_9042 = (nav * weighting / 100) / closing_level

    # Extract the date
    all_ps_9042 = soup_9042.find_all('p', {'class': 'as-of-date'})
    date_9042 = all_ps_9042[1].get_text(strip=True).replace('As of ', '')
    date_9042 = datetime.strptime(date_9042, '%d-%m-%Y').strftime('%Y-%m-%d')

    # Store data in Snowflake
    store_data_in_snowflake(date_9042, btc_holdings_9042, '9042')

"""## **Scrape and calculate Harvest data (9439)**   """

def fetch_and_store_9439():

    # Step 1: Fetch the BTC price from BOS bitcoin
    url_9008 = 'https://www.bosera.com.hk/en-US/products/fund/detail/BTCL'
    response_9008 = requests.get(url_9008)
    soup_9008 = BeautifulSoup(response_9008.content, 'lxml')

    # Locate the price
    all_tds_9008 = soup_9008.find_all('td', {'class': 'ant-table-cell'})
    price = all_tds_9008[144].get_text(strip=True)
    price = float(price.replace(',', ''))

    # Step 2: Use Selenium to fetch AUM data from HKEX
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)

    url_9439 = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Exchange-Traded-Products/Exchange-Traded-Products-Quote?sym=9439&sc_lang=en"
    driver.get(url_9439)
    time.sleep(5)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # Find the AUM element
    aum_element_9439 = soup.find('dt', {'class': 'ico_data col_aum'})
    aum_9439 = aum_element_9439.text.strip() if aum_element_9439 else "N/A"

    # Extract and convert AUM to float
    if aum_9439 != "N/A":
        aum_9439 = aum_9439[3:]  # Remove "US$"
        aum_9439 = float(aum_9439[:-1].replace(',', '')) * 1000000

    driver.quit()

    # Calculate BTC Holdings
    btc_holdings_9439 = aum_9439 / price if price != 0 else 0

    # Step 3: Scrape and format the date
    date_9439_element = soup.find('dt', {'class': 'ico_data col_aum_date'})
    date_9439 = date_9439_element.text.strip() if date_9439_element else "N/A"

    if date_9439 != "N/A":
        date_9439 = date_9439.replace('as at ', '').replace('(', '').replace(')', '').strip()
        date_9439 = datetime.strptime(date_9439, '%d %b %Y').strftime('%Y-%m-%d')

    # Step 4: Store the data in Snowflake
    store_data_in_snowflake(date_9439, btc_holdings_9439, '9439')

"""## **Get the price from the bosera website**"""

def fetch_price():
    # Selenium setup
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    # Navigate to the URL
    url_9008 = 'https://www.bosera.com.hk/en-US/products/fund/detail/BTCL'
    driver.get(url_9008)
    time.sleep(5)

    # Get page source after JavaScript has executed
    html = driver.page_source
    soup_9008 = BeautifulSoup(html, 'lxml')

    # Close the Selenium driver
    driver.quit()

    # Extract the price
    all_tds_9008 = soup_9008.find_all('td', {'class': 'ant-table-cell'})
    price = all_tds_9008[144].get_text(strip=True)
    price = float(price.replace(',', ''))

    # Extract the date
    price_date = all_tds_9008[1].get_text(strip=True)
    price_date = datetime.strptime(price_date, '%d/%m/%Y').strftime('%Y-%m-%d')

    return price_date, price

"""## **Store bitcoin holding data in Database**"""

def store_data_in_snowflake(date, btc_holdings, etf):
    # Connect to Snowflake
    conn = connect(
        user='SQRR',
        password='Panama9-Latticed8-Chamomile5-Scary3-Krypton9',
        account='tlnygtx-hc90982',
        database='HKEX',
        schema='HKEX'
    )

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Create table if it does not exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS BTC_HOLDINGS (
            Date DATE PRIMARY KEY,
            btc_holdings_9008 FLOAT,
            inflow_outflow_9008 FLOAT,
            value_9008 FLOAT,
            btc_holdings_9042 FLOAT,
            inflow_outflow_9042 FLOAT,
            value_9042 FLOAT,
            btc_holdings_9439 FLOAT,
            inflow_outflow_9439 FLOAT,
            value_9439 FLOAT,
            price FLOAT
        )
        """)

        # Check if the record already exists for the date
        cursor.execute("SELECT * FROM BTC_HOLDINGS WHERE Date = %s", (date,))
        existing_row = cursor.fetchone()

        # Fetch the previous date's holdings for the ETF
        cursor.execute(f"""
        SELECT btc_holdings_{etf} FROM BTC_HOLDINGS
        WHERE Date < %s ORDER BY Date DESC LIMIT 1
        """, (date,))
        previous_row = cursor.fetchone()

        previous_holdings = previous_row[0] if previous_row else 0

        # Calculate inflow/outflow
        inflow_outflow = btc_holdings - previous_holdings

        if not existing_row:
            # Insert a new row with BTC holdings and inflow/outflow
            cursor.execute(f"""
            INSERT INTO BTC_HOLDINGS (Date, btc_holdings_{etf}, inflow_outflow_{etf})
            VALUES (%s, %s, %s)
            """, (date, btc_holdings, inflow_outflow))
        else:
            # Update the existing row with new BTC holdings and inflow/outflow
            cursor.execute(f"""
            UPDATE BTC_HOLDINGS
            SET btc_holdings_{etf} = %s, inflow_outflow_{etf} = %s
            WHERE Date = %s
            """, (btc_holdings, inflow_outflow, date))

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

"""## **Store price data in Database**"""

# In[7]:


def store_price_in_snowflake(price_date, price):
    # Connect to Snowflake
    conn = connect(
        user='SQRR',
        password='Panama9-Latticed8-Chamomile5-Scary3-Krypton9',
        account='tlnygtx-hc90982',
        database='HKEX',
        schema='HKEX'
    )

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Update the existing record with the price where the date matches
        cursor.execute("""
        UPDATE BTC_HOLDINGS
        SET price = %s
        WHERE Date = %s
        """, (price, price_date))

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

"""## **Update the dollar values in the database**"""

def update_dollar_values(price_date):
    # Connect to Snowflake
    conn = connect(
        user='SQRR',
        password='Panama9-Latticed8-Chamomile5-Scary3-Krypton9',
        account='tlnygtx-hc90982',
        database='HKEX',
        schema='HKEX'
    )

    try:
        # Create a cursor object
        cursor = conn.cursor()

        # Update value columns with calculated dollar values
        cursor.execute("""
        UPDATE BTC_HOLDINGS
        SET
            value_9008 = inflow_outflow_9008 * price,
            value_9042 = inflow_outflow_9042 * price,
        WHERE inflow_outflow_9008 IS NOT NULL
           OR inflow_outflow_9042 IS NOT NULL
        """)

        cursor.execute("""
        UPDATE BTC_HOLDINGS
        SET
            value_9439 = inflow_outflow_9439 * price
        WHERE inflow_outflow_9439 IS NOT NULL
           AND price_date == Date
        """)

        # Commit the transaction
        conn.commit()

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

"""## **Run Functions**"""

fetch_and_store_9008()

fetch_and_store_9042()

fetch_and_store_9439()

price_date, price = fetch_price()
store_price_in_snowflake(price_date, price)

# Call this function at the end
update_dollar_values(price_date)

