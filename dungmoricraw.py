#!/usr/bin/env python
# coding: utf-8

import os
import time
# import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread
from google.oauth2 import service_account
from gspread_dataframe import set_with_dataframe
import pandas as pd

# options = uc.ChromeOptions()
options = webdriver.ChromeOptions()
# options.add_argument(f"user-data-dir={profile_path}")
# options.add_argument(f"profile-directory={profile_name}")
# driver = uc.Chrome(options=options)
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

current_directory = os.getcwd()

try:
    driver.get("https://dungmori.com/backend/sql")

    accounttxt_path = os.path.join(current_directory, 'account.txt')

    with open(accounttxt_path, 'r', encoding='utf-8') as file:
        accounttxt = file.readlines()
        email = accounttxt[0].strip()
        password = accounttxt[1].strip()

    # Điền địa chỉ email và tiếp tục
    time.sleep(5)
    email_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="email"]'))
    )
    email_input.send_keys(email)

    # Đợi và điền mật khẩu
    time.sleep(2)
    password_input = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.XPATH, '//*[@id="password"]'))
    )
    password_input.send_keys(password)

    time.sleep(2)
    next_button = WebDriverWait(driver, 1000).until(
        EC.element_to_be_clickable((By.XPATH, '/html/body/section/div/div/div/form/div[5]/div/button'))
    )
    next_button.click()

    time.sleep(10)
    driver.get("https://dungmori.com/backend/sql")

    group_query_path = os.path.join(current_directory, 'group info.sql')

    with open(group_query_path, 'r', encoding='utf-8') as file:
        group_query = file.read()

    time.sleep(2)
    query_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="txtArea"]'))
    )

    driver.execute_script("arguments[0].value = arguments[1];", query_input, group_query)

    time.sleep(2)
    query_button = driver.find_element(By.XPATH, '//*[@id="sql_manager"]/button')
    query_button.click()

    time.sleep(5)

    soup = BeautifulSoup(driver.page_source, features="lxml")

    table = soup.find_all('table', class_='table table-bordered')

    if not table:
        raise ValueError("No table found on the page")

    headers = [header.get_text() for header in table[0].find_all('thead')[0].find_all('th')]

    rows = []
    for row in table[0].find_all('tbody')[0].find_all('tr'):
        cells = [cell.get_text() for cell in row.find_all('th')] 
        rows.append(cells)

    if not headers or not rows:
        raise ValueError("Headers or rows are empty")

    group_info_df = pd.DataFrame(rows, columns=headers)

    time.sleep(2)
    driver.get('https://dungmori.com/backend/sql-school')

    group_time_query_path = os.path.join(current_directory, 'group_time_tbl_count.sql')

    with open(group_time_query_path, 'r', encoding='utf-8') as file:
        group_time_query = file.read()

    time.sleep(2)
    group_time_query_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="txtArea"]'))
    )

    driver.execute_script("arguments[0].value = arguments[1];", group_time_query_input, group_time_query)

    time.sleep(2)
    query_button = driver.find_element(By.XPATH, '//*[@id="sql_manager"]/button')
    query_button.click()

    time.sleep(5)

    group_times_soup = BeautifulSoup(driver.page_source, features="lxml")

    group_times_table = group_times_soup.find_all('table', class_='table table-bordered')

    if not group_times_table:
        raise ValueError("No group times table found on the page")

    group_times_table_rows = []
    for row in group_times_soup.find_all('tbody')[0].find_all('tr'):
        cells = [cell.get_text() for cell in row.find_all('th')] 
        group_times_table_rows.append(cells)

    group_times_df = pd.DataFrame(group_times_table_rows, columns=['group_id', 'session_count'])

    


    final_df = pd.merge(group_info_df, group_times_df, how='inner', on='group_id')

    change_type_columns = ['group_id', 'vip_session', 'count_students', 'session_count']

    for column in change_type_columns:
        final_df[column] = final_df[column].astype(int)

    final_df['đợt chăm sóc'] = final_df.apply(
        lambda row: 1 if (
            (row['type'] == 'vip1' and row['session_count'] == 6) or
            (row['type'] == 'vip15' and row['session_count'] == 6) or
            (row['type'] == 'matgoc' and row['session_count'] == 3) or
            (row['type'] == 'captoc' and row['session_count'] == 12)
        ) else 2 if (
            (row['type'] == 'vip1' and row['session_count'] == 22) or
            (row['type'] == 'vip15' and row['session_count'] in range(28, 35)) or
            (row['type'] == 'matgoc' and row['session_count'] == 8) or
            (row['type'] == 'captoc' and row['session_count'] == 24)
        ) else 3 if (
            (row['type'] == 'vip1' and row['session_count'] == 40) or
            (row['type'] == 'vip15' and row['session_count'] in range(40, 51))
        ) else 0,
        axis=1
    )

    time.sleep(2)
    driver.get('https://dungmori.com/backend/sql-school')

    countinous_missing_student_query_path = os.path.join(current_directory, 'countinous_missing_student.sql')

    with open(countinous_missing_student_query_path, 'r', encoding='utf-8') as file:
        countinous_missing_student_query = file.read()

    time.sleep(2)
    countinous_missing_student_query_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="txtArea"]'))
    )

    driver.execute_script("arguments[0].value = arguments[1];", countinous_missing_student_query_input, countinous_missing_student_query)

    time.sleep(2)
    query_button = driver.find_element(By.XPATH, '//*[@id="sql_manager"]/button')
    query_button.click()

    time.sleep(5)

    countinous_missing_students_soup = BeautifulSoup(driver.page_source, features="lxml")

    countinous_missing_students_table = countinous_missing_students_soup.find_all('table', class_='table table-bordered')

    if not countinous_missing_students_table:
        raise ValueError("No group times table found on the page")

    countinous_missing_students_table_rows = []
    for row in countinous_missing_students_soup.find_all('tbody')[0].find_all('tr'):
        cells = [cell.get_text() for cell in row.find_all('th')] 
        countinous_missing_students_table_rows.append(cells)

    countinous_missing_students_df = pd.DataFrame(countinous_missing_students_table_rows, columns=['date_attendance','group_id', 'student_id','continuous_missing_count'])
    final_countinous_missing_students_df = pd.merge(group_info_df[['group_id','name']],countinous_missing_students_df,how = 'inner', on='group_id')

    # Đặt phạm vi (scope)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Tải credentials từ file JSON
    file_path = os.path.join(current_directory, 'service_account_credentials.json')
    creds = service_account.Credentials.from_service_account_file(file_path, scopes=scope)
    client = gspread.authorize(creds)

    # Mở Google Sheet bằng URL hoặc ID
    spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/16c6xG0Rb9u_jkdGsjS7hU8wvDVdvnq2x7V2bhsMF42I/edit?gid=0#gid=0')
    worksheet = spreadsheet.get_worksheet(0)  # Assuming you want to paste into the first worksheet
    countinous_missing_students_worksheet = spreadsheet.get_worksheet(1) 
    # Xóa tất cả dữ liệu trong worksheet
    time.sleep(1)
    worksheet.clear()
    countinous_missing_students_worksheet.clear()
    # Ghi dữ liệu mới vào worksheet
    time.sleep(1)
    set_with_dataframe(worksheet, final_df) 

    # Ghi dữ liệu mới vào worksheet
    time.sleep(1)
    set_with_dataframe(countinous_missing_students_worksheet, final_countinous_missing_students_df) 

    spreadsheet2 = client.open_by_url('https://docs.google.com/spreadsheets/d/1iaQHNoFmLn0yYXFoVvd03KvJCDH7kEEiQ3ULRs4UHgo/edit?gid=0#gid=0')
    worksheet2 = spreadsheet2.get_worksheet(0) 
    time.sleep(1)
    last_row = len(worksheet2.get_all_values()) + 1  
    set_with_dataframe(worksheet2, final_countinous_missing_students_df,include_index=False,include_column_header=False,row=last_row,resize=False)

finally:

    try:
        driver.quit()  
    except Exception as e:
        print(f"Error closing the driver: {e}")