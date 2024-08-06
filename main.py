import os.path
import csv
import gspread


gc = gspread.service_account(filename='creds.json')

wks = gc.open('mafia').worksheet('Games')

values = wks.get('B20:G8343')


with open('output.csv', mode='w', newline='', encoding='utf-8-sig') as file:
       writer = csv.writer(file)
       writer.writerows(values)

