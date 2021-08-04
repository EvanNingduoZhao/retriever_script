import csv
with open("merch_vss_pilot.csv") as pilot:
    reader = csv.reader(pilot, delimiter=',')
    for row in reader:
        text="('{keyword}', '{url}'),"
        print(text.format(keyword = row[0], url=row[1]))