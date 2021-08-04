import csv
with open("merch_vss_pilot.csv") as pilot:
    reader = csv.reader(pilot, delimiter=',')
    count = 0
    for row in reader:
        if "'" in row[0]:
            preQuote = row[0].split("'")[0]
            postQuote = row[0].split("'")[1]
            text="'{pre}\\\'{post}',"
            print(text.format(pre = preQuote, post=postQuote))
        else:
            text="'{keyword}',"
            print(text.format(keyword = row[0]))