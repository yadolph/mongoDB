import csv
import re
from datetime import datetime
from pprint import pprint
from pymongo import MongoClient


def read_data(csv_file, db):
    db.drop()
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = []
        for row in reader:
            row['Цена'] = int(row['Цена'])  # Иначе Монго считает это строкой и сортирует по первой цифре (800>4000)
            temp_date = row['Дата'].split('.')
            row['Дата'] = datetime(2020, int(temp_date[1]), int(temp_date[0]))
            rows.append(row)
    result = db.insert_many(rows)
    return result


def find_cheapest(db):
    cheapest = list(db.find().sort('Цена', 1))
    return cheapest


def find_by_name(name, db):
    name = re.sub('[^A-Za-z0-9\-\s]', '', name)
    regexp = re.compile(name, re.IGNORECASE)
    found = list(concerts.find({'Исполнитель': regexp}).sort('Цена', 1))
    return found


def find_by_date(start_date, end_date, db):
    found = list(db.find({'Дата': {'$gte': start_date, '$lte': end_date}}))
    return found


if __name__ == '__main__':
    client = MongoClient()
    database = client['testbase']
    concerts = database['concerts']

    read_data('artists.csv', concerts)
    pprint(find_cheapest(concerts))

    print('\n Нашли: \n')
    pprint(find_by_name('enter', concerts))
    pprint(find_by_name('seconds', concerts))
    pprint(find_by_name('t-fest!*+', concerts))

    print('\n Нашли в июле: \n')
    pprint(find_by_date(datetime(2020,7,1), datetime(2020,7,30), concerts))