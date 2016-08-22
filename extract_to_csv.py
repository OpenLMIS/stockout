import psycopg2

conn = psycopg2.connect("dbname='openlmis' user='openlmis' host='localhost' password='openlmis'")
cur = conn.cursor()
sql = "select " \
      "sce.id, sce.stockcardid, sce.type, sce.quantity, sce.adjustmenttype, sce.occurred, " \
      "sc.facilityid as facilityid, sc.productid as productid, " \
      "scekv.valuecolumn as soh " \
      "from stock_card_entries as sce " \
      "join stock_cards as sc on sc.id = sce.stockcardid " \
      "join stock_card_entry_key_values as scekv on scekv.stockcardentryid = sce.id " \
      "where scekv.keycolumn = 'soh'"
cur.execute(sql)
rows = cur.fetchall()

aggregates = {}

for record in rows:
    stock_card_id = record[1]
    movement_type = record[2]
    quantity = record[3]
    adjustment_type = record[4]
    occurred_month = ("%s" % record[5])[:-3]
    facility = record[6]
    product = record[7]
    soh = int(record[8])
    aggregate_key = (facility, product, occurred_month)
    try:
        aggregate = aggregates[aggregate_key]
    except KeyError:
        # aggregate = {'tx': 0, 'total_quantity': 0, 'has_stock_out': False}
        aggregate = {'tx_in': 0, 'tx_out': 0, 'total_quantity_in': 0, 'total_quantity_out': 0, 'has_stock_out': False}
        aggregates[aggregate_key] = aggregate

    if quantity < 0:
        aggregate['tx_out'] += 1
        aggregate['total_quantity_out'] += quantity * -1
    else:
        aggregate['tx_in'] += 1
        aggregate['total_quantity_in'] += quantity

    if soh == 0:
        aggregate['has_stock_out'] = True

print len(aggregates)

for i in range(10):
    key = aggregates.keys()[i]
    value = aggregates.get(key)
    print "---"
    print key
    print value

import numpy as np

matrix = np.zeros((len(aggregates), 8), dtype=np.dtype(object))

for i, key in enumerate(aggregates.keys()):
    value = aggregates.get(key)
    matrix[i] = key + (value['tx_in'], value['total_quantity_in'], value['tx_out'], value['total_quantity_out'], value['has_stock_out'])
# matrix = sorted(matrix, key=lambda record: record[4])

# import pylab as pylab
#
# positives = matrix[matrix[:, 5] == True]
# negatives = matrix[matrix[:, 5] == False]
#
# pylab.scatter(positives[:, 3], positives[:, 4], color='red')
# pylab.scatter(negatives[:, 3], negatives[:, 4])
# pylab.show()

import csv

with open('stockout/aggregated.csv', 'w') as csv_file:
    writer = csv.writer(csv_file, delimiter="\t")
    for record in matrix:
        writer.writerow(record)
