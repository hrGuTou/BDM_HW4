# -*- coding: utf-8 -*-
from pyspark import SparkContext
import sys, csv


def mapper(idx, data):
    if idx == 0:
        next(data)
    reader = csv.reader(data)
    counts = {}

    for row in reader:
        if len(row) > 7 and row[0][:4].isdigit():
            tup = (row[0][:4], row[1].lower(), row[7].lower())
            counts[tup] = counts.get(tup, 0) + 1

    return counts.items()


def reducer(data):
    counts = list(data[1])
    total = sum(counts)
    companies = len(counts)
    top_perc = int(max(counts) * 100.0 / total + 0.5)
    year, product = data[0]
    data = '{},{}'.format(product, year)
    if ',' in product:
        product = '"{}"'.format(product)
    return data, ','.join((product, year, str(total), str(companies), str(top_perc)))


if __name__ == '__main__':
    sc = SparkContext()
    file = sys.argv[1]
    rdd = sc.textFile(file)
    res = rdd.mapPartitionsWithIndex(mapper) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[0][:2], x[1])) \
        .groupByKey() \
        .map(reducer) \
        .sortByKey() \
        .map(lambda x: x[1]) \
        .collect()

    sc.parallelize(res).saveAsTextFile('HW4_output')