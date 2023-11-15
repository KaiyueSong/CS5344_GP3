# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from operator import add
from pyspark import SparkConf, SparkContext
import csv
from io import StringIO


# Define a function to parse a CSV line
def parse_csv_line(line):
    reader = csv.reader(StringIO(line))
    return next(reader)


def create_vertex_rdd(rdd):
    vertex_rdd = rdd.map(lambda x: (x[1], 1)) \
        .reduceByKey(add)
    return vertex_rdd


path = "bq-results-20231028-20221003.csv"
conf = SparkConf().setAppName("Implement Graph")
sc = SparkContext(conf=conf)

csv_rdd = sc.textFile(path).filter(lambda line: "DocumentIdentifier" not in line)

# Parse the CSV data
event_rdd = csv_rdd.map(parse_csv_line) \
    .map(lambda x: (x[3], (x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13],x[14],x[15],x[16],x[17],x[18],x[19])))

# Create them RDDs
taxes_rdd = event_rdd.filter(lambda x: x[1][0] == '1').map(lambda x: (x[0], 'taxes'))
unemployment_rdd = event_rdd.filter(lambda x: x[1][1] == '1').map(lambda x: (x[0], 'unemployment'))
economy_rdd = event_rdd.filter(lambda x: x[1][2] == '1').map(lambda x: (x[0], 'economy'))
international_relations_rdd = event_rdd.filter(lambda x: x[1][3] == '1').map(lambda x: (x[0], 'international_relations'))
border_issues_rdd = event_rdd.filter(lambda x: x[1][4] == '1').map(lambda x: (x[0], 'border_issues'))
health_care_rdd = event_rdd.filter(lambda x: x[1][5] == '1').map(lambda x: (x[0], 'health_care'))
public_order_rdd = event_rdd.filter(lambda x: x[1][6] == '1').map(lambda x: (x[0], 'public_order'))
civil_liberties_rdd = event_rdd.filter(lambda x: x[1][7] == '1').map(lambda x: (x[0], 'civil_liberties'))
environment_rdd = event_rdd.filter(lambda x: x[1][8] == '1').map(lambda x: (x[0], 'environment'))
education_rdd = event_rdd.filter(lambda x: x[1][9] == '1').map(lambda x: (x[0], 'education'))
domestic_politics_rdd = event_rdd.filter(lambda x: x[1][10] == '1').map(lambda x: (x[0], 'domestic_politics'))
poverty_rdd = event_rdd.filter(lambda x: x[1][11] == '1').map(lambda x: (x[0], 'poverty'))
disaster_rdd = event_rdd.filter(lambda x: x[1][12] == '1').map(lambda x: (x[0], 'disaster'))
religion_rdd = event_rdd.filter(lambda x: x[1][13] == '1').map(lambda x: (x[0], 'religion'))
infrastructure_rdd = event_rdd.filter(lambda x: x[1][14] == '1').map(lambda x: (x[0], 'infrastructure'))
media_internet_rdd = event_rdd.filter(lambda x: x[1][15] == '1').map(lambda x: (x[0], 'media_internet'))

event_theme_rdd = taxes_rdd.union(unemployment_rdd) \
    .union(economy_rdd).union(international_relations_rdd) \
    .union(border_issues_rdd).union(health_care_rdd) \
    .union(public_order_rdd).union(civil_liberties_rdd)  \
    .union(environment_rdd).union(education_rdd) \
    .union(domestic_politics_rdd).union(poverty_rdd) \
    .union(disaster_rdd).union(religion_rdd) \
    .union(infrastructure_rdd).union(media_internet_rdd)

vertex_rdd = create_vertex_rdd(event_theme_rdd)
print("Vertex of the graph:")
# print(vertex_rdd.collect())

for theme, cnt in vertex_rdd.collect():
    print(f"{theme},{cnt}")

themes_rdd = event_theme_rdd.map(lambda x: (x[1], x[0])).groupByKey().mapValues(list)
# print(themes_rdd.take(3))

theme_tie = []
i = 0
j = 0
row = 0
for theme, id_list in themes_rdd.collect():
    for theme_01, id_list_01 in themes_rdd.collect():
        if j > i:
            rdd1 = sc.parallelize(id_list)
            rdd2 = sc.parallelize(id_list_01)
            # , numSlices=1000
            num = len(rdd1.intersection(rdd2).collect())
            theme_tie.append((theme, theme_01, num))
            print(f"Row {row},{theme},{theme_01},{num}")
            # print(f"Theme 1:{theme} Theme 2:{theme_01} Intersection:{num}")
            row = row+1
        j = j+1
    i = i + 1
    j = 0

# theme_tie_rdd = sc.parallelize(theme_tie).map(lambda x: ((x[0], x[1]), x[2]))
# print("Edge of the Graph:")
# print(theme_tie_rdd.collect())
sc.stop()
