import requests
from lxml import etree
from StringIO import StringIO

# go to the site: http://www.somervillema.gov/sweeper
# and see how many pages of tables there are
NUM_TABLES = 104
timeToFloat = lambda x: float(x.rstrip(" apm.").split(":")[0]) + (float(x.rstrip(" apm.").split(":")[1][0:2]) / 60 if ":" in x else 0) + (12.0 if "p.m." in x and not "12" in x else 0)

data = []
for x in range(0, NUM_TABLES + 1):
    d = requests.get("http://www.somervillema.gov/sweeper?page={}".format(x)).text
    parser = etree.HTMLParser()
    tree = etree.parse(StringIO(d), parser)

    nested_table_hell = 'body/center/table/tbody/tr/td/table/tbody/tr/td/table/tbody/tr/td/div/div/table/tbody/tr'
    # I forgot how to use xpath. Don't judge me
    for y in tree.getroot().xpath(nested_table_hell):
        data.append([z.text.strip() for z in y.xpath('td') if z.text.strip()])

for x in enumerate(data):
    num, x = x[0], x[1]
    startTime, endTime = [timeToFloat(y) for y in x[4].split(" - ")]
    data[num] = [x[0] + " " + x[1], x[2], startTime, endTime, [y for y in x[3].split() if "day" in y][0],
        ",".join([y for y in x[3] if y.isdigit()]), x[5] if len(x) >= 6 else ""]
