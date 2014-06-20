from grab import Grab
import csv

csvfile = open('data.csv', 'w', newline='')
writer = csv.writer(csvfile, delimiter=',')


def register_trope(name, trope):
    writer.writerow([name, trope['name'], trope['href']])

g = Grab()
ns = "Film"
name = "ReturnOfTheJedi"
g.go("http://tvtropes.org/pmwiki/pmwiki.php/{ns}/{name}".format(ns=ns, name=name))
tropes = g.doc.select('//div[@id="wikitext"]//ul[1]/li/a[1]')
for trope in tropes:
    trope_d = {"name": trope.text(),
               "href": trope.attr('href')}
    register_trope(name, trope_d)

csvfile.close()
