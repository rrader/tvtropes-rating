from grab import Grab
import csv


def generate_trope_list(ns, name):
    csvfile = open('data.csv', 'w', newline='')
    writer = csv.writer(csvfile, delimiter=',')

    def register_trope(name, trope):
        writer.writerow([name, trope['name'], trope['href']])

    g = Grab()
    g.go("http://tvtropes.org/pmwiki/pmwiki.php/{ns}/{name}".format(ns=ns, name=name))
    tropes = g.doc.select('//div[@id="wikitext"]//ul[1]/li/a[1]')
    for trope in tropes:
        trope_d = {"name": trope.text(),
                   "href": trope.attr('href')}
        register_trope(name, trope_d)
    csvfile.close()

def generate_film_list(ns):
    # csvfile = open('data.csv', 'w', newline='')
    # writer = csv.writer(csvfile, delimiter=',')

    def register_trope(name, trope):
        print(name, trope)
        # writer.writerow([name, trope['name'], trope['href']])

    g = Grab()
    g.go("http://tvtropes.org/pmwiki/pmwiki.php/{ns}".format(ns=ns))
    tropes = g.doc.select('//div[@id="wikitext"]/ul[1]/li/a[1]')
    for trope in tropes:
        trope_d = {"name": trope.text(),
                   "href": trope.attr('href')}
        register_trope(ns, trope_d)
        # TODO: process every page
    # csvfile.close()

if __name__ == "__main__":
    ns = "Film"
    # name = "ReturnOfTheJedi"
    # generate_trope_list(ns, name)

    generate_film_list(ns)
