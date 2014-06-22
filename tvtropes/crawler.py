import csv
from grab.spider import Spider, Task
import logging


class TVTropesSpider(Spider):
    ns = 'Film'
    initial_urls = ['http://tvtropes.org/pmwiki/pmwiki.php/' + ns]

    def prepare(self):
        super().prepare()
        self.full_file = open('data.csv', 'w', newline='')
        self.result_file = csv.writer(self.full_file)
        self.short_file = open('s_data.csv', 'w', newline='')
        self.s_result_file = csv.writer(self.short_file)

    def shutdown(self):
        super().shutdown()
        self.full_file.close()
        self.short_file.close()

    def task_initial(self, grab, task):
        sections = grab.doc.select('//div[@id="wikitext"]/ul[1]/li/a[1]')
        for section in sections:
            yield Task("section", s_name=section.text(), url=section.attr('href'))

    def task_section(self, grab, task):
        films = grab.doc.select('//div[@id="wikitext"]//li//a[1]')
        for film in films:
            if film.attr('href').split('/')[-2] == self.ns:
                yield Task("film", f_name=film.text(), url=film.attr('href'))

    def task_film(self, grab, task):
        tropes = grab.doc.select('//div[@id="wikitext"]//li//a[1]')
        for trope in tropes:
            if trope.attr('href').split('/')[-2] == "Main":
                self.result_file.writerow([
                    task.url,
                    task.f_name,
                    trope.text(),
                    trope.attr('href')
                ])
                self.s_result_file.writerow([
                    task.f_name,
                    trope.text()
                ])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    bot = TVTropesSpider(thread_number=10)
    bot.run()
