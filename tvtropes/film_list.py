import logging
import csv


def save_set(csv_name, s_set):
    with open(csv_name, "w", newline='') as file:
        csvf = csv.writer(file)
        for f in s_set:
            csvf.writerow(f)

if __name__ == "__main__":
    films = set()
    tropes = set()
    logging.basicConfig(level=logging.DEBUG)
    with open("gen/s_data.csv", newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            films.add((row[0], row[2]))
            tropes.add((row[1],))

    save_set("gen/films.csv", films)
    save_set("gen/tropes.csv", tropes)
