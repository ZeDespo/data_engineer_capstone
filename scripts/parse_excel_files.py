import csv
import xlrd


def main() -> None:
    """
    Extract asylum data by country from three seperate excel files and join them into one csv that
    Spark can parse appropriately.
    :return: Nothing.
    """
    results = {}
    files = ['fy2018_table14d.xlsx', 'fy2018_table17d.xlsx', 'fy2018_table19d.xlsx']
    width = 11
    heights = [76, 118, 109]
    for idx, (file, height) in enumerate(zip(files, heights)):
        wb = xlrd.open_workbook(file, on_demand=True)
        sheet = wb.sheet_by_index(0)
        for i in range(15, height):
            country = sheet.cell(i, 0).value
            if country not in results:
                results[country] = {}
            for j in range(1, width):
                year = int(sheet.cell(3, j).value)
                if year not in results[country]:
                    results[country][year] = [None] * len(files)
                value = sheet.cell(i, j).value
                if type(value) is float:  # Any letters means that the value is null
                    results[country][year][idx] = int(value)
    csv_list = []
    for country in results:
        items = results[country].items()
        for item in items:
            year, stats = item[0], item[1]
            csv_list.append([country, year, *stats])
    csv_list.sort(key=lambda x: x[0])
    with open('asylum_cleaned.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['country', 'year', 'num_arrivals', 'num_accepted_affirmitavely', 'num_accepted_defensively'])
        writer.writerows(csv_list)


if __name__ == '__main__':
    exit()
