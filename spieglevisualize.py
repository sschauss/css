from datetime import datetime

from matplotlib import pyplot
from pandas import read_csv

from spiegel import min_date

sentiment_file = "sentiments-csv/part-00000-4d2476ec-dbb5-4955-8473-b2e47d7e9a31-c000.csv"

if __name__ == '__main__':
    data = read_csv(sentiment_file)

    data["avg"] = data["sum"] / data["count"]
    data["date"] = data["month"].map(lambda x: (datetime(min_date.year + x // 12, 1 + (x % 12), 1)))

    pyplot.figure(dpi=300)
    pyplot.title("Range and average sentiment")
    pyplot.plot(data["date"], data["avg"])
    pyplot.fill_between(data["date"].values, data["min"], data["max"], alpha=0.2)
    pyplot.savefig("range.svg")

    pyplot.figure(dpi=300)
    pyplot.title("Overall sentiment")
    pyplot.plot(data["date"], data["sum"])
    pyplot.fill_between(data["date"].values, data["sum_neg"], data["sum_pos"], alpha=0.2)
    pyplot.savefig("overall.svg")

    pyplot.figure(dpi=300)
    pyplot.title("Volume")
    pyplot.plot(data["date"], data["n"])
    pyplot.savefig("volume.svg")
