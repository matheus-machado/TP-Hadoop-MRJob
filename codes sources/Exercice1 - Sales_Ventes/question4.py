#Pour chaque pays, quel est le magasin (CUSTOMERNAME) qui a réalisé le plus gros chiffre d’affaire dans la catégorie de produits Trucks and Buses ?

from mrjob.job import MRJob
from mrjob.step import MRStep

class MaxRevenueStoreByCountry(MRJob):
    def mapper(self, _, line):

        # Ignore the first line with field names
        if line.startswith('ORDERNUMBER'):
            return

        fields = line.split(',')
        country = fields[20]
        customer_name = fields[13]
        product_line = fields[10]
        quantity = int(fields[1])
        price = float(fields[2])

        if product_line == "Trucks and Buses":
            revenue = quantity * price
            yield (country, customer_name), revenue

    def reducer_revenues(self, key, revenues):
        country, customer_name = key
        yield country, (customer_name, "{:.2f}".format(sum(revenues)))

    def reducer_magasins(self, country, results):
        yield country, max(results, key=lambda x: x[1])

    def steps(self):
        return[
            MRStep(mapper=self.mapper,
                reducer=self.reducer_revenues),
            MRStep(reducer=self.reducer_magasins)   
        ]

if __name__ == '__main__':
    MaxRevenueStoreByCountry.run()