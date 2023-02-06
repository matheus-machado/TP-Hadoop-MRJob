#Quel est le chiffre d’affaire (cf QUANTITYORDERED et PRICEEACH) par an et par catégorie de produits (tous pays confondus) ?

from mrjob.job import MRJob

class RevenueByProductLineAndYear(MRJob):
    def mapper(self, _, line):
        # Ignore the first line with field names
        if line.startswith('ORDERNUMBER'):
            return
        fields = line.split(',')
        year = fields[9] # Extract the year 
        product_line = fields[10]
        quantity = int(fields[1])
        price = float(fields[2])
        revenue = quantity * price
        yield (product_line, year), revenue

    def reducer(self, key, revenues):
        product_line, year = key
        yield (product_line, year), "{:.2f}".format(sum(revenues))

if __name__ == '__main__':
    RevenueByProductLineAndYear.run()