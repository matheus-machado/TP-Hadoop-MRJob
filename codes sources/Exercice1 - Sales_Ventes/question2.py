from mrjob.job import MRJob

class SalesByProductLine(MRJob):
    def mapper(self, _, line):
        # Ignore the first line with field names
        if line.startswith('ORDERNUMBER'):
            return
        product_line = line.split(',')[10]
        quantity =  int(line.split(',')[1])
        yield product_line, quantity

    def reducer(self, product_line, quantities):
        yield product_line, sum(quantities)

if __name__ == '__main__':
    SalesByProductLine.run()