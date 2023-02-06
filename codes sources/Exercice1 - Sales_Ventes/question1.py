from mrjob.job import MRJob

class ProductLines(MRJob):
    def mapper(self, _, line):
        # Sauter la première ligne (les headers)
        if line.startswith('ORDERNUMBER'):
            return
        product_line = line.split(',')[10]
        yield None, product_line

    def reducer(self, key, values):
        unique_product_lines = set(values)
        for product_line in unique_product_lines:
            yield product_line, None

if __name__ == '__main__':
    ProductLines.run()
