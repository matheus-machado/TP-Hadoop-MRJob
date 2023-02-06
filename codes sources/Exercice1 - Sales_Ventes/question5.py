from mrjob.job import MRJob
from mrjob.step import MRStep

class SalesByCity(MRJob):
  
  def steps(self):
    return [
        MRStep(mapper=self.mapper,
               reducer=self.reducer_city),
        #MRStep(reducer=self.reducer_country)
    ]
  
  def mapper(self, _, line):
    # Ignore the first line with field names
    if line.startswith('ORDERNUMBER'):
        return

    fields = line.split(',')
    country = fields[20]
    city = fields[17]
    product_line = fields[10]
    quantity = int(fields[1])
    
    if product_line == 'Motorcycles':
        yield country, (city, quantity)
    
  def reducer_city(self, country, values):
    city_sales = {}
    for city, quantity in values:
        if city not in city_sales:
            city_sales[city] = 0
        city_sales[city] += quantity
    max_city = max(city_sales, key=city_sales.get)
    
    yield country, (max_city, city_sales[max_city])
    
  def reducer_country(self, country, values):
    yield country, max(values, key=lambda x: x[1])
    
if __name__ == '__main__':
  SalesByCity.run()
