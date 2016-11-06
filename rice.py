import requests
import urllib
import sys
import xml.etree.ElementTree as et
from xml.etree.ElementTree import Element, SubElement
import re
import xml.dom.minidom as minidom
from tqdm import tqdm
import argparse
import os.path
import hashlib

def md5(fname):
	file_size = os.path.getsize(fname)
	hash_md5 = hashlib.md5()
	with open(fname, 'rb') as f, \
		 tqdm(desc='Calculating md5',
		 	  unit_scale=True,
			  unit='bytes',
			  total=file_size) as bar:
		for chunk in iter(lambda: f.read(4096), b""):
			hash_md5.update(chunk)
			bar.update(len(chunk))
		return hash_md5.hexdigest()

def SubElementWithText(top, tag, text):
	ele = SubElement(top, tag)
	ele.text = text
	return ele

class Product:
	def __init__(self, entry, auth):
		self.auth = auth
		self.attributes = {child.attrib['name']: child.text
		                   for child in entry
		                   if 'name' in child.attrib}
		self.id = entry.find('{http://www.w3.org/2005/Atom}id').text
		self.product_link = entry.find('{http://www.w3.org/2005/Atom}link').attrib['href']

	def __str__(self):
		return str(self.attributes)

	def name(self):
		return self.attributes['identifier']

	def download(self, destination_path):
		filename = self.attributes['filename'].replace('.SAFE', '.zip')
		destination_file_path = destination_path + '/' + filename

		if not os.path.isfile(destination_file_path):
			resp = requests.get(self.product_link, stream=True, auth=self.auth)
			with open(destination_file_path, 'wb') as f, \
				 tqdm(desc='Downloading ' + filename,
				 	  unit_scale=True,
				 	  unit='bytes',
					  total=int(resp.headers['content-length'])) as bar:
				for data in resp.iter_content(chunk_size=1024):
					bytes_written = f.write(data)
					bar.update(bytes_written)

		md5_link = ("https://scihub.copernicus.eu/dhus/odata/v1/Products('"
			         + self.id
			         + "')/Checksum/Value/$value")

		resp = requests.get(md5_link, auth=self.auth)
		expected = resp.text.lower()
		actual = md5(destination_file_path)
		if actual != expected:
			print('md5 checksum of {filename} failed, expected={expected}, actual={actual}'.format(
				  filename=filename, actual=actual, expected=expected),
			      file=sys.stderr)

		return destination_file_path

	def to_kml(self, top):
		def ExtendedDataElement(top, name, value):
			data = SubElement(top, 'Data')
			data.attrib['name'] = name
			return SubElementWithText(data, 'value', value)

		folder = SubElement(top, 'Folder')
		SubElementWithText(folder, 'name', self.attributes['beginposition'])

		placemark = SubElement(folder, 'Placemark')

		SubElementWithText(placemark, 'name', self.attributes['identifier'])

		time_span = SubElement(placemark, 'TimeSpan')
		SubElementWithText(time_span, 'begin', self.attributes['beginposition'])
		SubElementWithText(time_span, 'end', self.attributes['endposition'])

		extended_data = SubElement(placemark, 'ExtendedData')
		ExtendedDataElement(extended_data, 'Mode', self.attributes['sensoroperationalmode'])
		ExtendedDataElement(extended_data, 'ObservationTimeStart', self.attributes['beginposition'])
		ExtendedDataElement(extended_data, 'ObservationTimeStop', self.attributes['endposition'])
		ExtendedDataElement(extended_data, 'IngestionDate', self.attributes['ingestiondate'])
		ExtendedDataElement(extended_data, 'Id', self.id)
		ExtendedDataElement(extended_data, 'DownloadLink', self.product_link)

		linear_ring = SubElement(placemark, 'LinearRing')
		SubElementWithText(linear_ring, 'tessellate', 'true')
		SubElementWithText(linear_ring, 'altitudeMode', 'clampToGround')

		cords = re.match(r".*\(\((.*)\)\)", self.attributes['footprint']).group(1)
		cords = ' '.join([cord.replace(' ', ',') + ',0' for cord in cords.split(',')])
		SubElementWithText(linear_ring, 'coordinates', cords)

class ProductList:
	def __init__(self, products):
		self.products = products

	def __iter__(self):
		return self.products.__iter__()

	def to_kml(self):
		def prettify(elem):
			rough_string = et.tostring(elem, 'utf-8')
			reparsed = minidom.parseString(rough_string)
			return reparsed.toprettyxml(indent="\t")

		root = Element('kml')
		document = SubElement(root, 'Document')
		SubElementWithText(document, 'name', 'Products')

		for product in self.products:
			product.to_kml(document)

		return prettify(root)

class Search:
	def __init__(self, user, password):
		self.auth = (user, password)

	def __get_products(self, options):
		query = ' AND '.join([key + ':' + value for key, value in options.items()])
		base_url = 'https://scihub.copernicus.eu/dhus/search?start=0&rows=100&q='
		request_url = base_url + urllib.parse.quote(query)
		resp = requests.get(request_url, auth=self.auth)

		if resp.status_code != 200:
			print('Error, got HTTP status code: ' + str(resp.status_code))
			sys.exit(1)

		entries = et.fromstring(resp.text).findall('{http://www.w3.org/2005/Atom}entry')
		products = [Product(entry, self.auth) for entry in entries]

		return ProductList(products)

	def search_identifier(self, identifier):
		options = {'identifier': identifier}
		return self.__get_products(options)

	def search_position(self, position, days_back=7):
		(lat, lon) = position
		min_lat = str(lat - 5)
		min_lon = str(lon - 5)
		max_lat = str(lat + 5)
		max_lon = str(lon + 5)
		footprint = ('POLYGON(('
			+ min_lon + ' ' + max_lat + ', '
			+ max_lon + ' ' + max_lat + ', '
			+ max_lon + ' ' + min_lat + ', '
			+ min_lon + ' ' + min_lat + ', '
			+ min_lon + ' ' + max_lat + '))')
		options = {}
		options['ingestiondate'] = '[NOW-' + str(days_back) + 'DAYS TO NOW]'
		options['footprint'] = '"Intersects(' + footprint + ')"'
		options['productType'] = 'GRD'

		return self.__get_products(options)

def main():
	parser = argparse.ArgumentParser(description='Find ice.')
	parser.add_argument('--user', required=True)
	parser.add_argument('--password', required=True)
	parser.add_argument('--kml', action='store_true')
	parser.add_argument('--list', action='store_true')
	parser.add_argument('--download')
	parser.add_argument('--position', nargs=2, type=float, default=[62.00, 15.00])
	args = parser.parse_args()
	args.position = tuple(args.position)

	search = Search(args.user, args.password)

	if args.list:
		for product in search.search_position(args.position):
			print(product.name())
	elif args.kml:
		print(search.search_position(args.position).to_kml())
	elif args.download:
		products = search.search_identifier(args.download).products
		if len(products) == 1:
			products[0].download('.')
		else:
			print("Couldn't find " + args.download)

if __name__ == '__main__':
	main()
