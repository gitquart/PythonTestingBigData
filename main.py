import urllib.request
import json

contents=''
url='https://jsonplaceholder.typicode.com/photos'
contents = urllib.request.urlopen(url).read()
json_data=json.loads(contents)
print(len(json_data))