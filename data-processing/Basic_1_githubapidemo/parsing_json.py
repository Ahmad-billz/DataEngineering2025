import json
data = '{"name":"Tanisha", "age":29}'
print(data)
details = json.loads(data)
print(details['name'], details['age'])

#dumping Data into a file
with open("data.json", "w") as f:
    json.dump(details, f)   

with open("data.json",'r') as f:
    details = json.load(f)
    print(json.dumps(details, indent=4))