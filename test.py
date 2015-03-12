import json
import Message

r = Message.Request("insert 1 2 3")
json_obj= json.dumps(r)
print json