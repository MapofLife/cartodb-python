
from cartodb import CartoDB
import codecs
import simplejson

settings = simplejson.loads(
    codecs.open('cartodb.json', encoding='utf-8').read(),
    encoding='utf-8'
)

cdb = CartoDB(
    settings['CONSUMER_KEY'],
    settings['CONSUMER_SECRET'],
    settings['user'],
    settings['password'],
    settings['cartodb_domain']
)

sql = "INSERT INTO %(table_name)s (name) VALUES ('%(name)s')" % {
    'table_name': 'temp',
    'name': 'Any name you choose (GET)'
}
sql = "SELECT current_database(), user"

print "Sending GET request: " + sql
print "Response: " + cdb.sql(sql, False).__str__()

sql = "INSERT INTO %(table_name)s (name) VALUES ('%(name)s')" % {
    'table_name': 'temp',
    'name': 'Any name you choose (POST)'
}
sql = "SELECT current_database(), user"

print "Sending POST request: " + sql
print "Response: " + cdb.sql_post(sql).__str__()
