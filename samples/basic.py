
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

print "Which database and user are we running under with GET?"
sql = "SELECT current_database(), user;"
response = cdb.sql(sql)
print "Response: " + response.__str__()
print "\tDatabase: " + response['rows'][0]['current_database']
print "\tUser: " + response['rows'][0]['current_user']
print

sql = "SELECT current_database(), user;"

print "Which database and user are we running under with POST?"
print "Sending POST request: " + sql
response = cdb.sql_post(sql)
print "Response: " + response.__str__()
print "\tDatabase: " + response['rows'][0]['current_database']
print "\tUser: " + response['rows'][0]['current_user']
