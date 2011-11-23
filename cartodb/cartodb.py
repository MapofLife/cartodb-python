# -*- encoding: utf-8 -*-

"""
  ** CartoDBClient **

    A simple CartoDB client to perform requests against the CartoDB API.
    Internally it uses OAuth

  * Requirements:

     python 2.5
     pip install oauth2
     pip install simplejson # if you're running python < 2.6

  * Example use:
        user =  'your@mail.com'
        password =  'XXXX'
        CONSUMER_KEY='XXXXXXXXXXXXXXXXXX'
        CONSUMER_SECRET='YYYYYYYYYYYYYYYYYYYYYYYYYY'
        cartodb_domain = 'vitorino'
        cl = CartoDB(CONSUMER_KEY, CONSUMER_SECRET, user, password, cartodb_domain)
        print cl.sql('select * from a')

"""

import urlparse
import oauth2 as oauth
import urllib
import httplib2
import sys

try:
    import json
except ImportError:
    import simplejson as json

from oauth2 import Request

REQUEST_TOKEN_URL = '%(domain)s/oauth/request_token'
ACCESS_TOKEN_URL = '%(domain)s/oauth/access_token'
AUTHORIZATION_URL = '%(domain)s/oauth/authorize'
# RESOURCE_URL = '%(domain)s/api/v1/sql'
RESOURCE_URL = '%(domain)s/api/v1/queries'


class CartoDBException(Exception):
    pass

class CartoDB(object):
    """ basic client to access cartodb api """

    def __init__(self, key, secret, email, password, cartodb_domain, domain=None):

        self.consumer_key = key
        self.consumer_secret = secret
        consumer = oauth.Consumer(self.consumer_key, self.consumer_secret)

        client = oauth.Client(consumer)
        client.set_signature_method = oauth.SignatureMethod_HMAC_SHA1()

        params = {}
        params["x_auth_username"] = email
        params["x_auth_password"] = password
        params["x_auth_mode"] = 'client_auth'

        # Set the server domains for the URLs
        if domain is None:
            # If no domain specified, use the user's account name
            # on CartoDB itself.
            domain = 'https://%s.cartodb.com' % (cartodb_domain)

        self.request_token_url = REQUEST_TOKEN_URL % {'domain': domain}
        self.access_token_url = ACCESS_TOKEN_URL % {'domain': domain}
        self.authorization_url = AUTHORIZATION_URL % {'domain': domain}
        self.resource_url = RESOURCE_URL % {'domain': domain}

        # Get Access Token
        # print "Connecting to '%s'" % self.access_token_url
        resp, token = client.request(self.access_token_url, method="POST", body=urllib.urlencode(params))
        if resp['status'] == '401':
            raise CartoDBException("CartoDB username or password invalid: access denied.")
        access_token = dict(urlparse.parse_qsl(token, False, True))
        token = oauth.Token(access_token['oauth_token'], access_token['oauth_token_secret'])

        # prepare client
        self.client = oauth.Client(consumer, token)


    def req(self, url, body="", http_method="GET", http_headers=None):
        """ make an autorized request """
        resp, content = self.client.request(
            url,
            body=body,
            method=http_method,
            headers=http_headers
        )
        return resp, content

    def sql(self, sql, parse_json=True):
        """ executes sql in cartodb server
            set parse_json to False if you want raw reponse
        """
        p = urllib.urlencode({'sql': sql})
        url = self.resource_url
        print "Making a request for '%s'" % url
        resp, content = self.req(url, body=p);
        print "Response: %s, content: %s" % (resp, content)
        if resp['status'] == '200':
            if parse_json:
                return json.loads(content)
            return content
        elif resp['status'] == '400':
            json = json.loads(content)
            if 'error' in json:
                raise CartoDBException(json['error'])
            if 'errors' in json:
                raise CartoDBException(json['errors'])
            raise CartoDBException(json)
        elif resp['status'] == '500':
            raise CartoDBException('internal server error')

        return None



