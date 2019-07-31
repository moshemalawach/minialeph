""" This is the simplest aleph network client available.
"""
from binascii import hexlify
import time
import requests
import json
import hashlib
from nuls import get_private_key, get_address, NulsSignature

DEFAULT_SERVER = "https://apitest.aleph.im"


def get_verification_buffer(message):
    """ Returns a serialized string to verify the message integrity
    (this is was it signed)
    """
    return '{chain}\n{sender}\n{type}\n{item_hash}'.format(**message)\
        .encode('utf-8')


def ipfs_push(content, api_server=DEFAULT_SERVER):
    resp = requests.post("%s/api/v0/ipfs/add_json" % api_server,
                         data=json.dumps(content))
    return resp.json().get('hash')


def broadcast(message, api_server=DEFAULT_SERVER):
    resp = requests.post("%s/api/v0/ipfs/pubsub/pub" % api_server,
                         json={'topic': 'ALEPH-TEST',
                               'data': json.dumps(message)})
    return resp.json().get('value')


def create_post(post_content, post_type, address=None,
                channel='TEST', private_key=None,
                api_server=DEFAULT_SERVER):
    if address is None:
        address = get_address(private_key=private_key)

    post = {
        'type': post_type,
        'address': address,
        'content': post_content,
        'time': time.time()
    }
    return submit(post, 'POST', channel=channel,
                  private_key=private_key, api_server=api_server)


def create_aggregate(key, content, address=None,
                     channel='TEST', private_key=None,
                     api_server=DEFAULT_SERVER):
    if address is None:
        address = get_address(private_key=private_key)

    post = {
        'key': key,
        'address': address,
        'content': content,
        'time': time.time()
    }
    return submit(post, 'AGGREGATE', channel=channel,
                  private_key=private_key, api_server=api_server)


def submit(content, message_type, channel='IOT_TEST',
           private_key=None, api_server=DEFAULT_SERVER,
           inline=True):

    if private_key is None:
        private_key = get_private_key()

    
    message = {
      #'item_hash': ipfs_hash,
      'chain': 'NULS',
      'channel': channel,
      'sender': get_address(private_key=private_key),
      'type': message_type,
      'time': time.time()
    }
    
    if inline:
        message['item_content'] = json.dumps(content, separators=(',',':'))
        h = hashlib.sha256()
        h.update(message['item_content'].encode('utf-8'))
        message['item_hash'] = h.hexdigest()
    else:
        message['item_hash'] = ipfs_push(content, api_server=api_server)
        
    sig = NulsSignature.sign_message(private_key,
                                     get_verification_buffer(message))
    message['signature'] = hexlify(sig.serialize()).decode('utf-8')
    print(message)
    broadcast(message, api_server=api_server)
    return message


def fetch_aggregate(address, key, api_server=DEFAULT_SERVER):
    resp = requests.get("%s/api/v0/aggregates/%s.json?keys=%s" % (
        api_server, address, key
    ))
    return resp.json().get('data', dict()).get(key)
