import redis

r = redis.Redis(host='6.tcp.eu.ngrok.io', port=18294, decode_responses=True)

r.set('foo', 'bar')

print(r.get("foo"))
