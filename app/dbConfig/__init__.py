from sqlalchemy import create_engine


# connect_server = {
#     "user": "root",
#     "password": "",
#     "host": "localhost",
#     "db":"test_server_db"
# }

connect_server = {
    "user": "naru",
    "password": "546326",
    "host": "192.168.74.207",
    "port": "3312",
    "db": "movies"
}

def connect_db():
    try:
        engine = create_engine('mysql+pymysql://' + connect_server['user'] + ':' + connect_server['password'] + '@' + connect_server['host'] + '/' + connect_server['db'], pool_recycle=3600)
        return engine
    except Exception as e:
        print(str(e))