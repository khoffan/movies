from sqlalchemy import create_engine


connect_server = {
    "user": "root",
    "password": "",
    "host": "localhost",
    "db":"test_server_db"
}

def connect_db():
    try:
        engine = create_engine('mysql+pymysql://' + connect_server['user'] + ':' + connect_server['password'] + '@' + connect_server['host'] + '/' + connect_server['db'], pool_recycle=3600)
        return engine
    except Exception as e:
        print(str(e))