import os
from datetime import datetime
from konlpy.tag import Okt
import pymongo

okt = Okt()
db_conn = None


def conn_db(address, database, collection):
    connection = pymongo.MongoClient(address)
    db = connection[database]
    coll = db[collection]
    coll.delete_many({})
    return coll


def get_datetime(msg):
    try:
        msg = msg.strip()
        idx = msg.index('일')
        sdate = msg[:idx + 1].strip()
        stime = msg[idx + 1:].strip()
        if stime[0:2] == '오전':
            return datetime.strptime(sdate + ' AM ' + stime[3:], '%Y년 %m월 %d일 %p %I:%M')
        if stime[0:2] == '오후':
            return datetime.strptime(sdate + ' PM ' + stime[3:], '%Y년 %m월 %d일 %p %I:%M')

        return None

    except ValueError:
        return None


def make_bulk_data(bulk_data, date, name, msg):
    ret = okt.pos(msg)

    data = {
        'date': date,
        'name': name,
        'msg': msg,
    }

    for i in ret:
        if i[1] == 'Foreign':
            continue
        elif i[1] not in data:
            data.update({
                i[1]: [i[0]]
            })
        else:
            data[i[1]].append(i[0])

    bulk_data.append(data)


def parse_talk(filepath, db_conn):
    f = open(filepath, 'r', encoding='utf-8')

    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(0, os.SEEK_SET)

    title = f.readline().strip()
    export_date = f.readline().strip()
    export_date = get_datetime(export_date[export_date.index(':') + 1:])
    start_date = None
    end_date = None

    date = None
    name = None
    msg = None
    bulk_data = list()

    progress = 0
    while True:
        line = f.readline()
        if not line:
            break
        if line in f.newlines:
            continue

        curr = int(f.tell() / size * 100)
        if curr != progress and curr <= 100:
            print('%d %%' % curr)
            progress = curr

        if len(bulk_data) > 1000:
            db_conn.insert_many(bulk_data)
            bulk_data.clear()

        try:
            idx = line.index(',')
            date = get_datetime(line[:idx])
            if date is None:
                raise ValueError

            if start_date is None:
                start_date = date
            end_date = date

            line = line[idx + 1:].strip()
            idx = line.index(':')
            name = line[:idx].strip()
            msg = line[idx + 1:].strip()

            make_bulk_data(bulk_data, date, name, msg)

        except ValueError:
            if date is None:
                continue
            if name is None:
                continue

            line = line.strip()
            if len(line) < 1:
                continue

            if get_datetime(line) is not None:
                continue

            make_bulk_data(bulk_data, date, name, line)
            continue

    f.close()

    db_conn.insert_many(bulk_data)

    print('title', title)
    print('start_date', start_date)
    print('end_date', end_date)
    print('export_date', export_date)


db_conn = conn_db('mongodb://192.168.50.152:27017', 'test', 'talk')
parse_talk('talk.txt', db_conn)

## msg count by date
print('날짜 순위')
pipeline = [
    {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d',
                    'date': '$date'
                }
            },
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    },
    {
        '$limit': 10
    }
]
for i in db_conn.aggregate(pipeline):
    print(i)

## msg count by hour
print('시간 순위')
pipeline = [
    {
        '$group': {
            '_id': {'$hour': '$date'},
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    }
]
for i in db_conn.aggregate(pipeline):
    print(i)

## msg count by name
print('메시지 순위')
pipeline = [
    {
        '$group': {
            '_id': '$name',
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    }
]
for i in db_conn.aggregate(pipeline):
    print(i)

## emoji count by name
print('이모티콘 순위')
pipeline = [
    {
        '$match': {'msg': '이모티콘'}
    },
    {
        '$group': {
            '_id': '$name',
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    }
]
for i in db_conn.aggregate(pipeline):
    print(i)

## pic count by name
print('사진 순위')
pipeline = [
    {
        '$match': {'msg': '사진'}
    },
    {
        '$group': {
            '_id': '$name',
            'count': {'$sum': 1}
        }
    },
    {
        '$sort': {'count': -1}
    }
]
for i in db_conn.aggregate(pipeline):
    print(i)

## word top
print('단어 순위')
pipeline = [
    {
        '$project': {
            '_id': False,
            'tags': {
                '$concatArrays': [
                    {'$ifNull': ['$Noun', []]},
                    # {'$ifNull': ['$KoreanParticle', []]},
                    # {'$ifNull': ['$Adjective', []]},
                    # {'$ifNull': [ '$Josa', [] ]},
                    # {'$ifNull': [ '$Verb', [] ]},
                    # {'$ifNull': ['$Punctuation', []]},
                    # {'$ifNull': [ '$Conjunction', [] ]},
                    # {'$ifNull': [ '$Suffix', [] ]},
                    # {'$ifNull': [ '$Adverb', [] ]},
                    # {'$ifNull': [ '$VerbPrefix', [] ]},
                    # {'$ifNull': [ '$Adjective', [] ]},
                    # {'$ifNull': [ '$Determiner', [] ]},
                    # {'$ifNull': [ '$Modifier', [] ]},
                    # {'$ifNull': [ '$Number', [] ]},
                ]
            }
        }
    },
    {
        '$unwind': {'path': '$tags'}
    },
    {
        '$group': {
            '_id': '$tags',
            'count': {'$sum': 1.0}
        }
    },
    {
        '$sort': {'count': -1}
    },
    {
        '$limit': 30
    }
]
for i in db_conn.aggregate(pipeline):
    print(i)
