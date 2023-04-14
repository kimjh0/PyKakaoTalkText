import os
import re
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
    msg = msg.strip()
    idx = msg.index('요일')
    return datetime.strptime(msg[:idx - 1].strip(), '%Y년 %m월 %d일')


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
    export_date = f.readline()
    export_date = datetime.strptime(
        export_date[export_date.index(':') + 1:].strip(), '%Y-%m-%d %H:%M:%S')
    start_date = None
    end_date = None

    date = None
    name = None
    msg = None
    bulk_data = list()

    date_pattern = re.compile(r'^-{15} \d{4}년 \d{1,2}월 \d{1,2}일 \w요일 -{15}$')
    msg_pattern = re.compile(r'\[(.*?)\] \[(.*?)\] (.*)')

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

        if re.match(date_pattern, line):
            date = get_datetime(line[15:len(line)-16])
            continue

        msg_match = re.match(msg_pattern, line)
        if msg_match:
            if name is not None:
                make_bulk_data(bulk_data, date, name, msg)

            name = msg_match.group(1)
            time = msg_match.group(2)
            msg = msg_match.group(3)

            am_pm, time_str = time.split()
            hour_str, minute_str = time_str.split(":")

            hour = int(hour_str)
            if am_pm == "오후" and hour != 12:
                hour += 12
            elif am_pm == "오전" and hour == 12:
                hour = 0

            date = date.replace(hour=hour, minute=int(minute_str))
        else:
            msg += line

        if start_date is None:
            start_date = date
        end_date = date

    f.close()

    if len(bulk_data) > 1:
        db_conn.insert_many(bulk_data)

    print('title', title)
    print('start_date', start_date)
    print('end_date', end_date)
    print('export_date', export_date)


db_conn = conn_db('mongodb://localhost:27017', 'test', 'talk')
parse_talk('talk.txt', db_conn)

# msg count by date
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

# msg count by hour
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

# msg count by name
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

# emoji count by name
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

# pic count by name
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

# word top
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
