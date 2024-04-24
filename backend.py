import sqlite3
import csv

conn = sqlite3.connect('events.db')
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    event_id TEXT,
    title TEXT,
    sentiment TEXT,
    description TEXT,
    author_id INTEGER,
    date TEXT
)
''')

with open('./events.csv', 'r') as csvfile:
    dr = csv.DictReader(csvfile)
    to_db = [(i['eventId'], i['title'], i['sentiment'], i['description'], i['authorId'], i['timestamp']) for i in dr]

cur.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON events (event_id);')
cur.execute('CREATE INDEX IF NOT EXISTS idx_sentiment ON events (sentiment);')

cur.executemany("INSERT INTO events (event_id, title, sentiment, description, author_id, date) VALUES (?, ?, ?, ?, ?, ?);", to_db)
conn.commit()

conn.close()


from flask import Flask, request, jsonify, after_this_request
from flask_restful import Api, Resource
from flask_cors import CORS
import json 
import pandas as pd
import ast
import random
from sqlalchemy import create_engine, Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlite3
from datetime import datetime

Base = declarative_base()
class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True)
    event_id = Column(String)
    title = Column(String)
    sentiment = Column(Integer)
    description = Column(String)
    author_id = Column(Integer)

    

engine = create_engine('sqlite:///events.db')
Session = sessionmaker(bind=engine)

app = Flask(__name__)
CORS(app)
api = Api(app)


class deleteEventId(Resource):
    # ORM
    def get(self, id):
        session = Session()
        try:
            session.connection(execution_options={'isolation_level': 'SERIALIZABLE'})
            event_to_delete = session.query(Event).filter(Event.event_id == id).first()
            session.delete(event_to_delete)
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()




class addEvent(Resource):
    # ORM
    def get(self, title, sentiment, description):
        session = Session()
        try:
            session.connection(execution_options={'isolation_level': 'SERIALIZABLE'})
            eventId = '00002E'+str(random.randint(1,100000))
            authorId = 7
            new_event = Event(event_id=eventId, title=title, sentiment=sentiment, description=description, author_id=authorId)
            session.add(new_event)
            session.commit()
            return {'a':'good'}
        except:
            session.rollback()
            return {'a':'error'}
        finally:
            session.close()
        



class editEvent(Resource):
    #ORM
    def get(self, eventId, title, description, sentiment):
        session = Session()
        try:
            session.connection(execution_options={'isolation_level': 'SERIALIZABLE'})
            event_to_update = session.query(Event).filter(Event.event_id == eventId).first()
            event_to_update.title = title
            event_to_update.sentiment = int(sentiment[:-1])
            event_to_update.description = description
            session.commit()
            session.close()
            return {'data': 'event updated'}
        except:
            session.rollback()
            session.close()
            return {'data':'failed'}


class getFeedContents(Resource):
    # ORM
    def get(self):
        session = Session()
        try:
            session.connection(execution_options={'isolation_level': 'READ UNCOMMITTED'})
            events = session.query(Event).all()
            # Convert to a list of dictionaries
            events_list = [ 
                { 
                    'id': event.id,
                    'eventId': event.event_id,
                    'title': event.title,
                    'sentiment': event.sentiment,
                    'description': event.description,
                    'authorId': event.author_id,
                } for event in events ]
        
            json_data = json.loads(json.dumps(events_list))  # This step mimics ast.literal_eval() on a JSON string
            session.commit()
            session.close()
            return {'data': json_data}
        except Exception as e:
            session.rollback()
            session.close()
            return {'data': str(e)}
        
        
        
        
    
class filterFeedContents(Resource):
    def get(self,range):
        mins,maxs = range.split(',')
        mins, maxs = int(mins),int(maxs)
        conn = sqlite3.connect('events.db')
        cur = conn.cursor()
        query = "SELECT * FROM events WHERE sentiment >= ? AND sentiment <= ?"
        cur.execute(query, (mins, maxs))
        events = cur.fetchall()
        cur.close()
        conn.close()
        json_data = json.loads(json.dumps(events))
        jx = []
        for j in json_data:
            jx.append({})
            jx[-1]['eventId'] = j[1]
            jx[-1]['title'] = j[2]
            jx[-1]['sentiment'] = j[3]
            jx[-1]['description'] = j[4]
            jx[-1]['author'] = j[5]
        return {'data':jx}

api.add_resource(deleteEventId, "/delete/<string:id>")
api.add_resource(addEvent, "/add/<string:title>/<string:sentiment>/<string:description>")
api.add_resource(editEvent, "/edit/<string:eventId>/<string:title>/<string:description>/<string:sentiment>")
api.add_resource(filterFeedContents, "/filter/<string:range>")
api.add_resource(getFeedContents, "/getFeedContents")
app.run(host='0.0.0.0', port=8080)
