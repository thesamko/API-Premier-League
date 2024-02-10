from flask import Flask, render_template
from flask_restful import reqparse, Api, Resource
import json
import pyodbc
import socket
from threading import Lock
from functools import wraps


app = Flask(__name__, template_folder="template")

api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('player')

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:[redacted].database.windows.net,1433;Database=[redacted];Uid=[redacted];Pwd=[redacted];Encrypt=yes;')

def retry(delay=2, retries=4):
    def retry_decorator(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            opt_dict = {'retries': retries, 'delay': delay}
            while opt_dict['retries'] > 1:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    msg = "Exception: {}, Retrying in {} seconds...".format(e, delay)
                    print(msg)
                    time.sleep(opt_dict['delay'])
                    opt_dict['retries'] -= 1
            return f(*args, **kwargs)

        return f_retry

    return retry_decorator

class ConnectionManager(object):    
    __instance = None
    __connection = None
    __lock = Lock()

    def __new__(cls):
        if ConnectionManager.__instance is None:
            ConnectionManager.__instance = object.__new__(cls)        
        return ConnectionManager.__instance       
    
    def __getConnection(self):
        if (self.__connection == None):
            application_name = ";APP={0}".format(socket.gethostname())  
            self.__connection = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:sami-epl-landing.database.windows.net,1433;Database=epl_landing;Uid=admin_epl;Pwd={Z@bavn0sladko};Encrypt=yes;')
        return self.__connection

    def __removeConnection(self):
        self.__connection = None

    @retry()
    def executeQueryJSON(self, procedure, payload=None):
        result = {}  
        try:
            conn = self.__getConnection()

            cursor = conn.cursor()
            
            if payload:
                cursor.execute(f"EXEC {procedure} ?", json.dumps(payload))
            else:
                cursor.execute(f"EXEC {procedure}")

            result = cursor.fetchone()

            if result:
                result = json.loads(result[0])                           
            else:
                result = {}

            cursor.commit()    
        except pyodbc.OperationalError as e:            
            app.logger.error(f"{e.args[1]}")
            if e.args[0] == "08S01":
                # If there is a "Communication Link Failure" error, 
                # then connection must be removed
                # as it will be in an invalid state
                self.__removeConnection() 
                raise                        
        finally:
            cursor.close()
                         
        return result

class Queryable(Resource):
    def executeQueryJson(self, verb, payload=None):
        result = {}  
        entity = type(self).__name__.lower()
        procedure = f"web.{verb}_{entity}"
        result = ConnectionManager().executeQueryJSON(procedure, payload)
        return result

class Player(Queryable):
    def get(self, player_id):
        player = {'PlayerID': player_id}
        result = self.executeQueryJson("get", player)
        #cursor = conn.cursor()
        #cursor.execute("EXEC web.get_player ?", json.dumps(player))
        #result = json.loads(cursor.fetchone()[0])
        #cursor.close()
        return result, 200

class Players_id(Queryable):
    def get(self):
        pl_result = self.executeQueryJson("get")
        #cursor = conn.cursor()
        #cursor.execute("EXEC web.get_players_id")
        #pl_result = json.loads(cursor.fetchone()[0])
        #cursor.close()
        return pl_result, 200

class Team(Queryable):
    def get(self, team_id):
        team = {'TeamID': team_id}
        team_result = self.executeQueryJson("get", team)
        #cursor = conn.cursor()
        #cursor.execute("EXEC web.get_team ?", json.dumps(team))
        #team_result = json.loads(cursor.fetchone()[0])
        #cursor.close()
        return team_result, 200

class Teams_id(Queryable):
    def get(self):
        teams_result = self.executeQueryJson("get")
        #cursor = conn.cursor()
        #cursor.execute("EXEC web.get_teams_id")
        #teams_result = json.loads(cursor.fetchone()[0])
        #cursor.close()
        return teams_result, 200

api.add_resource(Player, '/player', '/player/<player_id>')
api.add_resource(Players_id, '/all_players', '/all_players')
api.add_resource(Team, '/team', '/team/<team_id>')
api.add_resource(Teams_id, '/all_teams', '/all_teams')

@app.route("/")
def home():
    return render_template("index.html")

