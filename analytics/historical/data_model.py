import datetime
from sqlite3 import IntegrityError
import pyodbc
import pandas as pd
from sqlite import create_connection

class CyclingAnalyticsDataModel:
    def __init__(self, file: str) -> None:
        self.conn = create_connection(file)

    def create_train(self, date, name='Indoor training', type='wattbike', user='dummy') -> int:
        sql = ''' INSERT INTO train(name,type,user,date)
              VALUES(?,?,?,?) '''
        try:
            cur = self.conn.cursor()
            cur.execute(sql, (name, type, user, date))
            self.conn.commit()

            return self.get_last_train_id()
        except IntegrityError as e:
            raise e
        
    def get_last_train_id(self) -> int:
        cur = self.conn.cursor()
        cur.execute('SELECT MAX(id) from train')
        result = cur.fetchone()
        return result[0]

    def save_train_fit_values(self, data: pd.DataFrame) -> None:
        try:
            cur = self.conn.cursor()
            cur.executemany('INSERT INTO train_fit_values(timestamp,variable,value,unit,train_id) VALUES (?,?,?,?,?)', data.values)
        except IntegrityError as e:
            raise e

    def get_train_by_id(self, train_id, train_type='wattbike') -> pd.DataFrame:
        cur = self.conn.cursor()
        sql = '''
                SELECT t.id as train_id, name, type, user, date, timestamp, variable, value, unit
                FROM train t join train_fit_values tfv on tfv.train_id = t.id
                WHERE t.id = (?) AND t.type = (?); '''
        cur.execute(sql, (train_id, train_type))
        db_results = cur.fetchall()
        results = []
        for row in db_results:
            results.append(list(row))

        data = pd.DataFrame(results, columns=['train_id', 'name', 'type', 'user', 'date', 'timestamp', 'variable', 'value', 'unit'])
        data['timestamp'] = data['timestamp'].apply(lambda x: datetime.datetime.strptime(x, "%m-%d-%Y %H:%M:%S"))
        data['date'] = data['date'].apply(lambda x: datetime.datetime.strptime(x, "%m-%d-%Y %H:%M:%S"))
        return data
    
    def get_train_by_user(self, user) -> pd.DataFrame:
        cur = self.conn.cursor()
        sql = '''
                SELECT t.id as train_id, name, type, user, date, timestamp, variable, value, unit
                FROM train t join train_fit_values tfv on tfv.train_id = t.id
                WHERE t.user = (?); '''
        cur.execute(sql, (user,))
        db_results = cur.fetchall()
        results = []
        for row in db_results:
            results.append(list(row))

        data = pd.DataFrame(results, columns=['train_id', 'name', 'type', 'user', 'date', 'timestamp', 'variable', 'value', 'unit'])
        data['timestamp'] = data['timestamp'].apply(lambda x: datetime.datetime.strptime(x, "%m-%d-%Y %H:%M:%S"))
        data['date'] = data['date'].apply(lambda x: datetime.datetime.strptime(x, "%m-%d-%Y %H:%M:%S"))
        return data


class PerfProDataModel:
    def __init__(self, file: str) -> None:
        self.conn = create_connection(file)

    def get_performance_by_user(self, user: str) -> pd.DataFrame:
        try:
            col_names = ['userName', 'CourseID', 'units', 'dateStamp', 'userWeight', 'minutes', 
            'aveSpeed', 'aveWatts', 'aveHR', 'aveRPMs', 'NormPower', 'aveCalories', 'avePowerL', 'avePowerR',
            'EWMA', 'TSS', 'aveLoad', 'MinLoad', 'MaxLoad', 'UserMaxWatts',
            'WattsTH5S'	,'WattsTH10S','WattsTH20S','WattsTH30S','WattsTH60S','WattsTH2M','WattsTH3M','WattsTH5M',
            'WattsTH8M','WattsTH10M','WattsTH15M','WattsTH20M','WattsTH30M','WattsTH45M','WattsTH60M',
            'PwrTimes', 'PwrRanges', 'HRTimes', 'HRRanges', 'PaceTimes', 'PaceRanges', 'UserLTHR', 'UserMaxHR'
            ]

            query = f'select {", ".join(col_names)} FROM performances p join perfzones pz on pz.PerfID = p.ID join wattagethresh wt on wt.PerfID = p.ID where p.userName = (?);'
            
            cursor = self.conn.cursor()
            cursor.execute(query, (user,))
            db_results = cursor.fetchall()
            results = []
            for row in db_results:
                results.append(list(row))
            return pd.DataFrame(results, columns=col_names)

        except Exception as e:
          raise e

    def get_performances(self) -> pd.DataFrame:
        try:
            col_names = ['userName', 'CourseID',  'units', 'dateStamp', 'userWeight', 'minutes', 
            'aveSpeed', 'aveWatts', 'aveHR', 'aveRPMs', 'NormPower', 'aveCalories', 'avePowerL', 'avePowerR',
            'EWMA', 'TSS', 'aveLoad', 'MinLoad', 'MaxLoad', 'UserMaxWatts',
            'WattsTH5S'	,'WattsTH10S','WattsTH20S','WattsTH30S','WattsTH60S','WattsTH2M','WattsTH3M','WattsTH5M',
            'WattsTH8M','WattsTH10M','WattsTH15M','WattsTH20M','WattsTH30M','WattsTH45M','WattsTH60M',
            'PwrTimes', 'PwrRanges', 'HRTimes', 'HRRanges', 'PaceTimes', 'PaceRanges', 'UserLTHR', 'UserMaxHR'
            ]
            query = f'select {", ".join(col_names)} FROM performances p join perfzones pz on pz.PerfID = p.ID join wattagethresh wt on wt.PerfID = p.ID'
            cursor = self.conn.cursor()
            cursor.execute(query)
            db_results = cursor.fetchall()
            results = []
            for row in db_results:
                results.append(list(row))
            return pd.DataFrame(results, columns=col_names)

        except Exception as e:
          raise e
    
    