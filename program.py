import itertools
import os
import sys
import traceback
from datetime import datetime
import time
import functools
from pprint import pprint
from bson.code import Code

from haversine import haversine, Unit
from tabulate import tabulate

from DbConnector import DbConnector


class Program:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.keysToSkip = []
        self.tpsToAdd = []
        self.acitivityTpsToAdd = []
        self.activity_id_counter = 0
        self.userColl = self.db['User']
        self.activityColl = self.db['Activity']
        self.trackpointColl = self.db['Trackpoint']

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()
        print(f'Dropped collection: {collection}')
    
    def cleanDB(self):
        """Drop all tables and create them again"""
        collections = ['User', 'Trackpoint', 'Activity']

        for coll in collections:
            self.drop_coll(coll)
        
        print('################################################################')

        for coll in collections:
            self.create_coll(coll)
        
        print('Cleaned DB')

    def readIds(self):
        """Read what users has labels"""
        with open('./dataset/labeled_ids.txt') as f:
            return f.readlines()

    def insertIntoActivityWithLabels(self, activities, trackpoints, user):
        """Inserts into activities table AND finds corresponding trackpoints"""
        while True:
            activity_to_add = None
            try:
                tp = next(trackpoints)
                for activity in activities: # Loop through activities
                    yyyy = tp[0][0:4] # Store year of first trackpoint in file
                    mm = tp[0][4:6] # Store month of first trackpoint in file
                    dd = tp[0][6:8] # Store day of first trackpoint in file
                    hh = tp[0][8:10] # Store hour of first trackpoint in file
                    m = tp[0][10:12] # Store minutes of first trackpoint in file
                    ss = tp[0][12:14]  # Store seconds of first trackpoint in file
                    date_trackpoint = f'{yyyy}/{mm}/{dd} {hh}:{m}:{ss}'
                    
                    if date_trackpoint == activity[0] and activity[1] == tp[-1][-1]: # If we have a full match on activity start and end
                        self.keysToSkip.append(tp[0]) # We skip this trackpoint later on
                        activity_to_add = {
                            'transportation_mode': activity[-1],
                            'start_date_time': activity[0],
                            'end_date_time': activity[1],
                            'user_id': user
                        }
                if activity_to_add is not None:
                    tmp_tps = [activity_to_add]
                    for trackpoint in tp: # Loop through all trackpoints in this activity
                        if type(trackpoint) == list:
                            tmp_tps.append(tuple(trackpoint))
                    self.acitivityTpsToAdd.append(tmp_tps)
            except StopIteration:
                break
            
        
    def prepareTrackPoints(self, trackpoints, activity=False, user=None):
        """Inserts new acties to Activity table and prepares trackpoints for insert"""
        while True:
            try:
                tps = next(trackpoints) # get next from generator
                if not activity and tps[0] not in self.keysToSkip:  # If we are not to skip this activity and it is not an activity with label
                    a = {
                        'transportation_mode': None,
                            'start_date_time': tps[1][-1],
                            'end_date_time': tps[-1][-1],
                            'user_id': user
                    }
                    tmp_tps = [a] # We get the last insert id.
                    for tp in tps: # For all trackpoints in the activity
                        if type(tp) != str:
                            tmp_tps.append(tuple(tp))
                    self.tpsToAdd.append(tmp_tps)


            except StopIteration:
                break

    def readLabels(self, path):
        activities = []  # Init empty activity list
        with open(path) as f: # Read file at path
            lines = f.readlines()[1:] # Skip header
            for line in lines: # Loop through lines in file
                l = tuple(map(lambda x: x.strip(), line.split('\t'))) # Get each row as elements in list
                activities.append(l) # Add activity to list
        return activities # Return list

    def readTrackPoints(self, paths, root):
        # print('HEEEY')
        trackpoints = {}  # Init empty trackpoints dict
        # print(path)
        for path in paths:  # Loop through files in the path
            # if '021' in root: print(path)
            with open(root + '/' + path, 'r') as f:
                lines = f.read().splitlines(True)[6:]  # Skip headers
                if len(lines) <= 2500:  # If the file are more than 2500 lines, skip it
                    # print(len(lines))
                    tmp = [path.split('.')[0]] # Init temp list for holding trackpoints 
                    for line in lines:  # Loop through every line in the file
                        if len(line) > 0:
                            l = list(map(lambda x: x.strip(), line.split(','))) # Split the line to get a list of the elements, strip \n and \t
                            l[-2] = (l[-2] + ' ' + l[-1]).replace('-', '/')  # Convert date and time to datetime string
                            del l[2] # Delete third element, we don't need it
                            del l[-1] # Delete last value, because it's merged with l[-2]
                            tmp.append(l)
                    yield tmp

    def insertTrackpointsWithActivities(self, trackpoints, users):
        count = 0
        for tps in trackpoints:
            print(f'Trackpoint {count}/{len(trackpoints)}. {round(count/len(trackpoints)*100, 2)}% done')

            self.activity_id_counter += 1
            activity = tps[0]
            activity['_id'] = self.activity_id_counter

            trackpointColl = self.db['Trackpoint']
            activityColl = self.db['Activity']

            tps_to_add = map(lambda x: {
                'activity_id': self.activity_id_counter,
                'lat': x[0],
                'lon': x[1],
                'altitude': x[2],
                'date_days': x[3],
                'date_time': x[4]
            }, tps[1:])

            res = trackpointColl.insert_many(tps_to_add)

            activity['trackpoints'] = res.inserted_ids
            
            res = activityColl.insert_one(activity)
            list(filter(lambda x: x['_id'] == activity['user_id'], users))[0]['activities'].append(res.inserted_id)
            count += 1


    def insertData(self):
        users = [] # Create user dictionaty
        labeledUsers = tuple(map(lambda x: str(x.strip()), self.readIds())) # Find all users that has label
        activities = {} # Create empty activities dict
        trackpoints = {} # Create empty trackpoints dict
        num = 1 # Just for percentage printing

        for root, dirs, files in os.walk('./dataset/Data'):  # Loop through folders
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue
            userid = str(root.split('/')[3])  # Get user id from folder name
            if userid not in list(map(lambda x: x['_id'], users)):
                users.append({'_id': userid, 'hasLabels': userid in labeledUsers, 'activities': []})

        count = 0
        for root, dirs, files in os.walk('./dataset/Data'):
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue

            userid = str(root.split('/')[3]) # Get user id from folder name

            if 'labels.txt' in files: # If the user has labeled activities ..
                print(f'Labels for user {count}/69 - {round(count/69 * 100, 2)}% done')
                print(root)
                self.insertIntoActivityWithLabels(self.readLabels(f'{root}/labels.txt'), self.readTrackPoints(os.listdir(root + '/Trajectory'), root + '/Trajectory'), userid)
                count += 1
        
        for root, dirs, files in os.walk('./dataset/Data'):
            if len(dirs) > 0 and len(files) == 0:  # Skip folders where the only folder are 'Trajectory'
                continue

            userid = str(root.split('/')[3])  # Get user id from folder name
            
            if "Trajectory" in root: # If we are in a trajectory folder ...
                print(f'Reading trackpoints for user {userid} - {round(num/182 * 100, 2)}% done')
                self.prepareTrackPoints(self.readTrackPoints(files, root), user=userid) # Read trackpoints
                num += 1
        
        print('Inserting labeled trackpoints...')

        self.insertTrackpointsWithActivities(self.acitivityTpsToAdd, users)

        print('Inserted labeled activities with trackpints')
        print('Inserting rest of trackpoints...')
        
        self.insertTrackpointsWithActivities(self.tpsToAdd, users)
        
        print('Inserted trackpoints')

        print('Inserting users')
        usersColl = self.db['User']
        usersColl.insert_many(users)
        print('Inserted Users')


    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find()
        return documents

    def part2Task1(self):
        users = self.userColl.count_documents({})
        activities = self.activityColl.count_documents({})
        trackpoints = self.trackpointColl.count_documents({})

        print(f'There are {users} users, {activities} activities and {trackpoints} trackpoints in the database')
    
    def part2Task2(self):
        users = self.fetch_documents('User')
        usersCount = self.userColl.count_documents({})
        activityCount = 0
        for user in users:
            activityCount += len(user['activities'])
        
        print(f'Average number of activities for users are {activityCount/usersCount}')
    
    def part2Task3(self):
        users = list(self.userColl.aggregate([
            {'$unwind': '$activities'},
            {'$group': {'_id': '$_id', 'Activity count': {'$sum': 1}}},
            {'$sort': {'Activity count': -1}},
            {'$limit': 20}
        ]))

        pprint(users)

    def part2Task4(self):
        taxiActivities = list(map(lambda x: x['user_id'], list(self.activityColl.find({'transportation_mode': 'taxi'}, {'user_id': 1, '_id': 0}))))
        users = self.userColl.find({'_id': {'$in': taxiActivities}}, {'_id': 1})
        pprint(list(users))

    def part2Task5(self):
        mapper = Code("""
            function () {
                if(this.transportation_mode !== null){
                    emit(this.transportation_mode, 1);
                }
            }
        """)

        reducer = Code("""
            function (key, values) {
                var total = 0;
                for(var i = 0; i < values.length; i++) {
                    total += values[i];
                }
                return total;
            } 
        """ )

        modes = self.activityColl.map_reduce(mapper, reducer, "res")
        for doc in modes.find().sort("value"):
            pprint(doc)
    

def main():
    # try:
    program = Program()
    # program.cleanDB()
    # program.insertData()
    # program.part2Task1()
    # program.part2Tak2()
    # program.part2Task3()
    # program.part2Task4()
    program.part2Task5()



if __name__ == '__main__':
    main()
