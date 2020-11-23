import pymongo
from pymongo import MongoClient

class Persistence:
    def __init__(self):
        client = MongoClient()
        self.db = client.db_final
        self.driver_collection = self.db.driver_collection
        self.route_collection = self.route_collection
        self.assignment_collection = self.db.assignment_collection
    

    def get_assignment_by_index(self, index):
        query = {'index': index}
        result = self.assignment_collection.find_one(query)
        return result

    """
    The program should get the name of a driver (first and last), and print out the driver’s
    information, together with the route that the driver is assigned to. (If there is multiple drivers
    that satisfies the query, print them all (one after the other)).
    """
    def get_by_name(self, first_name, last_name):

        #Get the ids from driver_collection like first_and last name 

        #Get the docs with one of those ids from assignment_collection
        print("get_by_name")

    """
    The program should get the name of a city, and print out all the routes that go through the city
    (separate departure and destination, order by time: assume Sunday 00:00 am is the starting time). 
    """
    def get_by_city(self, city):
        #Get the route ids that have city in to: OR from: fields in route_collection

        #return all docs with one of those ids in assignment_collection

        print("get_by_city")

    
    """
    The program should get the route of a bus and print out all information about the route,
    including the name and ID of the driver that is assigned to it
    """
    def get_by_route(self, route):
        #get the route information, including its id (maybe check dont get multiple names back) out of route_collection

        #go look in assignments by route_id and get all assignements (retrieving driver ids)

        #get get out all the drivers that are in the list of assignment ids

        print("get_by_route")


    """
    The program should get the name of two cities, and respond if there is a bus route that go from
    the first city directly to the second. If so, print all info about that route (as the previous query). If
    there is more than one, print them all (one after the other)
    NOTE: this is a direct question, not a does a route exist question
    """
    def get_is_there_a_route(self, city_a, city_b):
        #do a query on routes_collection with city_a in from: and city_b in to:
        
        #do a query on routes_collection with city_b in from: and city_a in to:

        #Merge the two lists by ids

        #return the merged list

        print("get_is_there_a_route")

