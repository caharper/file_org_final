import pymongo
from pymongo import MongoClient

class Persistence:
    
    
    def __init__(self):
        client = MongoClient()
        # database name 
        self.db = client.bus_db
        # driver collection
        self.driver_collection = self.db.drivers
        # route collection
        self.route_collection = self.db.routes
        # assignment collection
        self.assignment_collection = self.db.assignments
    

    def get_assignment_by_index(self, index):
        query = {'index': index}
        result = self.assignment_collection.find_one(query)
        return result

    """
    The program should get the name of a driver (first and last), and print out the driver’s
    information, together with the route that the driver is assigned to. (If there are multiple drivers
    that satisfies the query, print them all (one after the other)).
    """
    def get_by_name(self, first_name, last_name):
        # print(first_name, last_name)
        
        # construct query for the first and last name
        query = {'$and':[
            {"FirstName": {"$in" :[first_name]}},
            {"LastName": {"$in" :[last_name]}},
            ]}

        # search through the driver collection for the first and last name query
        cursor_driver = self.driver_collection.find(query)

        # ID set for all the driver ids we get
        id_set = set()
        # Maps ID to actual driver, to assignment dictionary
        driver_assignment_dict = {}
        # Dictionary storing all routes for a driver
        driver_route_dict = {}
        # stores each driver and their information, just more readable than cursor_driver
        driver_information = list()
        for doc in cursor_driver:
            id_set.add(doc.get("ID"))
            driver_assignment_dict[doc.get("ID")] = set()
            driver_route_dict[doc.get("ID")] = list()
            driver_information.append(doc)
        
        # Construct query for driver id based on results from query 1
        query = {"DriverID": {"$in": list(id_set)}}

        # search through assignment collection  for the driverid query
        cursor_assignment = self.assignment_collection.find(query)

        # set of all routes that were found in assignment collection 
        route_set = set()
        for doc in cursor_assignment:
            # add to the set if not already in it 
            route_set.add(doc.get("RouteNumber"))
            # add this to the set that the driver has been on
            driver_assignment_dict[doc.get("DriverID")].add(doc.get("RouteNumber"))

        
        # construct query for routenumber based on the routeid that were returned from route collectuon
        query = {"RouteNumber": {"$in": list(route_set)}}

        # search through route collection to find all routes that have been assigned to the drivers with names first, last
        cursor_routes = self.route_collection.find(query)
        
        # for each route
        for doc in cursor_routes:
            # for each driver
            for driver in driver_assignment_dict:
                # if the route number is in the drivers dictionary add it to their routes
                if doc.get("RouteNumber") in driver_assignment_dict[driver]:
                    driver_route_dict[driver].append(doc)
        
        # return a list of driver with their corresponding routes they are assignmned to 
        to_Return = list()

        for driver in driver_information:

            theid = driver.get("ID")
            to_Return.append((driver, driver_route_dict[theid]))
        
        return to_Return

        


    """
    The program should get the name of a city, and print out all the routes that go through the city
    (separate departure and destination, order by time: assume Sunday 00:00 am is the starting time). 
    """
    def get_by_city(self, city):
        #Get the route ids that have city in to: OR from: fields in route_collection

        # need to do two queries to seperate out destination and departure parts
        
        # query for destination city 
        query1 = {"DestinationCity": {"$in" :[city]}}
        
        # query for departure city
        query2 = {"DepartureCity": {"$in" :[city]}}
        
        
        cursor1 = self.route_collection.find(query1)
        
        cursor2 = self.route_collection.find(query2)

        # Set of destination ids
        destination_id_set = set()
        # Set of departure ids
        departure_id_set = set()

        # maps destination routenumber to a routes information information
        destination_content = {}
        # maps destination routenumber to a map of assignments
        destination_dict = {}
        
        # for document in destination cities
        for doc in cursor1:
            destination_id_set.add(doc.get("RouteNumber"))
            destination_dict[doc.get("RouteNumber")] = list()
            destination_content[doc.get("RouteNumber")] = doc
        # maps departure routenumber to a routes information information
        departure_content = {}
        # maps departure routenumber to a map of assignments
        departure_dict = {}
        # for document in departure cities
        for doc in cursor2:
            departure_id_set.add(doc.get("RouteNumber"))
            departure_dict[doc.get("RouteNumber")] = list()
            departure_content[doc.get("RouteNumber")] = doc
        # query for destination routenumber in assignments
        query3 = {"RouteNumber": {"$in": list(destination_id_set)}}
        # query for departure routenumber in assignments
        query4 = {"RouteNumber": {"$in": list(departure_id_set)}}

        cursor3 = self.assignment_collection.find(query3)
        cursor4 = self.assignment_collection.find(query4)

        # for assignment in destinations routes
        for doc in cursor3:
            # add day to route to destination route
            destination_dict[doc.get("RouteNumber")].append(doc.get("Day"))
        
        # for assignment in departure routes
        for doc in cursor4:
            # add day to departure route
            departure_dict[doc.get("RouteNumber")].append(doc.get("Day"))

        # list to hold destinations
        destination_return = list()

        # for each route add the days
        for route in destination_dict:
            # add the route and days 
            destination_return.append((destination_content[route],destination_dict[route]))

        # list to hold departures
        departure_return = list()

        # for each route in departure
        for route in departure_dict:
            # add the routes and days
            departure_return.append((departure_content[route], departure_dict[route]))

        return destination_return, departure_return
        

    
    """
    The program should get the route of a bus and print out all information about the route,
    including the name and ID of the driver that is assigned to it
    """
    def get_by_route(self, route):

        # query for route number in routes collections
        query1 = {"RouteNumber": {"$in" :[route]}}
        
        cursor1 = self.route_collection.find(query1)

        # route id dictionary 
        route_ids = {}
        for c in cursor1:
            # print(c['RouteNumber'])
            # dictionary of route number to route content
            route_ids[c['RouteNumber']] = c
        
        route_keys = route_ids.keys()

        # query for routenumber in assignment collection
        query2 = {"RouteNumber": {"$in": list(route_keys)}}

        cursor2 = self.assignment_collection.find(query2)

        # maps driver id to list of assignments
        assignment_ids = {}

        # for assignment found with route number
        for c in cursor2:
            
            if c['DriverID'] not in assignment_ids:
                assignment_ids[c['DriverID']] = list()
                assignment_ids[c['DriverID']].append(c)
            else:
                assignment_ids[c['DriverID']].append(c)
        
        assignment_keys = assignment_ids.keys()
        
        # seach for driver ids in driver collections
        query3 = {"ID": {"$in": list(assignment_keys)}}

        cursor3 = self.driver_collection.find(query3)

        # maps driver id to driver content 
        driver_ids = {}

        for c in cursor3:
            driver_ids[c['ID']] = c
        
        to_Return = list()

        for driver in assignment_ids:
            
            for assignment in assignment_ids[driver]:

                # add the route content, day, and driver content 
                to_Return.append((route_ids[assignment['RouteNumber']], assignment['Day'], driver_ids[assignment['DriverID']]))
                
                
        return to_Return
            



    
         


        

    """
    The program should get the name of two cities, and respond if there is a bus route that go from
    the first city directly to the second. If so, print all info about that route (as the previous query). If
    there is more than one, print them all (one after the other)
    NOTE: this is a direct question, not a does a route exist question
    """
    def get_is_there_a_route(self, city_a, city_b):
        
        # query for departure and destination city in route collection
        query1 = {'$and':[
            {"DepartureCity": {"$in" :[city_a]}},
            {"DestinationCity": {"$in" :[city_b]}},
            ]}
    
        
        cursor1 = self.route_collection.find(query1)

        # maps route id to route content
        route_ids = {}
        
        for c in cursor1:
            route_ids[c['RouteNumber']] = c
        
        route_keys = route_ids.keys()

        # search by route number in the assignment collection
        query2 = {"RouteNumber": {"$in": list(route_keys)}}

        cursor2 = self.assignment_collection.find(query2)

        # maps driver id to list of assignments
        assignment_ids = {}
        for c in cursor2:
            
            if c['DriverID'] not in assignment_ids:
                assignment_ids[c['DriverID']] = list()
                assignment_ids[c['DriverID']].append(c)
            else:
                assignment_ids[c['DriverID']].append(c)
        
        assignment_keys = assignment_ids.keys()

        # search by driver ids in driver collection
        query3 = {"ID": {"$in": list(assignment_keys)}}

        cursor3 = self.driver_collection.find(query3)

        # maps driver id to driver content
        driver_ids = {}

        for c in cursor3:
            driver_ids[c['ID']] = c

        # list of route, assignment day and driver information
        to_Return = list()

        for driver in assignment_ids:
            
            for assignment in assignment_ids[driver]:
                to_ret = []
                to_ret.append(route_ids[assignment['RouteNumber']])
                to_ret.append(assignment['Day'])
                to_ret.append(driver_ids[assignment['DriverID']])
                
                to_Return.append(to_ret)
        
        return to_Return


# p = Persistence()

# p.get_by_name("Jack", "Doe")
# print(p.get_is_there_a_route("Dallas", "Houston"))
# print(p.get_by_city("Dallas"))
# print(p.get_by_route(2))