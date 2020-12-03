import pymongo
from pymongo import MongoClient

class Persistence:
    
    
    def __init__(self):
        client = MongoClient()
        self.db = client.bus_db
        self.driver_collection = self.db.drivers
        self.route_collection = self.db.routes
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
        print(first_name, last_name)
        
        query = {'$and':[
            {"FirstName": {"$in" :[first_name]}},
            {"LastName": {"$in" :[last_name]}},
            ]}

        cursor_driver = self.driver_collection.find(query)

        id_set = set()
        driver_assignment_dict = {}
        driver_route_dict = {}
        driver_information = list()
        for doc in cursor_driver:
            id_set.add(doc.get("ID"))
            driver_assignment_dict[doc.get("ID")] = set()
            driver_route_dict[doc.get("ID")] = list()
            driver_information.append(doc)
        
        query = {"DriverID": {"$in": list(id_set)}}

        cursor_assignment = self.assignment_collection.find(query)


        route_set = set()
        for doc in cursor_assignment:
            route_set.add(doc.get("RouteNumber"))
            driver_assignment_dict[doc.get("DriverID")].add(doc.get("RouteNumber"))

        
        query = {"RouteNumber": {"$in": list(route_set)}}

        cursor_routes = self.route_collection.find(query)
        
        for doc in cursor_routes:
            for driver in driver_assignment_dict:
                if doc.get("RouteNumber") in driver_assignment_dict[driver]:
                    driver_route_dict[driver].append(doc)
        

        to_Return = list()

        for driver in driver_information:
            theid = driver.get("ID")
            to_Return.append((driver, driver_route_dict[theid]))
            #print(theid)
        
        return to_Return

        


    """
    The program should get the name of a city, and print out all the routes that go through the city
    (separate departure and destination, order by time: assume Sunday 00:00 am is the starting time). 
    """
    def get_by_city(self, city):
        #Get the route ids that have city in to: OR from: fields in route_collection

        #return all docs with one of those ids in assignment_collection
        #do a query on routes_collection with city_a in from: and city_b in to:
        query1 = {"DestinationCity": {"$in" :[city]}}
        query2 = {"DepartureCity": {"$in" :[city]}}
        
        
        cursor1 = self.route_collection.find(query1)
        cursor2 = self.route_collection.find(query2)

        destination_id_set = set()
        departure_id_set = set()

        destination_content = {}
        destination_dict = {}
        for doc in cursor1:
            destination_id_set.add(doc.get("RouteNumber"))
            destination_dict[doc.get("RouteNumber")] = list()
            destination_content[doc.get("RouteNumber")] = doc
        
        departure_content = {}
        departure_dict = {}
        for doc in cursor2:
            departure_id_set.add(doc.get("RouteNumber"))
            departure_dict[doc.get("RouteNumber")] = list()
            departure_content[doc.get("RouteNumber")] = doc
        
        query3 = {"RouteNumber": {"$in": list(destination_id_set)}}
        query4 = {"RouteNumber": {"$in": list(departure_id_set)}}

        cursor3 = self.assignment_collection.find(query3)
        cursor4 = self.assignment_collection.find(query4)

        for doc in cursor3:
            destination_dict[doc.get("RouteNumber")].append(doc.get("Day"))
        
        for doc in cursor4:
            departure_dict[doc.get("RouteNumber")].append(doc.get("Day"))

        
        destination_return = list()
        for route in destination_dict:
            destination_return.append((destination_content[route],destination_dict[route]))

        departure_return = list()
        for route in departure_dict:
            departure_return.append((departure_content[route], departure_dict[route]))

        return destination_return, departure_return
        

    
    """
    The program should get the route of a bus and print out all information about the route,
    including the name and ID of the driver that is assigned to it
    """
    def get_by_route(self, route):

        query1 = {"RouteNumber": {"$in" :[route]}}

        cursor1 = self.route_collection.find(query1)

        route_ids = {}
        for c in cursor1:
            print(c['RouteNumber'])
            route_ids[c['RouteNumber']] = c
        
        route_keys = route_ids.keys()
        query2 = {"RouteNumber": {"$in": list(route_keys)}}

        cursor2 = self.assignment_collection.find(query2)

        assignment_ids = {}
        for c in cursor2:
            
            if c['DriverID'] not in assignment_ids:
                assignment_ids[c['DriverID']] = list()
                assignment_ids[c['DriverID']].append(c)
            else:
                assignment_ids[c['DriverID']].append(c)
        
        assignment_keys = assignment_ids.keys()

        query3 = {"ID": {"$in": list(assignment_keys)}}

        cursor3 = self.driver_collection.find(query3)

        driver_ids = {}

        for c in cursor3:
            driver_ids[c['ID']] = c
        
        to_Return = list()

        for driver in assignment_ids:
            
            for assignment in assignment_ids[driver]:
                to_ret = "\nRoute:\n" + str(route_ids[assignment['RouteNumber']])
                to_ret += "\nOnDay: " + assignment['Day']
                to_ret += "\nWithDriver:\n" + str(driver_ids[assignment['DriverID']])
                # print(to_ret)
                to_Return.append((route_ids[assignment['RouteNumber']], assignment['Day'], driver_ids[assignment['DriverID']]))
                #to_Return.append(to_ret)
                
        return to_Return
            



    
         


        

    """
    The program should get the name of two cities, and respond if there is a bus route that go from
    the first city directly to the second. If so, print all info about that route (as the previous query). If
    there is more than one, print them all (one after the other)
    NOTE: this is a direct question, not a does a route exist question
    """
    def get_is_there_a_route(self, city_a, city_b):
        #do a query on routes_collection with city_a in from: and city_b in to:
        query1 = {'$and':[
            {"DepartureCity": {"$in" :[city_a]}},
            {"DestinationCity": {"$in" :[city_b]}},
            ]}
        
        # query2 = {'$and':[
        #     {"DestinationCity": {"$in" :[city_a]}},
        #     {"DepartureCity": {"$in" :[city_b]}},
        #     ]}
        
        cursor1 = self.route_collection.find(query1)

        # cursor2 = self.route_collection.find(query2)

        # double_cursor_list = list(cursor1) + list(cursor2)

        route_ids = {}
        
        for c in cursor1:
            # print(c['RouteNumber'])
            route_ids[c['RouteNumber']] = c
        
        route_keys = route_ids.keys()
        query2 = {"RouteNumber": {"$in": list(route_keys)}}

        cursor2 = self.assignment_collection.find(query2)

        assignment_ids = {}
        for c in cursor2:
            
            if c['DriverID'] not in assignment_ids:
                assignment_ids[c['DriverID']] = list()
                assignment_ids[c['DriverID']].append(c)
            else:
                assignment_ids[c['DriverID']].append(c)
        
        assignment_keys = assignment_ids.keys()

        query3 = {"ID": {"$in": list(assignment_keys)}}

        cursor3 = self.driver_collection.find(query3)

        driver_ids = {}

        for c in cursor3:
            driver_ids[c['ID']] = c
        
        to_Return = list()

        for driver in assignment_ids:
            
            for assignment in assignment_ids[driver]:
                to_ret = "\nRoute:\n" + str(route_ids[assignment['RouteNumber']])
                to_ret += "\nOnDay: " + assignment['Day']
                to_ret += "\nWithDriver:\n" + str(driver_ids[assignment['DriverID']])
                
                to_Return.append(to_ret)
        print(len(to_Return))
        return to_Return


# p = Persistence()

# p.get_by_name("Jack", "Doe")
# print(p.get_is_there_a_route("Dallas", "Houston"))
# print(p.get_by_city("Dallas"))
# print(p.get_by_route(2))