import os
from server import persistence
import json

#os.system('mongoimport --type csv -d bus_db -c drivers --fields="ID,FirstName,LastName,Age,City,State" --drop ./processed_csvs/drivers/drivers.csv')
#os.system('mongoimport --type csv -d bus_db -c assignments --fields="DriverID,RouteNumber,Day" --drop ./processed_csvs/assignments/assignments.csv')
#os.system('mongoimport --type csv -d bus_db -c routes --fields="RouteNumber,RouteName,DepartureCity,DepartureCode,DestinationCity,DestinationCode,RouteTypeCode,DepartureTimeHour,DepartureTimeMin,TravelTimeHour,TravelTimeMin" --drop ./processed_csvs/routes/routes.csv')

day_dict = {
    "s" : ("Sunday",0),
    "M" : ("Monday",1),
    "T" : ("Tuesday",2),
    "W" : ("Wednesday",3),
    "U" : ("Thursday",4), 
    "F" : ("Friday",5),
    "S" : ("Saurday",6),
    "0" : "s",
    "1" : "M",
    "2" : "T",
    "3" : "W",
    "4" : "U",
    "5" : "F", 
    "6" : "S"
}

a = persistence.Persistence()
res = a.get_is_there_a_route("New York", "Dallas")
print(res)
# route = res[0]
# day = day_dict[res[1]][0]
# driver_info = res[2]

# route_info = "{0} - {1}: From {2},{3} to {4},{5}\n\tType: {6}\n\tDeparting at: {7}:{8} {9}\n\tWith a travel time of {10}:{11}\n\tDriver: {12}, {13} {14}".format(
#     route["RouteNumber"], route["RouteName"], route["DepartureCity"], route["DepartureCode"], route["DestinationCity"], route["DestinationCode"], 
#     route["RouteTypeCode"], route["DepartureTimeHour"], route["DepartureTimeMin"] if route['DepartureTimeMin'] > 9 else "0" + str(route['DepartureTimeMin']), 
#     day, route["TravelTimeHour"], route["TravelTimeMin"] if route["TravelTimeMin"] > 9 else "0" + str(route["TravelTimeMin"]), driver_info["LastName"], 
#     driver_info["FirstName"], driver_info["ID"])
# print(route_info)  