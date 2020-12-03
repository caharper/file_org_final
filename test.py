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
res = a.get_by_city("Dallas")
arrivals = res[0]
arrival_output = "Arrivals:\n"
if len(arrivals) == 0:
    arrival_output += "\tNone\n"

for r in arrivals:
    route = r[0]
    arrival_mins = route["DepartureTimeMin"] + route["TravelTimeMin"]
    if arrival_mins < 10:
        arrival_mins = "0{0}".format(arrival_mins)
    arrival_hour = (route["DepartureTimeHour"] + route["TravelTimeHour"])%24
    days_traveled = int((route["DepartureTimeHour"] + route["TravelTimeHour"])/24)
    day = ""
    if len(r[1]) > 0:
        arr_day = day_dict[r[1][0]][1]
        arr_day = (arr_day + days_traveled)%7
        day = day_dict[day_dict[str(arr_day)]][0]
    
    arrival_time = "{0}:{1}".format(arrival_hour, arrival_mins)
    arrival_output += "\t{0} {1} {2} - Service from: {3},{4}\n".format(route["RouteNumber"], arrival_time, day, route["DepartureCity"], route["DepartureCode"])


departures = res[1]
departure_output = "Departures:\n"
if len(departures) == 0:
    departure_output += "\tNone\n"

for r in departures:
    route = r[0]
    day = day_dict[r[1][0]][0] if len(r[1]) > 0 else "" 
    departure_mins = route["DepartureTimeMin"]
    if departure_mins < 10:
        departure_mins = "0{0}".format(departure_mins)
    departure_time = "{0}:{1}".format(route["DepartureTimeHour"], departure_mins)
    departure_output += "\t{0} {1} {2} - Service to: {3},{4}\n".format(route["RouteNumber"], departure_time, day, route["DestinationCity"], route["DestinationCode"])


print(arrival_output + departure_output)