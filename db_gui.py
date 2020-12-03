import PySimpleGUI as sg
import os
from server import persistence

#sg.theme('BluePurple')

######################################################## LAYOUT ########################################################

# Main Buttons for selecting query type
query_ids = [('Driver Info','driver_info'), ('City Info','city_info'), ('Find a Route','find_route')]
query_buttons = [sg.Button(button_text=b[0], key=b[1], font=('Arial', 20), pad=((50,50),(10,20)), size=(12,1), focus=False) for b in query_ids]

# Inputs Column
inputs = [
    [sg.Text(text='First Name', key='input_label_1', font=('Arial', 15))],
    [sg.Input(key='input_1', font=('Arial', 18), size=(30,1), pad=((0,0),(0,20)))],
    [sg.Text(text='Last Name', key='input_label_2', font=('Arial', 15))],
    [sg.Input(key='input_2', font=('Arial', 18), size=(30,1), pad=((0,0),(0,40)))],
    [sg.Button(button_text='Search', key="search", pad=((0,20),(0,30)), font=('Arial', 15)), sg.Button(button_text='Search by City Name', key="toggle_route_params",  pad=((0,20),(0,30)), font=('Arial', 15), visible=False)]
]

output = [
    [sg.Multiline(default_text="", size=(55,14), auto_refresh=True, key='output', do_not_clear=False)]
]

route_csv = [[sg.Text(text='Routes CSV', font=('Arial', 15))],
            [sg.InputText(key='route_path', size=(25,1)), sg.FileBrowse()]]

driver_csv = [[sg.Text(text='Drivers CSV', font=('Arial', 15))],
            [sg.InputText(key='driver_path', size=(25,1)), sg.FileBrowse()]]

assignments_csv = [[sg.Text(text='Assignments CSV', font=('Arial', 15))],
            [sg.InputText(key='assignments_path', size=(25,1)), sg.FileBrowse()]]

layout = [query_buttons,
        [sg.HorizontalSeparator(color=None, pad=None, key=None, k=None)],
        [sg.Text(text="Driver Info", key='section_header', font=('Arial', 20), size=(10,1), justification='center')],
        [sg.Column(layout=inputs, justification='center', element_justification='left', expand_x=True),
            sg.Column(layout=output, element_justification='right', expand_x=True, vertical_alignment='top')],
        [sg.HorizontalSeparator(color=None, pad=None, key=None, k=None)],
        [sg.Column(layout=route_csv, justification='center', expand_x=True, element_justification='center'), 
        sg.Column(layout=driver_csv, justification='center', expand_x=True, element_justification='center'), 
        sg.Column(layout=assignments_csv, justification='center', expand_x=True, element_justification='center')],
        [sg.Button(button_text='Load Data', key="load_data", font=('Arial', 15))]]

window = sg.Window('Bus Network', layout, finalize=True, element_justification='c', debugger_enabled=False)

for button in query_ids:
    window[button[1]].expand(expand_x=True)

print(window['find_route'].get_size())

active_query = "driver_info"
search_by_driver = True
database = 0
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

######################################################## QUERY HANDLERS ########################################################

# Submit query and format response
def get_driver_info(first_name, last_name):

    # Handle Empty Inputs
    if len(first_name) == 0 or len(last_name) == 0:
        return("Must provide a first and last name")
    # Submit query
    drivers = database.get_by_name(first_name, last_name)

    # Handle Empty Response
    if len(drivers) == 0:
        return("No drivers named {0} {1}".format(first_name, last_name))

    # Format Response
    output = ""
    for d in drivers:
        driver = d[0]
        driver_info = "{0} {1}, {2}\n\tAge: {3}\n\tHometown: {4}, {5}\n".format(driver["ID"], driver["LastName"], driver["FirstName"], driver["Age"], driver["City"], driver["State"])
        assigned_routes = d[1]
        route_assignments = "\tRoutes:\n"
        for r in assigned_routes:
            route_assignments += "\t\t{0} - {1}: {2},{3} to {4},{5}\n".format(r['RouteNumber'], r['RouteName'], r['DepartureCity'], r['DepartureCode'], r['DestinationCity'], r['DestinationCode'])
        output += (driver_info + route_assignments)
    return(output) 

def get_city_info(city_name):

    # Handle Empty Inputs
    if len(city_name) == 0:
        return("Must provide a city name")
    # Submit query
    routes = database.get_by_city(city_name)

    # Handle Empty Response
    if len(routes[0]) == 0 and len(routes[1]) == 0:
        return("No routes through {0}".format(city_name))

    # Format Response
    # Arrivals
    arrivals = routes[0]
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

    # Departures
    departures = routes[1]
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

    return(arrival_output + departure_output)

def get_route_info_by_id(route_num):

    # Handle Empty Inputs
    if len(route_num) == 0:
        return("Must provide a route number")
    # Submit query
    matching_route = database.get_by_route(route_num)

    # Handle Empty Response
    if len(matching_route) == 0:
        return("No routes with the route number: {0}".format(route_num))

    matching_route = matching_route[0]
    # Format Response
    route = matching_route[0]
    day = day_dict[matching_route[1]][0]
    driver_info = matching_route[2]

    route_info = "{0} - {1}: From {2},{3} to {4},{5}\n\tType: {6}\n\tDeparting at: {7}:{8} {9}\n\tWith a travel time of {10}:{11}\n\tDriver: {12}, {13} {14}".format(
        route["RouteNumber"], route["RouteName"], route["DepartureCity"], route["DepartureCode"], route["DestinationCity"], route["DestinationCode"], 
        route["RouteTypeCode"], route["DepartureTimeHour"], route["DepartureTimeMin"] if route['DepartureTimeMin'] > 9 else "0" + str(route['DepartureTimeMin']), 
        day, route["TravelTimeHour"], route["TravelTimeMin"] if route["TravelTimeMin"] > 9 else "0" + str(route["TravelTimeMin"]), driver_info["LastName"], 
        driver_info["FirstName"], driver_info["ID"])
    return(route_info)  

def get_route_info_by_city(city_1, city_2):
    # Handle Empty Inputs
    if len(city_1) == 0 or len(city_2) == 0:
        return("Must provide two city names")
    # Submit query
    routes = database.get_is_there_a_route(city_1, city_2)

    # Handle Empty Response
    if len(routes) == 0:
        return("No routes between {0} and {1}".format(city_1, city_2))

    # Format Response
    # route = {
    #     "route_ID" : 12345,
    #     "route_name" : 'Mustang',
    #     "departure_city" : "Dallas",
    #     "departure_city_code" : 4356,
    #     "destination_city" : "Houston",
    #     "destination_city_code" : 2334, 
    #     "route_type" : 1,
    #     "departure_time_hours" : '2',
    #     "departure_time_minutes" : '30',
    #     "travel_time_hours" : '1',
    #     "travel_time_minutes" : '15',
    #     "driver_first_name" : "Bill",
    #     "driver_last_name" : "Smith",
    #     "driver_ID" : 3221
        
    # }
    routes_info = ""
    for r in routes:
        route = r[0]
        day = day_dict[r[1]][0]
        driver_info = r[2]
        route_info = "{0} - {1}: From {2},{3} to {4},{5}\n\tType: {6}\n\tDeparting at: {7}:{8} {9}\n\tWith a travel time of {10}:{11}\n\tDriver: {12}, {13} {14}".format(
            route["RouteNumber"], route["RouteName"], route["DepartureCity"], route["DepartureCode"], route["DestinationCity"], route["DestinationCode"], 
            route["RouteTypeCode"], route["DepartureTimeHour"], route["DepartureTimeMin"] if route['DepartureTimeMin'] > 9 else "0" + str(route['DepartureTimeMin']), 
            day, route["TravelTimeHour"], route["TravelTimeMin"] if route["TravelTimeMin"] > 9 else "0" + str(route["TravelTimeMin"]), driver_info["LastName"], 
            driver_info["FirstName"], driver_info["ID"])
        routes_info += route_info
    return(routes_info)  


######################################################## EVENT LOOP ########################################################

while True:  # Event Loop
    event, values = window.read()
    print(event, values)
    if event == sg.WIN_CLOSED or event == 'Exit':
        break
    if event == 'driver_info':
        # Update UI for driver info
        window['driver_info'].SetFocus(force=False)
        window['section_header'].update(value="Driver Info")
        window['input_label_1'].update(value='First Name', visible=True)
        window['input_label_2'].update(value='Last Name', visible=True)
        window['input_1'].update(visible=True, value='')
        window['input_2'].update(visible=True, value='')
        window['toggle_route_params'].update(visible=False)
        active_query = event

    if event == 'city_info':
        # Update UI to get city info
        window['city_info'].SetFocus(force=False)
        window['section_header'].update(value="City Info")
        window['input_label_1'].update(value='City Name', visible=True)
        window['input_label_2'].update(visible=False)
        window['input_1'].update(visible=True, value='')
        window['input_2'].update(visible=False, value='')
        window['toggle_route_params'].update(visible=False)
        active_query = event
    
    if event == 'find_route':
        # Update UI to get route info
        window['find_route'].SetFocus(force=False)
        window['section_header'].update(value="Find Route")
        if search_by_driver:
            window['input_label_1'].update(value='Route ID', visible=True)
            window['input_label_2'].update(visible=False)
            window['input_1'].update(visible=True, value='')
            window['input_2'].update(visible=False, value='')
            window['toggle_route_params'].update(text="Search by City", visible=True)
            active_query = event
        else:
            window['section_header'].update(value="Find Route")
            window['input_label_1'].update(value='City 1', visible=True)
            window['input_label_2'].update(value='City 2', visible=True)
            window['input_1'].update(visible=True, value='')
            window['input_2'].update(visible=True, value='')
            window['toggle_route_params'].update(text='Search by Bus ID', visible=True)
            active_query = event
    
    if event == 'toggle_route_params':
        # Update the "output" text element to be the value of "input" element
        search_by_driver = not search_by_driver
        window['find_route'].Click()
            
    if event == 'search':
        # Submit appropriate query from inputs
        if database == 0:
            window['output'].update(value="No Data in Database\nLoad data before submitting queries")
        elif active_query == 'driver_info':
            print('Submitting Driver name: {0} {1}'.format(window['input_1'].Get(), window['input_2'].Get()))
            window['output'].update(value=get_driver_info(window['input_1'].Get(),window['input_2'].Get()))

        elif active_query == 'city_info':
            print('Submitting city name: {0}'.format(window['input_1'].Get()))
            window['output'].update(value=get_city_info(window['input_1'].Get()))

        elif active_query == 'find_route':
            if search_by_driver:
                print('Submitting Route ID: {0}'.format(window['input_1'].Get()))
                window['output'].update(value=get_route_info_by_id(window['input_1'].Get()))

            else:
                print('Submitting cities : {0} {1}'.format(window['input_1'].Get(), window['input_2'].Get()))
                window['output'].update(value=get_route_info_by_city(window['input_1'].Get(), window['input_2'].Get()))

    if event == 'load_data':
        # Submit path to back end
        route_path = window['route_path'].Get()
        driver_path = window['driver_path'].Get()
        assignments_path = window['assignments_path'].Get()
        
        # Validate Input Data
        #os.system('python validation.py --"{0}" --"{1}" --"{2}"'.format(route_path, driver_path, assignments_path))
        
        # Load validated data into mongo
        #os.system('mongoimport --type csv -d bus_db -c drivers --fields="ID,FirstName,LastName,Age,City,State" --drop ./processed_csvs/drivers/drivers.csv')
        #os.system('mongoimport --type csv -d bus_db -c assignments --fields="DriverID,RouteNumber,Day" --drop ./processed_csvs/assignments/assignments.csv')
        #os.system('mongoimport --type csv -d bus_db -c routes --fields="RouteNumber,RouteName,DepartureCity,DepartureCode,DestinationCity,DestinationCode,RouteTypeCode,DepartureTimeHour,DepartureTimeMin,TravelTimeHour,TravelTimeMin" --drop ./processed_csvs/routes/routes.csv')
        
        # Create Query object
        database = persistence.Persistence()

window.close()

#route driver assignments