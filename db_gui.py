import PySimpleGUI as sg

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
            [sg.InputText(key='route_path', size=(25,1)), sg.FolderBrowse()]]

driver_csv = [[sg.Text(text='Drivers CSV', font=('Arial', 15))],
            [sg.InputText(key='driver_path', size=(25,1)), sg.FolderBrowse()]]

assignments_csv = [[sg.Text(text='Assignments CSV', font=('Arial', 15))],
            [sg.InputText(key='assignments_path', size=(25,1)), sg.FolderBrowse()]]

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

######################################################## QUERY HANDLERS ########################################################

# Submit query and format response
def get_driver_info(first_name, last_name):
    # Submit query
    #
    #
    # Response
    #Sample Driver
    driver = {
        "ID" : 1234,
        "Lastname" : "Lee",
        "Firstname" : "Matthew",
        "Age" : 22,
        "Hometown" : "Bedford",
        "Homestate" : "Texas"
    }
    driver_info = "{0} {1}, {2}\n\tAge: {3}\n\tHometown: {4}, {5}\n\t".format(driver["ID"], driver["Lastname"], driver["Firstname"], driver["Age"], driver["Hometown"], driver["Homestate"])
    route_assignments = "Routes:\n\t\t{0} - {1}".format(523, "Mustang")
    return(driver_info + route_assignments) 

def get_city_info(city_name):
    # Submit query
    #
    #
    # Response
    #Sample output
    d_routes = [
        {"ID" : 12345,
        "departure_time" : "12:00",
        "day" : "Monday",
        "destination_city" : "Dallas"},
        {"ID" : 12336,
        "departure_time" : "2:00",
        "day" : "Tuesday",
        "destination_city" : "Miami"},
        {"ID" : 125,
        "departure_time" : "4:00",
        "day" : "Thursday",
        "destination_city" : "Atlanta"}
    ]
    a_routes = [
        {"ID" : 12345,
        "arrival_time" : "12:00",
        "day" : "Monday",
        "source_city" : "Dallas"},
        {"ID" : 12336,
        "arrival_time" : "2:00",
        "day" : "Tuesday",
        "source_city" : "Miami"},
        {"ID" : 125,
        "arrival_time" : "4:00",
        "day" : "Thursday",
        "source_city" : "Atlanta"}
    ]
    departures = "Departures:\n"
    for route in d_routes:
        departures += "\t{0} {1} {2} Service to: {3}\n".format(route["ID"], route["departure_time"], route["day"], route["destination_city"])

    arrivals = "Arrivals:\n"
    for route in a_routes:
        arrivals += "\t{0} {1} {2} Service from: {3}\n".format(route["ID"], route["arrival_time"], route["day"], route["source_city"])
    return(departures+ arrivals)

def get_route_info_by_id(id):
    # Submit query
    #
    #
    # Response
    # Sample data
    route = {
        "route_ID" : 12345,
        "route_name" : 'Mustang',
        "departure_city" : "Dallas",
        "departure_city_code" : 4356,
        "destination_city" : "Houston",
        "destination_city_code" : 2334, 
        "route_type" : 1,
        "departure_time_hours" : '2',
        "departure_time_minutes" : '30',
        "travel_time_hours" : '1',
        "travel_time_minutes" : '15',
        "driver_first_name" : "Bill",
        "driver_last_name" : "Smith",
        "driver_ID" : 3221
        
    }
    route_info = "{0} - {1}: From {2} ({3}) to {4} ({5})\n\tType: {6}\n\tDeparting at: {7}:{8}\n\tWith a travel time of {9}:{10}\n\tDriver: {11}, {12} {13}".format(
        route["route_ID"], route["route_name"], route["departure_city"], route["departure_city_code"], route["destination_city"], route["destination_city_code"], 
        route["route_type"], route["departure_time_hours"], route["departure_time_minutes"], route["travel_time_hours"], route["travel_time_minutes"], route["driver_last_name"], 
        route["driver_first_name"], route["driver_ID"])
    return(route_info)  

def get_route_info_by_city(city_1, city_2):
    # Submit query
    #
    #
    # Response
    # Sample data
    route = {
        "route_ID" : 12345,
        "route_name" : 'Mustang',
        "departure_city" : "Dallas",
        "departure_city_code" : 4356,
        "destination_city" : "Houston",
        "destination_city_code" : 2334, 
        "route_type" : 1,
        "departure_time_hours" : '2',
        "departure_time_minutes" : '30',
        "travel_time_hours" : '1',
        "travel_time_minutes" : '15',
        "driver_first_name" : "Bill",
        "driver_last_name" : "Smith",
        "driver_ID" : 3221
        
    }
    route_info = "{0} - {1}: From {2} ({3}) to {4} ({5})\n\tType: {6}\n\tDeparting at: {7}:{8}\n\tWith a travel time of {9}:{10}\n\tDriver: {11}, {12} {13}".format(
        route["route_ID"], route["route_name"], route["departure_city"], route["departure_city_code"], route["destination_city"], route["destination_city_code"], 
        route["route_type"], route["departure_time_hours"], route["departure_time_minutes"], route["travel_time_hours"], route["travel_time_minutes"], route["driver_last_name"], 
        route["driver_first_name"], route["driver_ID"])
    return(route_info)  

######################################################## DB LOADING ########################################################
def load_db(path):
    # Pass path to back end to start up DB
    print(path)


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
            window['input_label_1'].update(value='Bus ID', visible=True)
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
        if active_query == 'driver_info':
            print('Submitting Driver name: {0} {1}'.format(window['input_1'].Get(), window['input_2'].Get()))
            window['output'].update(value=get_driver_info(1,2))

        elif active_query == 'city_info':
            print('Submitting city name: {0}'.format(window['input_1'].Get()))
            window['output'].update(value=get_city_info(1))

        elif active_query == 'find_route':
            if search_by_driver:
                print('Submitting driver ID: {0}'.format(window['input_1'].Get()))
                window['output'].update(value=get_route_info_by_id(1))

            else:
                print('Submitting cities : {0} {1}'.format(window['input_1'].Get(), window['input_2'].Get()))
                window['output'].update(value=get_route_info_by_city(1,2))

    if event == 'load_data':
        # Submit path to back end
        route_path = load_db(window['route_path'].Get())
        driver_path = load_db(window['driver_path'].Get())
        assignments_path = load_db(window['assignments_path'].Get())

window.close()

#route driver assignments