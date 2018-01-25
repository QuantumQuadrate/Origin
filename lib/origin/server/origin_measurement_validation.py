"""
Function for making sure stream data matches the template
"""

from origin import data_types, TIMESTAMP

# should check that the fields are consistent. That they aren't trying
# to insert a string into a float field
def measurement_validation(measurement, template):
    '''making sure stream data matches the template'''
    meas = measurement
    meas_keys = meas.keys()
    meas_keys.sort()
    try:
        meas_keys.remove(TIMESTAMP)
    except ValueError:
        pass 
    template_keys = template.keys()
    template_keys.sort()
    if meas_keys != template_keys:
        #logger.error("Measurement validation error: meas_keys != template_keys")
        return False

    for field_name in meas_keys:
        field_type_name = template[field_name]["type"]
        try:
            # try to cast data as the expected type
            data_types[field_type_name]["type"](measurement[field_name])
        except TypeError:
            #msg = "Data: {} is not of type: {}"
            #logger.error(msg.format(measurement[field_name], data_types[field_type_name["type"]]))
            return False
    return True
