import origin

# dict of datatypes recognized by the server
# entries keys are the server string and contain: the mysql data type, the format char for the
# python struct library, and if the type is allowed in the binary data format

# for the python struct ilbrary see:
data_types = {}

# integer types ====================================================================================
# 32b - default
data_types["int"] = {
    "mysql": "INT",
    "numpy": "int32",
    "format_char": "i",
    "binary_allowed": True,
    "size": 4,
    "type": int
}
data_types["uint"] = {
    "mysql": "INT UNSIGNED",
    "numpy": "uint32",
    "format_char": "I",
    "binary_allowed": True,
    "size": 4,
    "type": int
}
data_types["int32"] = data_types["int"]
data_types["uint32"] = data_types["uint"]
# 64b
data_types["int64"] = {
    "mysql": "BIGINT",
    "numpy": "int64",
    "format_char": "q",
    "binary_allowed": True,
    "size": 8,
    "type": int
}
data_types["uint64"] = {
    "mysql": "BIGINT UNSIGNED",
    "numpy": "uint64",
    "format_char": "Q",
    "binary_allowed": True,
    "size": 8,
    "type": int
}
# 8b
data_types["int8"] = {
    "mysql": "TINYINT",
    "numpy": "int8",
    "format_char": "b",
    "binary_allowed": True,
    "size": 1,
    "type": int
}
data_types["uint8"] = {
    "mysql": "TINYINT UNSIGNED",
    "numpy": "uint8",
    "format_char": "B",
    "binary_allowed": True,
    "size": 1 ,
    "type": int
}
# 16b
data_types["int16"] = {
    "mysql": "SMALLINT",
    "numpy": "int16",
    "format_char": "h",
    "binary_allowed": True,
    "size": 2,
    "type": int
}
data_types["uint16"] = {
    "mysql": "SMALLINT UNSIGNED",
    "numpy": "uint16",
    "format_char": "H",
    "binary_allowed": True,
    "size": 2,
    "type": int
}

# floating point types =============================================================================
# 32b
data_types["float"] = {
    "mysql": "FLOAT",
    "numpy": "float32",
    "format_char": "f",
    "binary_allowed": True,
    "size": 4,
    "type": float
}
# 64b
data_types["double"] = {
    "mysql": "DOUBLE",
    "numpy": "float64",
    "format_char": "d",
    "binary_allowed": True,
    "size": 8,
    "type": float
}
data_types["float64"] = data_types["double"]
data_types["float32"] = data_types["float"]

# boolean types ====================================================================================
data_types["bool"] = {
    "mysql": "BOOL",
    "numpy": "bool_",
    "format_char": "?",
    "binary_allowed": True,
    "size": 1,
    "type": bool
}

# other types ======================================================================================
# max string size is fixed for hdf5 files, if you want to change it change "S10" to "Sx"
data_types["string"] = {
    "mysql": "TEXT",
    "numpy": "S10",
    "format_char": "s",
    "binary_allowed": False,
    "size": None,
    "type": str
}
