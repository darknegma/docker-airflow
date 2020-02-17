from enum import Enum



class PandasDFUDF(Enum):
    cleanse_column_names = cleanse_column_names
    fill_in_column = fill_in_column

#class PandasColumnUDF(Enum):


# Need transformations with positional args.
# need DF level transformations.
# need column level transformations.
# need row based transformations.


#{"name": "to_timezone", "args": {"from_": "pst", "to_": "utc"}