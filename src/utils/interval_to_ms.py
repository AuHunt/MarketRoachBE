'''
Util module that defines a function for converting interval strings into millisecond integers
'''

def interval_to_ms(interval: str):
    '''
    Util function to convert interval strings to milliseconds integer
    (e.g. second, minute, hour, day, etc.)
    '''
    match interval.lower():
        case 'second':
            return 1000
        case 'minute':
            return 60000
        case 'hour':
            return 3600000
        case 'day':
            return 86400000
        case 'week':
            return 604800000
