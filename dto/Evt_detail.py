from json import JSONEncoder

class Evt_detail:
    def __init__(self, topic,partition,offset,key):
        # private varibale or property in Python
        self.__topic = topic
        self.__partition = partition
        self.__offset = offset
        self.__key = key

    ## getter method to get the properties using an object
    def get_topic(self):
        return self.__topic

    ## setter method to change the value 'a' using an object
    def set_topic(self, topic):
        self.__topic = topic

    ## getter method to get the properties using an object
    def get_partition(self):
        return self.__partition

    ## setter method to change the value 'a' using an object
    def set_partition(self, partition):
        self.__partition = partition

    ## getter method to get the properties using an object
    def get_offset(self):
        return self.__offset

    ## setter method to change the value 'a' using an object
    def set_offset(self, offset):
        self.__offset = offset

    ## getter method to get the properties using an object
    def get_key(self):
        return self.__key

    ## setter method to change the value 'a' using an object
    def set_key(self, key):
        self.__key = key

    def __str__(self):
        return self.__key + " " + self.__offset

# # subclass JSONEncoder
# class detail(JSONEncoder):
#         def default(self, o):
#             return o.__dict__