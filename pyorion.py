#!/usr/bin/python

# requirements
import json
import pycurl
from StringIO import StringIO

# TODO List:
# - add the exception handling
# - should we use getters and setters?
# - check about APPEND on entity creation
# - add documentation to each method and class
# - add parsing of the JSON replies
# - modify OrionKP's methods to process lists of entities


# Attribute class
class OrionAttribute:

    """This class handles attributes of the entity of the Orion Context Broker"""

    # constructor
    def __init__(self, attr_name, attr_type, attr_value):

        """Initialization method for the OrionAttribute class. Its parameters
        are the attribute name, the type and the value"""

        self.attr_name = attr_name
        self.attr_type = attr_type
        self.attr_value = attr_value

    # json representation
    def to_json(self):

        """This method returns the JSON representation of the given attribute"""
        
        # creation of the json object
        json_attr = {}
        json_attr["name"] = self.attr_name
        json_attr["type"] = self.attr_type
        json_attr["value"] = self.attr_value

        # return
        return json_attr


# Entity class
class OrionEntity:

    """This class handles entities of the Orion Context Broker"""

    # constructor
    def __init__(self, entity_id, entity_type, attrs = []):

        """Initialization method for the OrionEntity class. The required parameters
        are the entity_id, entity_type """

        self.entity_id = entity_id
        self.entity_type = entity_type
        self.attrs = attrs

    # attributes addition
    def add_attributes(self, attributes):

        """This method receives a list of attributes that must be bound to the
        entity and then inserted into the Orion Context Broker"""
        
        for a in attributes:
            self.attrs.append(a)


    # attribute deletion
    def del_attributes(self, attributes):
        
        """It receives an attribute to delete from the entity"""

        for a in attributes:
            ind = self.attrs.index(a)
            del self.attrs[ind]
        

    # json representation
    def to_json(self):
    
        """This method returns the JSON representation of the entity"""

        # create a json object
        json_entity = {}

        # fill the entity id and type
        json_entity["id"] = self.entity_id
        json_entity["type"] = self.entity_type
        
        # convert the attrs to json
        json_attrs = []
        for attr in self.attrs:
            json_attrs.append(attr.to_json())
        json_entity["attributes"] = json_attrs

        # return
        return json_entity


# KP class
class OrionKP:

    """This class implements a knowledge processor able to interact
    with the Orion Context Broker for doing update, query and subscriptions"""

    # constructor
    def __init__(self, host, port, token, debug):

        """Initialization method for the OrionKP class. Required parameters
        are the host of the CB, the port, an authentication token (or None for
        no authentication) and a boolean parameter to turn on or off the debug
        functionalities"""

        self.host = host
        self.port = port
        self.token = token
        self.debug = debug


    # create entity
    def create_entity(self, entity):

        """As the name states it creates an entity in the Orion Context Broker.
        the expected parameter is an object of the OrionEntity class"""

        # build the entity url
        entity_url = "%s:%s/ngsi10/contextEntities/%s" % (self.host, self.port, entity.entity_id)

        # curl configuration
        buff = StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.URL, entity_url)
        c.setopt(pycurl.WRITEFUNCTION, buff.write)
        c.setopt(pycurl.HTTPHEADER, ['Accept: application/json', 'Content-Type: application/json'])
        c.setopt(pycurl.POST, 1)
        c.setopt(pycurl.POSTFIELDS, json.dumps(entity.to_json()))

        # curl debug configuration
        if self.debug:
            c.setopt(pycurl.VERBOSE, 1)
     
        # curl authentication configuration
        if self.token:
            c.setopt(pycurl.USERPWD, self.token)

        # send the request
        c.perform()

        # return the reply
        return buff.getvalue()


    # delete entity
    def delete_entity(self, entity_id):

        """As the name suggests, this method is used to delete an entity from
        the Orion Context Broker"""

        # build the entity url
        entity_url = "%s:%s/ngsi10/contextEntities/%s" % (self.host, self.port, entity_id)

        # curl configuration
        buff = StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.URL, entity_url)
        c.setopt(pycurl.WRITEFUNCTION, buff.write)
        c.setopt(pycurl.HTTPHEADER, ['Accept: application/json', 'Content-Type: application/json'])
        c.setopt(pycurl.CUSTOMREQUEST, "DELETE")

        # curl debug configuration
        if self.debug:
            c.setopt(pycurl.VERBOSE, 1)
     
        # curl authentication configuration
        if self.token:
            c.setopt(pycurl.USERPWD, self.token)

        # send the request
        c.perform()

        # return the reply
        return buff.getvalue()        

        

    # update entity
    def update_entity(self, entity):
        # TODO: yet to implement
        pass


    # query
    # TODO: distinguish between the various query types:
    # - query by entity
    # - query by entity types
    # - etc... se the NGSI10 documentation

    # query by entity id
    def query_by_entity_id(self, entity_id):
        
        """It performs a query on the CB using a given entity_id (string)"""

        # build the query url
        query_url = "%s:%s/ngsi10/contextEntities/%s" % (self.host, self.port, entity_id)

        # curl configuration
        buff = StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.URL, query_url)
        c.setopt(pycurl.WRITEFUNCTION, buff.write)
        c.setopt(pycurl.HTTPHEADER, ['Accept: application/json', 'Content-Type: application/json'])

        # curl debug configuration
        if self.debug:
            c.setopt(pycurl.VERBOSE, 1)
     
        # curl authentication configuration
        if self.token:
            c.setopt(pycurl.USERPWD, self.token)

        # send the request
        c.perform()

        # return the reply
        return buff.getvalue()

    
    # query by entity type
    def query_by_entity_type(self, entity_type):

        "It performs a query using a given entity type (string)"
        
        # build the query url
        query_url = "%s:%s/ngsi10/contextEntityTypes/%s" % (self.host, self.port, entity_type)
        
        # curl configuration
        buff = StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.URL, query_url)
        c.setopt(pycurl.WRITEFUNCTION, buff.write)
        c.setopt(pycurl.HTTPHEADER, ['Accept: application/json', 'Content-Type: application/json'])

        # curl debug configuration
        if self.debug:
            c.setopt(pycurl.VERBOSE, 1)
     
        # curl authentication configuration
        if self.token:
            c.setopt(pycurl.USERPWD, self.token)

        # send the request
        c.perform()

        # return the reply
        return buff.getvalue()


    # custom query
    def custom_query(self, query_suffix):
        
        """It performs a custom query based on the given URL."""

        # build the query url
        query_url = "%s:%s/ngsi10/%s" % (self.host, self.port, query_suffix)

        # curl configuration
        buff = StringIO()
        c = pycurl.Curl()
        c.setopt(pycurl.URL, query_url)
        c.setopt(pycurl.WRITEFUNCTION, buff.write)
        c.setopt(pycurl.HTTPHEADER, ['Accept: application/json', 'Content-Type: application/json'])

        # curl debug configuration
        if self.debug:
            c.setopt(pycurl.VERBOSE, 1)
     
        # curl authentication configuration
        if self.token:
            c.setopt(pycurl.USERPWD, self.token)

        # send the request
        c.perform()

        # return the reply
        return buff.getvalue()
