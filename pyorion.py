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

# Attribute class
class OrionAttribute:

    # constructor
    def __init__(self, attr_name, attr_type, attr_value):
        self.attr_name = attr_name
        self.attr_type = attr_type
        self.attr_value = attr_value

    # json representation
    def to_json(self):
        
        # creation of the json object
        json_attr = {}
        json_attr["name"] = self.attr_name
        json_attr["type"] = self.attr_type
        json_attr["value"] = self.attr_value

        # return
        return json_attr


# Entity class
class OrionEntity:

    # constructor
    def __init__(self, entity_id, entity_type, attrs = []):
        self.entity_id = entity_id
        self.entity_type = entity_type
        self.attrs = attrs

    # attribute addition
    def add_attribute(self, attribute):
        self.attrs.append(attribute)

    # attribute deletion
    def del_attribute(self, attribute):
        ind = self.attrs.index(attribute)
        del self.attrs[ind]
        
    # json representation
    def to_json(self):

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
class KP:

    # constructor
    def __init__(self, host, port, token, debug):
        self.host = host
        self.port = port
        self.token = token
        self.debug = debug

    # create entity
    def create_entity(self, entity):

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
