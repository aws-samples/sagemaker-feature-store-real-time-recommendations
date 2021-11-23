"""
A simple class for loading and storing JSON documents in a local file
"""

import json
import os.path
import pprint
from datetime import datetime


class ParameterStore:
    """
    Create a parameter store with namespace functionality
    that stores keys and values in a local JSON file
    """

    def __init__(
        self, path="", parameters={}, filename="parameters.json", verbose=True
    ):
        """
        Constructor
        """
        self.filename = path + filename
        self.verbose = verbose
        self.namespace = "experiment_1"

        # If the file already exists, load up the params
        if os.path.exists(self.filename):
            self.load()
            self.parameters.update(parameters)
        # Otherwise set params to be empty
        else:
            self.parameters = parameters

    def set_namespace(self, namespace):
        """
        Set namespace to store variables under.

        :param namespace: str
        :return: None
        """
        self.namespace = namespace

    def create(self, parameters={}):
        """
        Create a new parameter store with the option of
        separating keys and values by namespace.

        :param parameters: dict
        :return: None
        """
        self.parameters[self.namespace] = parameters
        if self.verbose:
            print(f"Creating : \n")
            pprint.pprint(parameters)

    def read(self):
        """
        Return a dictionary of parameters
        belonging to a namespace

        :return: dict
        """
        try:
            if self.parameters:
                if self.verbose:
                    print(f"Reading : {self.namespace}\n")
                    pprint.pprint(self.parameters)
                return self.parameters[self.namespace]
            else:
                return None

        except Exception as inst:
            print(type(inst))  # the exception instance
            print(inst.args)  # arguments stored in .args
            print(inst)

    def read_all(self, parameters={}):
        """
        Return entire dictionary of parameters

        :param parameters: dict
        """
        if self.parameters:
            pprint(self.parameters)

    def add(self, parameters={}):
        """
        Add new parameters including updating old ones with new values

        :param parameters: dict
        :return: None
        """
        try:
            self.parameters[self.namespace].update(parameters)
            if self.verbose:
                print(f"Updating Params : \n")
                pprint.pprint(parameters)

        except Exception as inst:
            print(type(inst))  # the exception instance
            print(inst.args)  # arguments stored in .args
            print(inst)

    def delete(self, key):
        """
        Delete a key and its value from the parameter store

        :param key: str
        :return: None
        """

        self.parameters[self.namespace].__delitem__(key)

    def clear(self):
        """
        Erase all parameter keys and values for a namespace

        :return: None
        """
        self.parameters[self.namespace] = {}

    def clear_all(self):
        """
        Erase all parameter keys and in every namespace

        :return: None
        """
        self.parameters = {}

    def load(self):
        """
        Load existing parameters from the given file name
        and namespace.

        :return: None
        """
        with open(self.filename, "r") as file:
            document = file.read()

        self.parameters = json.loads(document)
        if self.verbose:
            print(f"Loading : \n")
            pprint.pprint(self.parameters)

    def store(self):
        """
        Save the updates to the parameter store

        :return: None
        """
        ordered = dict(sorted(self.parameters.items()))
        self.parameters = ordered

        # get and update datetime for when you are storing
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        print("date and time: ", dt_string)

        self.add({"__timestamp": dt_string})
        if self.verbose:
            print(f"Storing : \n")
            pprint.pprint(self.parameters)
        with open(self.filename, "w") as file:
            file.write(json.dumps(self.parameters))
