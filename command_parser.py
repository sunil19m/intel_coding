"""
Handles the work of validating and processing command input.
"""
# Internal Imports
from db import session
from base import Command

# External imports
from subprocess import Popen, PIPE
from math import ceil
from multiprocessing import (Manager,
                             Process)
from os import (killpg,
                getpgid)
from signal import SIGTERM
from time import time


# Constants
COMMAND_LIST_FORMAT = "[COMMAND LIST]"
VALID_COMMANDS_FROMAT = "[VALID COMMANDS]"
KILL_COMMAND_TIME = 60

def exceute_command(command, shared_dict):
    """
    Function to run the the unix commands as seperate process
        @param command: unix command string
        @shared_dict: shared dictionary across different process
    """
    pro = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    output = pro.communicate()
    shared_dict[command] = [pro.pid, command, len(command), output[0]]
    
def check_command_validity(command_list, valid_command_dict):
    """
    Takes the commands to be executed along with the valid command list 
    and checks if the commands are present in valid list by lookup.

        @param command_list: list of unix commands (list)
        @param valid_command_dict: Valid commands as dictionary
    """
    valid_commands = list()
    for command in command_list:
        if command not in valid_command_dict:
            continue
        valid_commands.append(command)
    return list(set(valid_commands))

def fetch_commands_from_text(file_data):
    """
    Takes the raw data from the text file and parses 
    and puts the data to appropriate variables based on the section.

    The commands to execute are the sets (Removing duplication).
    The commands to verify are made as dictionary.
        @param file_data: raw file information
        @return command_list => Containing the commands to execute
                valid_command_dict => Containing the verification commands.
    """
    command_list = list()
    valid_command_dict = dict()
    is_valid_command_section = False
    for line in file_data:
        if not is_valid_command_section:
            if line.strip() == COMMAND_LIST_FORMAT:
                continue
            if line.strip() == VALID_COMMANDS_FROMAT:
                is_valid_command_section = True
                continue
            command_list.append(line.strip())
        else:
            valid_command_dict[line.strip()] = True
    return (command_list, valid_command_dict)

def get_valid_commands(lock, queue, file_name):
    """
    @param lock => puts the lock on the function so no threading issues.
    @param queue => Queue where the commands are put
    @param file_name => Containing the file path.
    @return queue

    1. First puts the lock on the function, because of threading issues.
    2. It validates whether the text file is a valid command format else raises exception.
    3. From the queue it fetches all the commands to execute and all the valid commands.
    4. If the commands are present in the valid commmand dictionary, then only puts to the queue.
    """
    lock.acquire()
    try:
        # Opens the file
        with open(file_name, "r") as file_pointer:
            lines = file_pointer.readlines()
            if lines and lines[0].strip() != COMMAND_LIST_FORMAT:
                raise Exception("Please provide the proper command file")
            # Fetches the two section
            (command_list, valid_command_dict) = fetch_commands_from_text(lines)
        # Validates the commands to execute
        commands = check_command_validity(command_list, valid_command_dict)
        queue.put(commands)
    finally:
        lock.release()
    return queue

def put_to_db(data):
    """
    This functions takes the data and puts to the database
        @param data containing the dictionary
    """
    insertion_list = list()
    for value in data.values():
        insertion_list.append(Command(value[1], value[2], value[4], value[3]))
    session.add_all(insertion_list)
    session.commit()

def kill_zombie_process(data):
    """
    If there exists any zombie process it kills it.
    """
    for val in data.values():
        try:
            killpg(getpgid(val[0].pid), SIGTERM)
        except:
            pass

def process_command_output(lock, queue):
    """
    1. Executes the commands in the queue.
    2. If the commands takes longer than 60 seconds, then kills that process
    3. This tracks the command, length, time taken to execute & the output put to stdout.
    4. The shell process is killed once everything is executed.
    5. Finally the information is put the database using SQLAlchemy

    @param lock: lock so its thread safe
    @param queue: The queue contains the commands to execute
    """
    lock.acquire()
    try:
        manager = Manager()
        shared_dict = manager.dict()

        # Checks if the queue is empty and does nothing
        if not (queue and queue.qsize() > 0):
            lock.release()
            return

        # Fetches the commands from the queue
        command = queue.get()
        for comm in command:
            time_spent = 0
            start_time = time()
            process = Process(target=exceute_command, \
                              name="exceute_command", args=(comm,shared_dict))
            process.start()
            process.join(KILL_COMMAND_TIME)
            
            # If the process is running after give time it terminates it
            if process.is_alive():
                process.terminate()
                process.join()
            else:
                time_spent = (time() - start_time)
            if comm not in shared_dict:
                shared_dict[comm] = [None, comm, len(comm), "", 0]
            else:
                val = shared_dict[comm]
                val.append(int(ceil(time_spent)))
                shared_dict[comm] = val
        
        kill_zombie_process(shared_dict)
        put_to_db(shared_dict)
    finally:
        lock.release()
