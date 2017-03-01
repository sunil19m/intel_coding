# Nervana Cloud Coding Challenge #

You are to build a server that processes valid bash command strings.
Your server takes the command strings from a file (commands.txt is provided as an example) and does the following:

- Checks that the command strings in the COMMAND_LIST section are valid command strings by cross-checking with the VALID_COMMAND strings section.
    - Regardless of the command itself, the command string needs to exactly match a command in the valid command strings list.
    - You are provided one `commands.txt` file, but your solution should be able to accept other files as input as well that are of the same structure.
    - Each file is separate and self-validating; ie the VALID_COMMAND section of one file doesn't affect what's valid in another file.
    - Assume the entire `commands.txt` file fits into memory, and that the "filename" argument represents a file present on the server.
    - `commands.txt` file format will always be the same.
    - For the example `commands.txt` file: `grep "tacos" commands.txt` isn't valid, but `grep "pwd" commands.txt` is.

## Assuming the command is valid: ##

1. Stores metadata about each command:
  - actual command itself as a string
  - length of command string
  - time to complete (if the command takes > 1 minute to complete, mark a 0 which will represent "Not finished")
  - eventual output (see below)
2. Grabs the command output from each command if possible. Do not store duplicate commands (you can assume they all have the same output).
3. Stores the output in the db provided (assume the db is already created at time of processing).
4. Enables the data to be fetched via the endpoint provided in the code. An acceptable solution will return all the data at once, with extended query options viewed as going above and beyond.

- The basics of the project have already been flushed out for you.
- Approach this project as if this were production code, with time and space complexity in mind.
- Also keep in mind edge cases; what about command strings that don't terminate in time? Invalid bash, commands that error when executed, and "malicious" command strings have already been screened out for you, and will not appear in the VALID_COMMANDS section.
- Write a few tests for your code as well.
- This challenge is open-ended as long as its basic requirements are met. It has a few TODOs in it, but feel free to add additional features/extensions as you see fit.
- Finally, when you're done, send us the link to where we can see the code on your Github/Bitbucket/etc.

**Good luck!**

**Bonus #1:** The `commands.txt` file no longer fits in memory. Tweak your solution to allow for this.


**Bonus #2:** Make the command executions themselves non-blocking.


**Bonus #3:** Take an optional parameter "file_data" that represents file data in the same format as the `commands.txt` file, but is sent in the POST request (doesn't exist on the server). Use this "file_data" instead of the "filename" argument (if present) to process the commands.

## Only if applying for a full-time position: ##
In addition to the main challenge above, extend/amend your project with the following:

Regarding #3, run each of them individually inside their own docker container. These runs need to be non-blocking (essentially bonus #2 above).


Some tools that might prove useful: Redis, Celery, gunicorn, bashlex, htcondor/Kubernetes, PostgreSQL/MySQL/MongoDB, Docker, cronjobs, python's `schedule` module, bash.


When you're done, host it and send us the link of where it's at!

**Bonus:** Assume that invalid, "malicious", or erroring command strings could exist in the VALID_COMMANDS section now; if a "valid" command turns out to be invalid, treat it as though it were invalid in the first place.
- Definition of invalid:
    1. returns an error on execution, ex: `ower0weg89245r`
- Definitions of malicious:
    1. attempts to write anything to /tmp
    2. attempts to delete anything


### For how to run either project ###
1. `make run` to start the project; see the `Makefile` for other helpful things like `make swagger` and `make style`
2. You can then hit it to either drop the db, init the db, fetch results, input data (curl or python requests).
   - Sample request to feed in the data: requests.post("http://127.0.0.1:8080/commands", params={'filename': 'commands.txt'})
