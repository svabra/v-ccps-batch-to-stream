# BATCH TO STREAM processor (message splitter or sequencer pattern)

## Purpose
This service picks up records one by one from a specific topic. Then reads the value field in which a series of events (json array of events) are stored (batch). These events are then inserted into a topic, which is also configured in the config.

This event processor implements the splitter pattern. https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html

Because this processor is configurable, it can be reused for any message splitting task.

IMPORTANT: Be careful on how to run the docker image. See the last line in the Dockerfile. Using the `--reset` flag, will force the processor to read from the earliest records every time the processor start. That is dangerous and could produce duplicate entries in the sink topic.

## Config
This pattern implementation requires certain configuration. e.g. source and sink topic, a key from the value to use. (e.g. a path in the json value), etc.

There is a sample configuration in the /examples/storage/config.ini folder.
Place the final config into the /storage folder.
- Define the source and the sink topic. 
- Define in what field or the source event we can find the batch messages.
- Define the key path to parse the key for the new event. This path is relative to the single event.

### Dev-Env

Create the virtul environment: `py -m venv .venv`

On Windows ease the policy: `Set-ExecutionPolicy Unrestricted`
Start the Environment: `./.venv/Scripts/activate` (or allow VS Code to start it). Use `deactivate`to stop it.

All the required libraries must be listed in requirements.txt and installed by  `python -m pip install -r .\requirements.txt`
For Dev use `python -m pip install -r .\requirements_dev.txt`

To cleanup the environment run:
`pip3 freeze > to-uninstall.txt` and then
`pip3 uninstall -y -r to-uninstall.txt`

or `pip3 install pip-autoremove`

To benefit of code-insight/completion select the venv Interpreter (Ctr) --> (Ctrl+Shift+P) then search for "Python: Select Interpreter"

## Running the environment 
Run the stream processor from the offset: `python src/main.py storage/config~.ini`
Run the stream processor from the beginning: `python src/main.py storage/config.ini --reset`

## Unit Tests
simply run `pytest` in the root directory

## Dockerize the app

#### Docker BUILD (and tag)
Build the image. 
    --pull Always attempt to pull a newer version of the image (set by default)
    --rm Remove intermediate containers after a successful build (set by default)
    -t will tag the build in addition (no need to tag it seperately)
`docker build --rm -t ccps/v-ccps-batch-to-stream .`

#### Docker RUN
LOCAL DEV

Run the image as container (--name of the container) locally on your DEV maschine.
--rm would remove the contaner automatically when it exits. --> Use for developing
--it would run the app and show the stdout immediately. --> Use for developing
`docker run --rm -it -v ${PWD}/secrets:/app/secrets -v ${PWD}/storage:/app/storage --name v-ccps-batch-to-stream  ccps/v-ccps-batch-to-stream`
In case it does not start: use `docker logs 8eba06d44bf2` to see what went wrong.

## Security

SSL implementation / Certificate implementation: https://github.com/confluentinc/confluent-kafka-python
