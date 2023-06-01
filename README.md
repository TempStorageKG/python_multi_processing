# python_multi_processing
This is a python example using os.fork() and sockets to perform multi processing.

Had an issue where I needed multi processing and the inherent python libraries were making things slower.
So put together code using os.fork() and sockets to manage the processes.
Removing the python multi processing packages and using this procedure there was an increase in processing speed.

This provides a solution to run multiple processes at the same time, report back to a main process that will gather all of the resposnes.
